import asyncio
import json
import logging
import heapq
from typing import Dict, List, Optional, Tuple
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    SHUTTLE_COMMAND_PORT, SHUTTLE_RESPONSE_PORT, SHUTTLE_READ_TIMEOUT,
    RETRY_INTERVAL, COMMAND_PRIORITIES
)

logger = logging.getLogger(__name__)

class ShuttleClient:
    def __init__(self, shuttles_config_path: str = "shuttles.json"):
        self.shuttles_config_path = shuttles_config_path
        self.shuttles = {}
        self.command_queue = []  # Очередь с приоритетами
        self.wms_client = None  # Будет установлен извне
        self.load_shuttles_config()
        
    def load_shuttles_config(self):
        """Загрузить конфигурацию шаттлов"""
        try:
            with open(self.shuttles_config_path, 'r') as f:
                data = json.load(f)
                self.shuttles = {s["ip"]: s for s in data["shuttles"]}
                logger.debug(f"Загружена конфигурация {len(self.shuttles)} шаттлов")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации шаттлов: {e}")
            self.shuttles = {}
    
    def save_shuttles_config(self):
        """Сохранить конфигурацию шаттлов"""
        try:
            # Преобразуем словарь в список для JSON
            shuttles_list = list(self.shuttles.values())
            data = {"shuttles": shuttles_list}
            
            # Сначала записываем во временный файл
            temp_file = f"{self.shuttles_config_path}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Затем переименовываем для атомарной операции
            import os
            os.replace(temp_file, self.shuttles_config_path)
            
            # Дополнительно логируем список IP шаттлов
            ip_list = [s["ip"] for s in shuttles_list]
            logger.debug(f"Конфигурация {len(shuttles_list)} шаттлов сохранена: {', '.join(ip_list)}")
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации шаттлов: {e}")

    
    def add_shuttle(self, ip: str, cell: str = "Unknown", warehouse: str = "Unknown"):
        """Добавить новый шаттл"""
        # Перезагружаем конфигурацию перед добавлением для синхронизации
        self.load_shuttles_config()
        
        if ip not in self.shuttles:
            logger.info(f"Новый шаттл добавлен: {ip}")
            self.shuttles[ip] = {"ip": ip, "cell": cell, "warehouse": warehouse}
            try:
                self.save_shuttles_config()
                logger.info(f"Конфигурация сохранена с новым шаттлом {ip}")
            except Exception as e:
                logger.error(f"Ошибка сохранения конфигурации с новым шаттлом {ip}: {e}")
        else:
            logger.debug(f"Шаттл {ip} уже существует в конфигурации")

    
    def update_shuttle_location(self, ip: str, cell: str = None, warehouse: str = None):
        """Обновить местоположение шаттла"""
        if ip in self.shuttles:
            if cell:
                self.shuttles[ip]["cell"] = cell
            if warehouse:
                self.shuttles[ip]["warehouse"] = warehouse
                if warehouse != self.shuttles[ip].get("warehouse"):
                    self.shuttles[ip]["cell"] = "Unknown"  # Сброс ячейки при смене склада
            self.save_shuttles_config()
            logger.info(f"Шаттл {ip} перемещён в ячейку {cell} на складе {warehouse}")
    
    async def send_command(self, ip: str, command: str, task_id: str = None) -> bool:
        """Отправить команду шаттлу"""
        try:
            full_command = f"{command} {task_id}" if task_id else command
            if not full_command.endswith('\r\n'):
                full_command += '\r\n'
            
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, SHUTTLE_COMMAND_PORT),
                timeout=5.0
            )
            
            writer.write(full_command.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            
            logger.debug(f"Отправлена команда: {full_command.strip()} -> {ip}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки команды {command} шаттлу {ip}: {e}")
            return False
    
    def add_command_to_queue(self, ip: str, command: str, task_id: str = None):
        """Добавить команду в очередь с приоритетом"""
        priority = COMMAND_PRIORITIES.get(command.split()[0], 10)
        heapq.heappush(self.command_queue, (priority, ip, command, task_id))
    
    async def process_command_queue(self):
        """Обработать очередь команд"""
        while self.command_queue:
            priority, ip, command, task_id = heapq.heappop(self.command_queue)
            await self.send_command(ip, command, task_id)
            await asyncio.sleep(0.1)  # Небольшая задержка между командами
    
    async def process_tasks(self, tasks: List[Dict]):
        """Обработать задачи из WMS"""
        for task in tasks:
            task_id = task.get("externalId")
            task_type = task.get("type", "PALLET_IN")  # По умолчанию PALLET_IN
            
            # Найти подходящий шаттл (простая логика - первый доступный)
            for ip in self.shuttles:
                self.add_command_to_queue(ip, task_type, task_id)
                break  # Берем первый доступный шаттл
        
        await self.process_command_queue()
    
    def parse_shuttle_response(self, data: str, ip: str) -> Optional[Tuple[str, str, str]]:
        """Парсить ответ шаттла"""
        try:
            data = data.strip()
            
            # Формат: COMMAND_DONE или COMMAND-ID_DONE
            if data.endswith('_DONE'):
                command_part = data[:-5]  # Убираем '_DONE'
                if '-' in command_part:
                    command, task_id = command_part.split('-', 1)
                    return command, 'DONE', task_id
                else:
                    return command_part, 'DONE', None
            
            # Формат: STATUS=VALUE или LOC=VALUE
            if '=' in data:
                key, value = data.split('=', 1)
                return key, value, None
            
            # Стандартный формат: COMMAND STATUS [TASK_ID]
            parts = data.split()
            if len(parts) >= 3:
                return parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                return parts[0], parts[1], None
            else:
                logger.debug(f"Простое сообщение от шаттла {ip}: {data}")
                return data, 'INFO', None
                
        except Exception as e:
            logger.error(f"Ошибка парсинга ответа шаттла {ip}: {e}")
            return None
    
    async def handle_shuttle_response(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Обработать ответ от шаттла"""
        peername = writer.get_extra_info("peername")
        if not peername:
            writer.close()
            await writer.wait_closed()
            return
        
        ip = peername[0]
        logger.debug(f"Получено сообщение от шаттла {ip}")
 
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=SHUTTLE_READ_TIMEOUT)
            raw_data = data.decode('utf-8').strip()
            logger.debug(f"Сырые данные от шаттла ({ip}): {raw_data}")
            
            # Перезагружаем конфигурацию перед проверкой
            self.load_shuttles_config()
            
            # Добавить шаттл если он новый
            if ip not in self.shuttles:
                logger.info(f"Обнаружен новый шаттл с IP: {ip}")
                self.add_shuttle(ip)
            
            # Парсить ответ
            parsed = self.parse_shuttle_response(raw_data, ip)
            if parsed:
                command, status, task_id = parsed
                logger.debug(f"Парсинг сообщения {ip}: {command} | {status} | {task_id}")
                
                # Обработать специальные команды
                if command == "MOVE_TO_CELL" and status == "DONE" and task_id:
                    self.update_shuttle_location(ip, cell=task_id)
                elif command == "CHANGE_WAREHOUSE" and status == "DONE" and task_id:
                    self.update_shuttle_location(ip, warehouse=task_id)
                
                # Обновить статус в WMS если есть task_id
                if task_id and status == "DONE" and self.wms_client:
                    await self.wms_client.update_task_status(task_id, "completed")
                    logger.debug(f"Задача {task_id} выполнена шаттлом {ip}")
                
                # Отправить MRCD в ответ на сообщения шаттлов
                asyncio.create_task(self.send_mrcd_response(ip))
            
        except asyncio.TimeoutError:
            logger.warning(f"Тайм-аут чтения от шаттла {ip}")
        except Exception as e:
            logger.error(f"Ошибка обработки ответа шаттла {ip}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def send_mrcd_response(self, ip: str):
        """Отправить MRCD в ответ на сообщение шаттла"""
        await asyncio.sleep(0.1)  # Небольшая задержка
        await self.send_command(ip, "MRCD")
        logger.debug(f"Отправлен MRCD шаттлу {ip}")
    
    async def periodic_config_save(self):
        """Периодически сохранять конфигурацию шаттлов"""
        while True:
            try:
                await asyncio.sleep(60)  # Сохраняем каждую минуту
                if self.shuttles:
                    logger.debug("Периодическое сохранение конфигурации шаттлов")
                    self.save_shuttles_config()
            except Exception as e:
                logger.error(f"Ошибка периодического сохранения: {e}")
    
    async def listen_shuttles(self):
        """Слушать ответы от шаттлов"""
        # Запускаем задачу периодического сохранения
        asyncio.create_task(self.periodic_config_save())
        
        while True:
            try:
                logger.info(f"Запуск сервера для прослушивания шаттлов на порту {SHUTTLE_RESPONSE_PORT}")
                server = await asyncio.start_server(
                    self.handle_shuttle_response,
                    host="0.0.0.0",
                    port=SHUTTLE_RESPONSE_PORT
                )
                
                async with server:
                    await server.serve_forever()
                    
            except Exception as e:
                logger.error(f"Ошибка сервера шаттлов: {e}")
                logger.info(f"Перезапуск через {RETRY_INTERVAL} секунд")
                await asyncio.sleep(RETRY_INTERVAL)
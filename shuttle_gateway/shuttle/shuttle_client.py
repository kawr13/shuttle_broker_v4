import asyncio
import json
import logging
import heapq
from typing import Dict, List, Optional, Tuple
from ..config import (
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
            data = {"shuttles": list(self.shuttles.values())}
            with open(self.shuttles_config_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug("Конфигурация шаттлов сохранена")
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации шаттлов: {e}")
    
    def add_shuttle(self, ip: str, cell: str = "Unknown", warehouse: str = "Unknown"):
        """Добавить новый шаттл"""
        if ip not in self.shuttles:
            self.shuttles[ip] = {"ip": ip, "cell": cell, "warehouse": warehouse}
            self.save_shuttles_config()
            logger.info(f"Новый шаттл добавлен: {ip}")
    
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
            parts = data.strip().split()
            if len(parts) >= 3:
                command = parts[0]
                status = parts[1]
                task_id = parts[2]
                return command, status, task_id
            elif len(parts) == 2:
                command = parts[0]
                status = parts[1]
                return command, status, None
            else:
                logger.warning(f"Не удалось разобрать сообщение шаттла: {data}")
                return None
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
        
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=SHUTTLE_READ_TIMEOUT)
            raw_data = data.decode('utf-8').strip()
            logger.debug(f"Сырые данные от шаттла ({ip}): {raw_data}")
            
            # Добавить шаттл если он новый
            if ip not in self.shuttles:
                self.add_shuttle(ip)
            
            # Парсить ответ
            parsed = self.parse_shuttle_response(raw_data, ip)
            if parsed:
                command, status, task_id = parsed
                
                # Обработать специальные команды
                if command == "MOVE_TO_CELL" and status == "DONE" and task_id:
                    self.update_shuttle_location(ip, cell=task_id)
                elif command == "CHANGE_WAREHOUSE" and status == "DONE" and task_id:
                    self.update_shuttle_location(ip, warehouse=task_id)
                
                # Обновить статус в WMS если есть task_id
                if task_id and status == "DONE" and self.wms_client:
                    await self.wms_client.update_task_status(task_id, "completed")
                    logger.debug(f"Задача {task_id} выполнена шаттлом {ip}")
            
        except asyncio.TimeoutError:
            logger.warning(f"Тайм-аут чтения от шаттла {ip}")
        except Exception as e:
            logger.error(f"Ошибка обработки ответа шаттла {ip}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def listen_shuttles(self):
        """Слушать ответы от шаттлов"""
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
"""
Менеджер постоянных TCP-соединений с шаттлами
"""
import asyncio
import logging
from typing import Dict, Tuple, Optional
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import CONNECTION_HEARTBEAT_INTERVAL

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Класс для управления постоянными TCP-соединениями с шаттлами.
    Поддерживает соединения активными и переподключается при необходимости.
    """
    def __init__(self, heartbeat_interval: int = CONNECTION_HEARTBEAT_INTERVAL):
        self.connections: Dict[str, Dict] = {}  # {ip: {'writer': writer, 'reader': reader, 'last_activity': timestamp}}
        self.locks: Dict[str, asyncio.Lock] = {}  # Блокировки для синхронизации доступа к соединениям
        self.heartbeat_interval = heartbeat_interval  # Интервал проверки соединений в секундах
        self.heartbeat_task = None
        
    async def get_connection(self, ip: str, port: int) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """
        Получить существующее соединение или создать новое
        
        Args:
            ip: IP-адрес шаттла
            port: Порт для подключения
            
        Returns:
            Tuple[asyncio.StreamReader, asyncio.StreamWriter]: Пара (reader, writer)
        """
        # Создаем блокировку для IP, если её еще нет
        if ip not in self.locks:
            self.locks[ip] = asyncio.Lock()
            
        # Используем блокировку для предотвращения одновременного создания нескольких соединений
        async with self.locks[ip]:
            # Проверяем существующее соединение
            if ip in self.connections:
                conn = self.connections[ip]
                writer = conn.get('writer')
                
                # Проверяем, что соединение все еще активно
                if writer and not writer.is_closing():
                    # Обновляем время последней активности
                    conn['last_activity'] = time.time()
                    return conn['reader'], conn['writer']
                else:
                    # Если соединение закрыто, удаляем его
                    await self._close_connection(ip)
            
            # Создаем новое соединение
            try:
                logger.debug(f"Создание нового TCP-соединения с {ip}:{port}")
                reader, writer = await asyncio.open_connection(ip, port)
                
                # Сохраняем соединение
                self.connections[ip] = {
                    'reader': reader,
                    'writer': writer,
                    'last_activity': time.time()
                }
                
                # Запускаем задачу проверки соединений, если она еще не запущена
                if self.heartbeat_task is None or self.heartbeat_task.done():
                    self.heartbeat_task = asyncio.create_task(self._heartbeat_connections())
                
                return reader, writer
            except Exception as e:
                logger.error(f"Ошибка создания соединения с {ip}:{port}: {e}")
                raise
    
    async def send_command(self, ip: str, port: int, command: str) -> bool:
        """
        Отправить команду через существующее или новое соединение
        
        Args:
            ip: IP-адрес шаттла
            port: Порт для подключения
            command: Команда для отправки
            
        Returns:
            bool: True если команда успешно отправлена, иначе False
        """
        try:
            # Получаем соединение
            reader, writer = await self.get_connection(ip, port)
            
            # Отправляем команду
            if not command.endswith('\r\n'):
                command += '\r\n'
                
            writer.write(command.encode())
            await writer.drain()
            
            # Обновляем время последней активности
            self.connections[ip]['last_activity'] = time.time()
            
            logger.debug(f"Команда отправлена через постоянное соединение: {command.strip()} -> {ip}:{port}")
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки команды {command} шаттлу {ip}:{port}: {e}")
            
            # Пытаемся закрыть соединение при ошибке
            await self._close_connection(ip)
            return False
    
    async def _close_connection(self, ip: str):
        """
        Закрыть соединение с шаттлом
        
        Args:
            ip: IP-адрес шаттла
        """
        if ip in self.connections:
            conn = self.connections[ip]
            writer = conn.get('writer')
            
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                    logger.debug(f"Соединение с {ip} закрыто")
                except Exception as e:
                    logger.error(f"Ошибка при закрытии соединения с {ip}: {e}")
            
            # Удаляем соединение из словаря
            del self.connections[ip]
    
    async def _heartbeat_connections(self):
        """
        Периодическая проверка и поддержание соединений
        """
        logger.info("Запущена задача проверки соединений")
        
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                current_time = time.time()
                ips_to_check = list(self.connections.keys())
                
                for ip in ips_to_check:
                    try:
                        conn = self.connections.get(ip)
                        if not conn:
                            continue
                            
                        # Проверяем время последней активности
                        last_activity = conn.get('last_activity', 0)
                        writer = conn.get('writer')
                        
                        # Если соединение неактивно более 2 интервалов или закрыто
                        if (current_time - last_activity > self.heartbeat_interval * 2) or \
                           (writer and writer.is_closing()):
                            logger.debug(f"Соединение с {ip} неактивно, закрываем")
                            await self._close_connection(ip)
                        # Если соединение неактивно более 1 интервала, отправляем пинг
                        elif current_time - last_activity > self.heartbeat_interval:
                            # Отправляем пустую команду для поддержания соединения
                            if writer and not writer.is_closing():
                                try:
                                    writer.write(b'\r\n')
                                    await writer.drain()
                                    conn['last_activity'] = current_time
                                    logger.debug(f"Отправлен пинг для поддержания соединения с {ip}")
                                except Exception as e:
                                    logger.error(f"Ошибка отправки пинга шаттлу {ip}: {e}")
                                    await self._close_connection(ip)
                    except Exception as e:
                        logger.error(f"Ошибка проверки соединения с {ip}: {e}")
            
            except asyncio.CancelledError:
                logger.info("Задача проверки соединений отменена")
                break
            except Exception as e:
                logger.error(f"Ошибка в задаче проверки соединений: {e}")
                await asyncio.sleep(5)  # Короткая пауза перед следующей попыткой
    
    async def close_all(self):
        """
        Закрыть все соединения
        """
        logger.info("Закрытие всех соединений")
        
        # Отменяем задачу проверки соединений
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Закрываем все соединения
        for ip in list(self.connections.keys()):
            await self._close_connection(ip)
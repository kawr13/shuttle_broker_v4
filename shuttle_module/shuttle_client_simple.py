import asyncio
import time
import logging
from typing import Optional, Dict, Any, Callable, List, Union

from core.config import get_config, ShuttleConfig

logger = logging.getLogger(__name__)

class ShuttleClientSimple:
    """Упрощенный клиент для взаимодействия с шаттлом, использующий только терминатор CRLF"""
    
    def __init__(self, shuttle_id: str, config: Optional[ShuttleConfig] = None):
        self.shuttle_id = shuttle_id
        self.config = config or get_config().shuttles.get(shuttle_id)
        if not self.config:
            raise ValueError(f"Конфигурация для шаттла {shuttle_id} не найдена")
        
        self.writer: Optional[asyncio.StreamWriter] = None
        self.reader: Optional[asyncio.StreamReader] = None
        self.connected = False
        self.message_handlers: List[Callable[[str], Any]] = []
        self.last_command_time = 0
        self.last_command = None
        self.last_status = "UNKNOWN"
        self.last_seen = 0
        self.error_code = None
    
    async def connect(self) -> bool:
        """Устанавливает соединение с шаттлом"""
        if self.connected:
            return True
        
        try:
            config = get_config()
            
            logger.info(f"Подключение к шаттлу {self.shuttle_id} ({self.config.host}:{self.config.command_port})")
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.config.host, self.config.command_port),
                timeout=config.tcp_connect_timeout
            )
            
            self.connected = True
            logger.info(f"Соединение с шаттлом {self.shuttle_id} установлено")
            
            # Регистрируем шаттл в слушателе
            from shuttle_module.shuttle_listener_v2 import get_shuttle_listener
            shuttle_listener = get_shuttle_listener()
            shuttle_listener.register_shuttle(self.shuttle_id, self.config.host)
            
            # Запрашиваем статус шаттла при подключении
            await self.send_command("STATUS")
            
            return True
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при подключении к шаттлу {self.shuttle_id} ({self.config.host}:{self.config.command_port})")
            self.error_code = "CONNECTION_TIMEOUT"
            return False
        except ConnectionRefusedError:
            logger.error(f"Соединение отклонено шаттлом {self.shuttle_id} ({self.config.host}:{self.config.command_port})")
            self.error_code = "CONNECTION_REFUSED"
            return False
        except Exception as e:
            logger.error(f"Ошибка при подключении к шаттлу {self.shuttle_id}: {e}")
            self.error_code = f"CONNECTION_ERROR: {str(e)}"
            return False
    
    async def disconnect(self):
        """Закрывает соединение с шаттлом"""
        if not self.connected:
            return
        
        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
            
            self.writer = None
            self.reader = None
            self.connected = False
            logger.info(f"Соединение с шаттлом {self.shuttle_id} закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения с шаттлом {self.shuttle_id}: {e}")
    
    async def send_command(self, command: str, params: str = None) -> bool:
        """
        Отправляет команду шаттлу с терминатором CRLF
        
        Args:
            command: Команда для отправки
            params: Параметры команды
            
        Returns:
            bool: Успешность отправки команды
        """
        if not self.connected:
            success = await self.connect()
            if not success:
                return False
        
        try:
            # Формируем команду
            cmd_str = command
            if params:
                cmd_str = f"{cmd_str}-{params}"
            
            # Формируем полную команду с терминатором CRLF
            full_command = f"{cmd_str}\r\n".encode('ascii')
            
            # Отправляем команду
            self.writer.write(full_command)
            await self.writer.drain()
            
            # Логируем отправку
            logger.info(f"Команда '{cmd_str}' отправлена шаттлу {self.shuttle_id}, HEX: {full_command.hex()}")
            
            # Обновляем информацию о последней команде
            self.last_command = cmd_str
            self.last_command_time = time.time()
            
            return True
        except Exception as e:
            logger.error(f"Ошибка при отправке команды шаттлу {self.shuttle_id}: {e}")
            self.error_code = f"SEND_ERROR: {str(e)}"
            await self.disconnect()  # Закрываем соединение при ошибке
            return False
    
    def process_message(self, message: str, message_hex: str):
        """
        Обрабатывает сообщение от шаттла
        
        Args:
            message: Текст сообщения
            message_hex: Шестнадцатеричное представление сообщения
        """
        # Обновляем время последнего сообщения
        self.last_seen = time.time()
        
        # Логируем сообщение
        logger.info(f"Обработка сообщения от шаттла {self.shuttle_id}: '{message}', HEX: {message_hex}")
        
        # Обрабатываем различные типы сообщений
        if message.startswith("STATUS="):
            status_value = message.split("=", 1)[1].strip()
            self.last_status = status_value
            logger.info(f"Шаттл {self.shuttle_id} сообщил статус: {status_value}")
        
        elif message.startswith("LOC="):
            location = message.split("=", 1)[1].strip()
            logger.info(f"Шаттл {self.shuttle_id} сообщил местоположение: {location}")
        
        elif message.startswith("F_CODE="):
            error_code = message.split("=", 1)[1].strip()
            self.error_code = error_code
            logger.warning(f"Шаттл {self.shuttle_id} сообщил код ошибки: {error_code}")
        
        elif "_DONE" in message:
            operation = message.split("_DONE")[0].strip()
            logger.info(f"Шаттл {self.shuttle_id} завершил операцию: {operation}")
            self.last_status = "FREE"
        
        elif "_STARTED" in message:
            operation = message.split("_STARTED")[0].strip()
            logger.info(f"Шаттл {self.shuttle_id} начал операцию: {operation}")
            self.last_status = "BUSY"
        
        elif "_ABORT" in message:
            operation = message.split("_ABORT")[0].strip()
            logger.warning(f"Шаттл {self.shuttle_id} прервал операцию: {operation}")
            self.error_code = f"OPERATION_ABORTED: {message}"
        
        # Вызываем обработчики сообщений
        for handler in self.message_handlers:
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Ошибка в обработчике сообщений для шаттла {self.shuttle_id}: {e}")
    
    def add_message_handler(self, handler: Callable[[str], Any]):
        """Добавляет обработчик сообщений от шаттла"""
        self.message_handlers.append(handler)
    
    def remove_message_handler(self, handler: Callable[[str], Any]):
        """Удаляет обработчик сообщений от шаттла"""
        if handler in self.message_handlers:
            self.message_handlers.remove(handler)
    
    def get_status(self) -> str:
        """Возвращает текущий статус шаттла"""
        return self.last_status
    
    def is_busy(self) -> bool:
        """Проверяет, занят ли шаттл"""
        return self.last_status == "BUSY"
    
    def is_error(self) -> bool:
        """Проверяет, находится ли шаттл в состоянии ошибки"""
        return self.last_status == "ERROR" or self.error_code is not None
    
    def get_error(self) -> Optional[str]:
        """Возвращает код ошибки шаттла"""
        return self.error_code
    
    def clear_error(self):
        """Сбрасывает код ошибки шаттла"""
        self.error_code = None
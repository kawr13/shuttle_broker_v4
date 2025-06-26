import asyncio
import logging
from typing import Dict, Callable, Any, Optional, Set

from core.config import get_config

logger = logging.getLogger(__name__)

class ShuttleListenerV2:
    """Сервер для прослушивания сообщений от шаттлов на порту 8181"""
    
    # Константы протокола
    MESSAGE_LENGTH = 20  # Фиксированная длина сообщений (20 байт)
    ENCODING = 'ascii'   # Кодировка сообщений
    
    def __init__(self):
        self.server = None
        self.running = False
        self.shuttle_ips: Dict[str, str] = {}  # shuttle_id -> IP
        self.ip_to_shuttle: Dict[str, str] = {}  # IP -> shuttle_id
        self.message_handlers: Dict[str, Callable[[str, str], Any]] = {}  # shuttle_id -> handler
        self.connections: Dict[str, asyncio.StreamWriter] = {}  # shuttle_id -> writer
    
    async def start(self):
        """Запускает сервер для прослушивания сообщений от шаттлов"""
        if self.running:
            return
        
        config = get_config()
        try:
            self.server = await asyncio.start_server(
                self._handle_connection,
                '0.0.0.0',  # Слушаем на всех интерфейсах
                config.shuttle_listener_port,
            )
            
            addr = self.server.sockets[0].getsockname()
            logger.info(f"Шлюз слушает шаттлы на {addr[0]}:{addr[1]}")
            
            self.running = True
            asyncio.create_task(self._serve_forever())
        except Exception as e:
            logger.error(f"Ошибка при запуске сервера для прослушивания шаттлов: {e}")
    
    async def stop(self):
        """Останавливает сервер"""
        if not self.running:
            return
        
        self.running = False
        
        # Закрываем все соединения
        for shuttle_id, writer in self.connections.items():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logger.error(f"Ошибка при закрытии соединения с шаттлом {shuttle_id}: {e}")
        
        # Закрываем сервер
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Сервер для прослушивания шаттлов остановлен")
    
    async def _serve_forever(self):
        """Запускает сервер в бесконечном цикле"""
        async with self.server:
            await self.server.serve_forever()
    
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Обрабатывает новое соединение от шаттла"""
        peer_name = writer.get_extra_info('peername')
        shuttle_ip, shuttle_port = peer_name
        logger.info(f"Новое соединение от {shuttle_ip}:{shuttle_port}")
        
        # Определяем ID шаттла по IP-адресу
        shuttle_id = self.ip_to_shuttle.get(shuttle_ip)
        if not shuttle_id:
            # Для отладки принимаем любые подключения и назначаем им простое имя
            last_octet = shuttle_ip.split('.')[-1]
            shuttle_id = f"shuttle_{last_octet}"
            logger.warning(f"Неизвестный шаттл с IP {shuttle_ip}. Назначен ID: {shuttle_id}")
            
            # Регистрируем временный обработчик сообщений
            self.register_message_handler(shuttle_id, self._handle_unknown_shuttle_message)
        
        # Сохраняем соединение
        self.connections[shuttle_id] = writer
        
        try:
            while self.running:
                try:
                    # Читаем ровно 20 байт (фиксированная длина сообщения)
                    data = await reader.readexactly(self.MESSAGE_LENGTH)
                    if not data:
                        logger.warning(f"Соединение с шаттлом {shuttle_id} закрыто")
                        break
                    
                    # Декодируем сообщение
                    try:
                        message = data.decode(self.ENCODING, errors='ignore').strip()
                        message_hex = data.hex()
                        logger.info(f"Получено сообщение от шаттла {shuttle_id}: '{message}', HEX: {message_hex}")
                        
                        # Вызываем обработчик сообщений для этого шаттла
                        if shuttle_id in self.message_handlers:
                            try:
                                await self.message_handlers[shuttle_id](message, message_hex)
                            except Exception as e:
                                logger.error(f"Ошибка в обработчике сообщений для шаттла {shuttle_id}: {e}")
                    except UnicodeDecodeError:
                        # Если не ASCII, выводим в сыром виде
                        logger.info(f"Получено сырое сообщение от шаттла {shuttle_id}: {data.hex()}")
                        
                except asyncio.IncompleteReadError:
                    logger.warning(f"Неполное чтение данных от шаттла {shuttle_id}")
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Ошибка при чтении данных от шаттла {shuttle_id}: {e}")
                    break
        finally:
            # Удаляем соединение из списка
            if shuttle_id in self.connections:
                del self.connections[shuttle_id]
            
            # Закрываем соединение
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            
            logger.info(f"Соединение с шаттлом {shuttle_id} закрыто")
    
    async def _handle_unknown_shuttle_message(self, message: str, message_hex: str):
        """Обрабатывает сообщения от неизвестных шаттлов"""
        logger.info(f"Получено сообщение от неизвестного шаттла: '{message}', HEX: {message_hex}")
    
    def register_shuttle(self, shuttle_id: str, ip: str):
        """Регистрирует шаттл для прослушивания"""
        self.shuttle_ips[shuttle_id] = ip
        self.ip_to_shuttle[ip] = shuttle_id
        logger.info(f"Шаттл {shuttle_id} зарегистрирован с IP {ip}")
    
    def unregister_shuttle(self, shuttle_id: str):
        """Удаляет регистрацию шаттла"""
        if shuttle_id in self.shuttle_ips:
            ip = self.shuttle_ips[shuttle_id]
            del self.shuttle_ips[shuttle_id]
            if ip in self.ip_to_shuttle:
                del self.ip_to_shuttle[ip]
            logger.info(f"Шаттл {shuttle_id} удален из регистрации")
    
    def register_message_handler(self, shuttle_id: str, handler: Callable[[str, str], Any]):
        """Регистрирует обработчик сообщений для шаттла"""
        self.message_handlers[shuttle_id] = handler
        logger.debug(f"Зарегистрирован обработчик сообщений для шаттла {shuttle_id}")
    
    def unregister_message_handler(self, shuttle_id: str):
        """Удаляет обработчик сообщений для шаттла"""
        if shuttle_id in self.message_handlers:
            del self.message_handlers[shuttle_id]
            logger.debug(f"Удален обработчик сообщений для шаттла {shuttle_id}")


# Глобальный экземпляр слушателя шаттлов
shuttle_listener = None

def get_shuttle_listener() -> ShuttleListenerV2:
    """Возвращает глобальный экземпляр слушателя шаттлов"""
    global shuttle_listener
    if shuttle_listener is None:
        shuttle_listener = ShuttleListenerV2()
    return shuttle_listener
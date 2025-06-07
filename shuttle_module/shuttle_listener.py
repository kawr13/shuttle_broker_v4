import asyncio
from typing import Dict, Callable, Any, Optional

from core.config import get_config
from core.logging import get_logger

logger = get_logger()


class ShuttleListener:
    """Сервер для прослушивания сообщений от шаттлов"""
    
    def __init__(self):
        self.server = None
        self.running = False
        self.message_handlers: Dict[str, Callable[[str, str], Any]] = {}
        self.connections: Dict[str, asyncio.StreamWriter] = {}
    
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
        shuttle_id = self._get_shuttle_id_by_ip(shuttle_ip)
        if not shuttle_id:
            # Для отладки принимаем любые подключения и назначаем им временный ID
            shuttle_id = f"temp_shuttle_{shuttle_ip.replace('.', '_')}"
            logger.warning(f"Неизвестный шаттл с IP {shuttle_ip}. Назначен временный ID: {shuttle_id}")
            
            # Регистрируем временный обработчик сообщений
            self.register_message_handler(shuttle_id, self._handle_unknown_shuttle_message)
        
        # Сохраняем соединение
        self.connections[shuttle_id] = writer
        
        try:
            while self.running:
                try:
                    # Читаем данные от шаттла
                    config = get_config()
                    data = await asyncio.wait_for(
                        reader.readuntil(b'\n'),
                        timeout=config.tcp_read_timeout
                    )
                    message = data.decode('utf-8').strip()
                    logger.info(f"Получено сообщение от шаттла {shuttle_id}: '{message}'")
                    
                    # Вызываем обработчик сообщений для этого шаттла
                    if shuttle_id in self.message_handlers:
                        try:
                            await self.message_handlers[shuttle_id](shuttle_id, message)
                        except Exception as e:
                            logger.error(f"Ошибка в обработчике сообщений для шаттла {shuttle_id}: {e}")
                except asyncio.TimeoutError:
                    # Проверяем, что соединение все еще активно
                    try:
                        writer.write(b"PING\n")
                        await writer.drain()
                    except Exception:
                        logger.warning(f"Соединение с шаттлом {shuttle_id} потеряно")
                        break
                except asyncio.IncompleteReadError:
                    logger.warning(f"Соединение с шаттлом {shuttle_id} закрыто")
                    break
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
    
    def _get_shuttle_id_by_ip(self, ip: str) -> Optional[str]:
        """Определяет ID шаттла по IP-адресу"""
        config = get_config()
        
        # Отладочный вывод
        logger.debug(f"Доступные шаттлы в конфигурации: {list(config.shuttles.keys())}")
        for shuttle_id, shuttle_config in config.shuttles.items():
            logger.debug(f"Шаттл {shuttle_id}: host={shuttle_config.host}")
        
        # Проверяем точное совпадение IP
        for shuttle_id, shuttle_config in config.shuttles.items():
            if shuttle_config.host == ip:
                logger.debug(f"Найдено точное совпадение для IP {ip}: шаттл {shuttle_id}")
                return shuttle_id
        
        # Для локальных подключений (127.0.0.1, localhost)
        if ip == "127.0.0.1" or ip == "::1" or ip == "localhost":
            # Ищем шаттл с локальным адресом
            for shuttle_id, shuttle_config in config.shuttles.items():
                if shuttle_config.host == "127.0.0.1" or shuttle_config.host == "localhost":
                    logger.info(f"Определен локальный шаттл {shuttle_id} для IP {ip}")
                    return shuttle_id
            
            # Если локальный шаттл не найден, но подключение локальное,
            # возвращаем первый виртуальный шаттл
            for shuttle_id in config.shuttles:
                if shuttle_id.startswith("virtual"):
                    logger.info(f"Выбран виртуальный шаттл {shuttle_id} для локального подключения {ip}")
                    return shuttle_id
        
        logger.warning(f"Не удалось определить ID шаттла для IP {ip}")
        return None
    
    async def _handle_unknown_shuttle_message(self, shuttle_id: str, message: str):
        """Обрабатывает сообщения от неизвестных шаттлов"""
        logger.info(f"Получено сообщение от неизвестного шаттла {shuttle_id}: '{message}'")
        
        # Отправляем MRCD в ответ на любое сообщение
        if message != "MRCD":
            await self.send_message(shuttle_id, "MRCD")
            logger.info(f"Отправлен MRCD неизвестному шаттлу {shuttle_id}")
        
        # Если шаттл отправляет статус, можно попытаться добавить его в конфигурацию
        if message.startswith("STATUS="):
            logger.info(f"Неизвестный шаттл {shuttle_id} сообщает статус: {message}")
            # Здесь можно добавить логику для автоматического добавления шаттла в конфигурацию
    
    def register_message_handler(self, shuttle_id: str, handler: Callable[[str, str], Any]):
        """Регистрирует обработчик сообщений для шаттла"""
        self.message_handlers[shuttle_id] = handler
    
    def unregister_message_handler(self, shuttle_id: str):
        """Удаляет обработчик сообщений для шаттла"""
        if shuttle_id in self.message_handlers:
            del self.message_handlers[shuttle_id]
    
    async def send_message(self, shuttle_id: str, message: str) -> bool:
        """Отправляет сообщение шаттлу"""
        if shuttle_id not in self.connections:
            logger.warning(f"Шаттл {shuttle_id} не подключен")
            return False
        
        try:
            writer = self.connections[shuttle_id]
            
            # Добавляем перевод строки, если его нет
            if not message.endswith('\n'):
                message += '\n'
            
            writer.write(message.encode('utf-8'))
            await writer.drain()
            logger.info(f"Сообщение '{message.strip()}' отправлено шаттлу {shuttle_id}")
            return True
        except ConnectionResetError:
            logger.error(f"Соединение с шаттлом {shuttle_id} сброшено")
            # Удаляем соединение из списка
            if shuttle_id in self.connections:
                del self.connections[shuttle_id]
            return False
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения шаттлу {shuttle_id}: {e}")
            return False


# Глобальный экземпляр слушателя шаттлов
shuttle_listener = None


def get_shuttle_listener() -> ShuttleListener:
    """Возвращает глобальный экземпляр слушателя шаттлов"""
    global shuttle_listener
    if shuttle_listener is None:
        shuttle_listener = ShuttleListener()
    return shuttle_listener
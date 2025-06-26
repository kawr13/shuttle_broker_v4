import asyncio
from typing import Dict, Callable, Any, Optional

from core.config import get_config, add_shuttle_to_config
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
            # Для отладки принимаем любые подключения и назначаем им простое имя
            last_octet = shuttle_ip.split('.')[-1]
            shuttle_id = f"shuttle_{last_octet}"
            logger.warning(f"Неизвестный шаттл с IP {shuttle_ip}. Назначен ID: {shuttle_id}")
            
            # Регистрируем временный обработчик сообщений
            self.register_message_handler(shuttle_id, self._handle_unknown_shuttle_message)
        
        # Сохраняем соединение
        self.connections[shuttle_id] = writer
        
        # Регистрируем соединение в менеджере соединений
        from shuttle_module.connection_manager import get_connection_manager
        connection_manager = get_connection_manager()
        connection_manager.register_connection(shuttle_id, reader, writer)
        
        try:
            while self.running:
                try:
                    # Читаем данные от шаттла (изменено с readuntil на read)
                    data = await reader.read(1024)
                    if not data:
                        logger.warning(f"Соединение с шаттлом {shuttle_id} закрыто")
                        break
                    
                    # Декодируем сообщение
                    try:
                        message = data.decode('utf-8').strip()
                        logger.info(f"Получено сообщение от шаттла {shuttle_id}: '{message}'")
                        
                        # Вызываем обработчик сообщений для этого шаттла
                        if shuttle_id in self.message_handlers:
                            try:
                                await self.message_handlers[shuttle_id](shuttle_id, message)
                            except Exception as e:
                                logger.error(f"Ошибка в обработчике сообщений для шаттла {shuttle_id}: {e}")
                    except UnicodeDecodeError:
                        # Если не UTF-8, выводим в сыром виде
                        logger.info(f"Получено сырое сообщение от шаттла {shuttle_id}: {data.hex()}")
                        
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
        
        # Если шаттл отправляет статус, добавляем его в конфигурацию
        if message.startswith("STATUS="):
            logger.info(f"Неизвестный шаттл {shuttle_id} сообщает статус: {message}")
            
            # Добавляем шаттл в конфигурацию и сохраняем в файл
            try:
                # Получаем IP шаттла
                peer_name = self.connections[shuttle_id].get_extra_info('peername')
                shuttle_ip = peer_name[0] if peer_name else "10.181.80.134"  # IP по умолчанию
                
                # Добавляем шаттл в конфигурацию и сохраняем в файл
                from core.config import add_shuttle_to_config, load_config
                config_saved = add_shuttle_to_config(shuttle_id, shuttle_ip, 'Главный')
                
                if config_saved:
                    logger.info(f"Шаттл {shuttle_id} с IP {shuttle_ip} добавлен в конфигурацию и сохранен в файл config.yaml")
                    
                    # Перезагружаем конфигурацию из файла
                    load_config('config.yaml')
                    
                    # Добавляем шаттл в менеджер
                    from shuttle_module.shuttle_manager import get_shuttle_manager
                    shuttle_manager = get_shuttle_manager()
                    
                    # Создаем конфигурацию шаттла
                    from core.config import ShuttleConfig
                    shuttle_config = ShuttleConfig(
                        host=shuttle_ip,
                        command_port=2000,
                        response_port=5000,
                        shuttle_health_check_interval=10
                    )
                    
                    # Добавляем шаттл в менеджер
                    await shuttle_manager.add_shuttle(shuttle_id, shuttle_config)
                    logger.info(f"Шаттл {shuttle_id} добавлен в менеджер шаттлов")
                else:
                    logger.warning(f"Не удалось сохранить шаттл {shuttle_id} в конфигурацию")
                
                # Запрашиваем статус шаттла
                await self.send_message(shuttle_id, "STATUS")
                logger.info(f"Запрошен статус шаттла {shuttle_id}")
                
            except Exception as e:
                logger.error(f"Ошибка при добавлении шаттла {shuttle_id} в конфигурацию: {e}")
        
        # Запрашиваем местоположение шаттла
        await self.send_message(shuttle_id, "LOC")
        logger.info(f"Запрошено местоположение шаттла {shuttle_id}")
    
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
            
            # Добавляем терминатор CRLF (\r\n), если его нет
            if not message.endswith('\r\n'):
                message = message.rstrip('\n')  # Удаляем существующий LF, если есть
                message += '\r\n'
            
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
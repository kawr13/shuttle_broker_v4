import asyncio
import time
from typing import Optional, Dict, Any, Callable, List, Tuple

from core.config import get_config, ShuttleConfig
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommandEnum, ShuttleStatus, ShuttleCommand
from shuttle_module.shuttle_state import ShuttleState

logger = get_logger()


class ShuttleClient:
    """Клиент для взаимодействия с шаттлом"""
    
    def __init__(self, shuttle_id: str, config: Optional[ShuttleConfig] = None):
        self.shuttle_id = shuttle_id
        self.config = config or get_config().shuttles.get(shuttle_id)
        if not self.config:
            raise ValueError(f"Конфигурация для шаттла {shuttle_id} не найдена")
        
        self.state = ShuttleState(shuttle_id=shuttle_id)
        self.writer: Optional[asyncio.StreamWriter] = None
        self.reader: Optional[asyncio.StreamReader] = None
        self.connected = False
        self.message_handlers: List[Callable[[str], Any]] = []
        self._connection_task = None
    
    async def connect(self) -> bool:
        """Устанавливает соединение с шаттлом"""
        if self.connected:
            return True
        
        try:
            config = get_config()
            
            # Получаем соединение через менеджер
            from shuttle_module.connection_manager import get_connection_manager
            connection_manager = get_connection_manager()
            
            self.reader, self.writer = await connection_manager.get_connection(
                self.shuttle_id,
                self.config.host,
                self.config.command_port,
                config.tcp_connect_timeout
            )
            
            self.connected = True
            
            # Регистрируем обработчик сообщений в ShuttleListener
            from shuttle_module.shuttle_listener import get_shuttle_listener
            shuttle_listener = get_shuttle_listener()
            shuttle_listener.register_message_handler(self.shuttle_id, self._process_message_from_listener)
            
            # Запрашиваем статус шаттла при подключении
            await self._request_shuttle_status()
            
            return True
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при подключении к шаттлу {self.shuttle_id} ({self.config.host}:{self.config.command_port})")
            self.state.status = ShuttleStatus.ERROR
            self.state.error_code = "CONNECTION_TIMEOUT"
            return False
        except ConnectionRefusedError:
            logger.error(f"Соединение отклонено шаттлом {self.shuttle_id} ({self.config.host}:{self.config.command_port})")
            self.state.status = ShuttleStatus.ERROR
            self.state.error_code = "CONNECTION_REFUSED"
            return False
        except Exception as e:
            logger.error(f"Ошибка при подключении к шаттлу {self.shuttle_id}: {e}")
            self.state.status = ShuttleStatus.ERROR
            self.state.error_code = f"CONNECTION_ERROR: {str(e)}"
            return False
    
    async def disconnect(self):
        """Закрывает соединение с шаттлом"""
        if not self.connected:
            return
        
        # Удаляем обработчик сообщений из ShuttleListener
        from shuttle_module.shuttle_listener import get_shuttle_listener
        shuttle_listener = get_shuttle_listener()
        shuttle_listener.unregister_message_handler(self.shuttle_id)
        
        # Закрываем соединение через менеджер
        from shuttle_module.connection_manager import get_connection_manager
        connection_manager = get_connection_manager()
        await connection_manager.close_connection(self.shuttle_id)
        
        self.writer = None
        self.reader = None
        self.connected = False
    
    async def send_command(self, command: ShuttleCommand) -> bool:
        """Отправляет команду шаттлу"""
        if not self.connected:
            success = await self.connect()
            if not success:
                return False
        
        command_str = command.to_string()
        try:
            config = get_config()
            self.writer.write(command_str.encode('utf-8'))
            await asyncio.wait_for(self.writer.drain(), timeout=config.tcp_write_timeout)
            
            logger.info(f"Команда '{command_str.strip()}' отправлена шаттлу {self.shuttle_id}")
            
            # Обновляем состояние шаттла
            self.state.last_command = command
            self.state.last_command_time = time.time()
            
            # Если это не MRCD, то шаттл переходит в состояние BUSY
            if command.command_type != ShuttleCommandEnum.MRCD:
                self.state.status = ShuttleStatus.BUSY
            
            return True
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при отправке команды шаттлу {self.shuttle_id}")
            self.state.status = ShuttleStatus.ERROR
            self.state.error_code = "SEND_TIMEOUT"
            return False
        except Exception as e:
            logger.error(f"Ошибка при отправке команды шаттлу {self.shuttle_id}: {e}")
            self.state.status = ShuttleStatus.ERROR
            self.state.error_code = f"SEND_ERROR: {str(e)}"
            await self.disconnect()  # Закрываем соединение при ошибке
            return False
    
    async def _listen_for_messages(self):
        """Прослушивает сообщения от шаттла"""
        config = get_config()
        
        while True:
            try:
                data = await asyncio.wait_for(
                    self.reader.readuntil(b'\n'),
                    timeout=config.tcp_read_timeout
                )
                message = data.decode('utf-8').strip()
                logger.info(f"Получено сообщение от шаттла {self.shuttle_id}: '{message}'")
                
                # Обновляем время последнего сообщения
                self.state.last_seen = time.time()
                
                # Обрабатываем сообщение
                await self._process_message(message)
                
                # Вызываем обработчики сообщений
                for handler in self.message_handlers:
                    try:
                        handler(message)
                    except Exception as e:
                        logger.error(f"Ошибка в обработчике сообщений для шаттла {self.shuttle_id}: {e}")
                
                # Отправляем MRCD в ответ на сообщение, если это не MRCD
                if message != ShuttleCommandEnum.MRCD.value:
                    mrcd_command = ShuttleCommand(
                        command_type=ShuttleCommandEnum.MRCD,
                        shuttle_id=self.shuttle_id
                    )
                    await self.send_command(mrcd_command)
            except asyncio.TimeoutError:
                # Проверяем, не прошло ли слишком много времени с последнего сообщения
                if time.time() - self.state.last_seen > 30:  # 30 секунд таймаут
                    logger.warning(f"Шаттл {self.shuttle_id} не отвечает более 30 секунд")
                    self.state.status = ShuttleStatus.ERROR
                    self.state.error_code = "NO_RESPONSE_TIMEOUT"
                    await self.disconnect()
                    break
            except asyncio.IncompleteReadError:
                logger.warning(f"Соединение с шаттлом {self.shuttle_id} закрыто")
                await self.disconnect()
                break
            except asyncio.CancelledError:
                logger.info(f"Прослушивание сообщений от шаттла {self.shuttle_id} отменено")
                break
            except Exception as e:
                logger.error(f"Ошибка при прослушивании сообщений от шаттла {self.shuttle_id}: {e}")
                await self.disconnect()
                break
    
    async def _process_message(self, message: str):
        """Обрабатывает сообщение от шаттла"""
        # Добавляем подробное логирование
        logger.info(f"Обработка сообщения от шаттла {self.shuttle_id}: '{message}'")
        
        # Обновляем состояние шаттла на основе сообщения
        if message.endswith("_STARTED"):
            self.state.status = ShuttleStatus.BUSY
            logger.info(f"Шаттл {self.shuttle_id} начал выполнение операции: {message}")
            
            # Определяем тип операции
            if "PALLET_IN" in message:
                self.state.status = ShuttleStatus.LOADING
                logger.info(f"Шаттл {self.shuttle_id} начал загрузку паллеты")
            elif "PALLET_OUT" in message:
                self.state.status = ShuttleStatus.UNLOADING
                logger.info(f"Шаттл {self.shuttle_id} начал выгрузку паллеты")
            elif "HOME" in message:
                self.state.status = ShuttleStatus.MOVING
                logger.info(f"Шаттл {self.shuttle_id} начал движение в домашнюю позицию")
        
        elif message.endswith("_DONE"):
            self.state.status = ShuttleStatus.FREE
            self.state.current_command = None
            logger.info(f"Шаттл {self.shuttle_id} завершил выполнение операции: {message}")
        
        elif message.endswith("_ABORT"):
            self.state.status = ShuttleStatus.ERROR
            self.state.error_code = message
            self.state.current_command = None
            logger.warning(f"Шаттл {self.shuttle_id} прервал выполнение операции: {message}")
        
        elif message.startswith("LOCATION="):
            location_data = message.split("=", 1)[1]
            self.state.location_data = location_data
            self.state.status = ShuttleStatus.FREE
            self.state.current_command = None
            
            # Извлекаем информацию о ячейке из местоположения
            try:
                # Предполагаем, что местоположение содержит информацию о ячейке в формате "CELL:A1"
                if "CELL:" in location_data:
                    cell_info = location_data.split("CELL:", 1)[1].split(",")[0].strip()
                    self.state.current_cell = cell_info
                    logger.info(f"Шаттл {self.shuttle_id} находится в ячейке {cell_info}")
            except Exception as e:
                logger.error(f"Ошибка при извлечении информации о ячейке: {e}")
        
        elif message.startswith("COUNT_") and "=" in message:
            self.state.pallet_count_data = message
            self.state.status = ShuttleStatus.FREE
            self.state.current_command = None
        
        elif message.startswith("STATUS="):
            status_val = message.split("=", 1)[1].upper()
            status_map = {
                "FREE": ShuttleStatus.FREE,
                "CARGO": ShuttleStatus.BUSY,
                "BUSY": ShuttleStatus.BUSY,
                "NOT_READY": ShuttleStatus.NOT_READY,
                "MOVING": ShuttleStatus.MOVING,
                "LOADING": ShuttleStatus.LOADING,
                "UNLOADING": ShuttleStatus.UNLOADING,
                "CHARGING": ShuttleStatus.CHARGING,
                "LOW_BATTERY": ShuttleStatus.LOW_BATTERY
            }
            old_status = self.state.status
            self.state.status = status_map.get(status_val, ShuttleStatus.UNKNOWN)
            
            logger.info(f"Шаттл {self.shuttle_id} сообщил статус: {status_val} (преобразовано в {self.state.status})")
            
            if old_status != self.state.status:
                logger.info(f"Статус шаттла {self.shuttle_id} изменился: {old_status} -> {self.state.status}")
            
            if self.state.status in [ShuttleStatus.FREE, ShuttleStatus.NOT_READY, ShuttleStatus.UNKNOWN]:
                self.state.current_command = None
        
        elif message.startswith("BATTERY="):
            level_str = message.split("=", 1)[1]
            self.state.battery_level = level_str
            logger.info(f"Шаттл {self.shuttle_id} сообщил уровень заряда батареи: {level_str}")
            
            # Проверяем низкий заряд батареи
            try:
                parsed_level = float(level_str.replace('%', '').lstrip('<'))
                if parsed_level < 20:  # Порог низкого заряда
                    self.state.status = ShuttleStatus.LOW_BATTERY
                    logger.warning(f"Низкий уровень заряда батареи шаттла {self.shuttle_id}: {parsed_level}%")
            except ValueError:
                logger.warning(f"Не удалось распарсить уровень заряда батареи: {level_str}")
        
        elif message.startswith("WDH="):
            try:
                hours = int(message.split("=", 1)[1])
                self.state.wdh_hours = hours
                logger.info(f"Шаттл {self.shuttle_id} сообщил количество часов работы: {hours}")
            except ValueError:
                logger.warning(f"Не удалось распарсить количество часов работы: {message}")
        
        elif message.startswith("WLH="):
            try:
                hours = int(message.split("=", 1)[1])
                self.state.wlh_hours = hours
                logger.info(f"Шаттл {self.shuttle_id} сообщил количество часов под нагрузкой: {hours}")
            except ValueError:
                logger.warning(f"Не удалось распарсить количество часов под нагрузкой: {message}")
        
        elif message.startswith("F_CODE="):
            self.state.error_code = message
            self.state.status = ShuttleStatus.ERROR
            self.state.current_command = None
            logger.error(f"Шаттл {self.shuttle_id} сообщил об ошибке: {message}")
    
    def add_message_handler(self, handler: Callable[[str], Any]):
        """Добавляет обработчик сообщений от шаттла"""
        self.message_handlers.append(handler)
    
    def remove_message_handler(self, handler: Callable[[str], Any]):
        """Удаляет обработчик сообщений от шаттла"""
        if handler in self.message_handlers:
            self.message_handlers.remove(handler)
    
    def get_state(self) -> ShuttleState:
        """Возвращает текущее состояние шаттла"""
        return self.state
    async def _process_message_from_listener(self, shuttle_id: str, message: str):
        """Обрабатывает сообщение от шаттла, полученное через ShuttleListener"""
        # Обновляем время последнего сообщения
        self.state.last_seen = time.time()
        self.state.last_message = message
        
        # Добавляем подробное логирование
        logger.info(f"Обработка сообщения от шаттла {shuttle_id}: '{message}'")
        
        # Обрабатываем сообщение
        await self._process_message(message)
        
        # Вызываем обработчики сообщений
        for handler in self.message_handlers:
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Ошибка в обработчике сообщений для шаттла {self.shuttle_id}: {e}")
        
        # Отправляем MRCD в ответ на сообщение, если это не MRCD
        if message != ShuttleCommandEnum.MRCD.value:
            from shuttle_module.shuttle_listener import get_shuttle_listener
            shuttle_listener = get_shuttle_listener()
            await shuttle_listener.send_message(self.shuttle_id, ShuttleCommandEnum.MRCD.value)
            logger.info(f"Отправлен MRCD в ответ на сообщение от шаттла {shuttle_id}: '{message}'")
    async def _request_shuttle_status(self):
        """Запрашивает статус шаттла при подключении"""
        try:
            # Создаем команду STATUS
            status_command = ShuttleCommand(
                command_type=ShuttleCommandEnum.STATUS,
                shuttle_id=self.shuttle_id
            )
            
            # Отправляем команду
            await self.send_command(status_command)
            logger.info(f"Запрошен статус шаттла {self.shuttle_id}")
            
            # Не отправляем дополнительную команду MRCD, чтобы избежать
            # дублирования запросов и лишних сообщений в логах
        except Exception as e:
            logger.error(f"Ошибка при запросе статуса шаттла {self.shuttle_id}: {e}")
            # Не прерываем подключение из-за ошибки запроса статуса
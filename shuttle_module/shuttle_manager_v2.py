import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Callable

from core.config import get_config
from shuttle_module.shuttle_client_v2 import ShuttleClientV2
from shuttle_module.shuttle_listener_v2 import get_shuttle_listener

logger = logging.getLogger(__name__)

class ShuttleManagerV2:
    """Менеджер для управления шаттлами"""
    
    def __init__(self):
        self.shuttles: Dict[str, ShuttleClientV2] = {}
        self.running = False
        self.health_check_task = None
    
    async def start(self):
        """Запускает менеджер шаттлов"""
        if self.running:
            return
        
        # Запускаем слушатель сообщений
        listener = get_shuttle_listener()
        await listener.start()
        
        # Инициализируем шаттлы из конфигурации
        config = get_config()
        for shuttle_id, shuttle_config in config.shuttles.items():
            await self.add_shuttle(shuttle_id)
        
        # Запускаем задачу проверки состояния шаттлов
        self.running = True
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info("Менеджер шаттлов запущен")
    
    async def stop(self):
        """Останавливает менеджер шаттлов"""
        if not self.running:
            return
        
        self.running = False
        
        # Отменяем задачу проверки состояния
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        # Отключаем все шаттлы
        for shuttle_id, shuttle in self.shuttles.items():
            await shuttle.disconnect()
        
        # Останавливаем слушатель
        listener = get_shuttle_listener()
        await listener.stop()
        
        logger.info("Менеджер шаттлов остановлен")
    
    async def add_shuttle(self, shuttle_id: str) -> bool:
        """
        Добавляет шаттл в менеджер
        
        Args:
            shuttle_id: ID шаттла
            
        Returns:
            bool: True, если шаттл успешно добавлен, иначе False
        """
        if shuttle_id in self.shuttles:
            logger.warning(f"Шаттл {shuttle_id} уже добавлен в менеджер")
            return True
        
        try:
            # Создаем клиент шаттла
            shuttle = ShuttleClientV2(shuttle_id)
            
            # Регистрируем обработчик сообщений
            listener = get_shuttle_listener()
            listener.register_message_handler(shuttle_id, self._create_message_handler(shuttle_id))
            
            # Добавляем шаттл в словарь
            self.shuttles[shuttle_id] = shuttle
            
            logger.info(f"Шаттл {shuttle_id} добавлен в менеджер")
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении шаттла {shuttle_id}: {e}")
            return False
    
    async def remove_shuttle(self, shuttle_id: str):
        """Удаляет шаттл из менеджера"""
        if shuttle_id not in self.shuttles:
            logger.warning(f"Шаттл {shuttle_id} не найден в менеджере")
            return
        
        # Отключаем шаттл
        shuttle = self.shuttles[shuttle_id]
        await shuttle.disconnect()
        
        # Удаляем обработчик сообщений
        listener = get_shuttle_listener()
        listener.unregister_message_handler(shuttle_id)
        listener.unregister_shuttle(shuttle_id)
        
        # Удаляем шаттл из словаря
        del self.shuttles[shuttle_id]
        
        logger.info(f"Шаттл {shuttle_id} удален из менеджера")
    
    async def send_command(self, shuttle_id: str, command: str, params: str = None, 
                          use_fixed_length: bool = True, max_retries: int = 3) -> bool:
        """
        Отправляет команду шаттлу с поддержкой повторных попыток
        
        Args:
            shuttle_id: ID шаттла
            command: Команда для отправки
            params: Параметры команды
            use_fixed_length: Использовать фиксированную длину (True) или терминатор CRLF (False)
            max_retries: Максимальное количество повторных попыток
            
        Returns:
            bool: True, если команда успешно отправлена, иначе False
        """
        if shuttle_id not in self.shuttles:
            logger.error(f"Шаттл {shuttle_id} не найден в менеджере")
            return False
        
        shuttle = self.shuttles[shuttle_id]
        
        # Проверяем, не занят ли шаттл
        if shuttle.is_busy() and command != "STATUS":
            logger.warning(f"Шаттл {shuttle_id} занят, команда {command} отложена")
            
            # Ждем, пока шаттл освободится (максимум 30 секунд)
            for _ in range(30):
                await asyncio.sleep(1)
                if not shuttle.is_busy():
                    break
            else:
                logger.error(f"Шаттл {shuttle_id} не освободился за 30 секунд")
                return False
        
        # Отправляем команду с повторными попытками
        for attempt in range(max_retries):
            success = await shuttle.send_command(command, params, use_fixed_length)
            if success:
                return True
            
            logger.warning(f"Попытка {attempt+1}/{max_retries} отправки команды {command} шаттлу {shuttle_id} не удалась")
            await asyncio.sleep(1)  # Ждем 1 секунду перед повторной попыткой
        
        logger.error(f"Не удалось отправить команду {command} шаттлу {shuttle_id} после {max_retries} попыток")
        return False
    
    def get_shuttle(self, shuttle_id: str) -> Optional[ShuttleClientV2]:
        """Возвращает клиент шаттла по ID"""
        return self.shuttles.get(shuttle_id)
    
    def get_all_shuttles(self) -> Dict[str, ShuttleClientV2]:
        """Возвращает словарь всех шаттлов"""
        return self.shuttles.copy()
    
    def get_shuttle_status(self, shuttle_id: str) -> Optional[str]:
        """Возвращает статус шаттла по ID"""
        shuttle = self.shuttles.get(shuttle_id)
        if not shuttle:
            return None
        return shuttle.get_status()
    
    def _create_message_handler(self, shuttle_id: str) -> Callable[[str, str], Any]:
        """Создает обработчик сообщений для шаттла"""
        async def handler(message: str, message_hex: str):
            if shuttle_id in self.shuttles:
                shuttle = self.shuttles[shuttle_id]
                shuttle.process_message(message, message_hex)
        
        return handler
    
    async def _health_check_loop(self):
        """Цикл проверки состояния шаттлов"""
        config = get_config()
        interval = config.shuttle_health_check_interval
        
        logger.info(f"Запущена проверка состояния шаттлов с интервалом {interval} секунд")
        
        while self.running:
            try:
                await self._check_shuttles_health()
            except Exception as e:
                logger.error(f"Ошибка при проверке состояния шаттлов: {e}")
            
            await asyncio.sleep(interval)
    
    async def _check_shuttles_health(self):
        """Проверяет состояние всех шаттлов"""
        for shuttle_id, shuttle in self.shuttles.items():
            try:
                # Если шаттл не подключен, пытаемся подключиться
                if not shuttle.is_connected():
                    await shuttle.connect()
                    continue
                
                # Если шаттл не отвечал более 30 секунд, запрашиваем статус
                if not shuttle.is_alive(30):
                    logger.warning(f"Шаттл {shuttle_id} не отвечал более 30 секунд, запрашиваем статус")
                    await shuttle.send_command("STATUS")
                
                # Если шаттл в состоянии ошибки, логируем это
                if shuttle.is_error():
                    error = shuttle.get_error()
                    logger.warning(f"Шаттл {shuttle_id} в состоянии ошибки: {error}")
            except Exception as e:
                logger.error(f"Ошибка при проверке состояния шаттла {shuttle_id}: {e}")


# Глобальный экземпляр менеджера шаттлов
shuttle_manager = None

def get_shuttle_manager() -> ShuttleManagerV2:
    """Возвращает глобальный экземпляр менеджера шаттлов"""
    global shuttle_manager
    if shuttle_manager is None:
        shuttle_manager = ShuttleManagerV2()
    return shuttle_manager
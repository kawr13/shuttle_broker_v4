import asyncio
import time
from typing import Dict, Set

from core.config import get_config
from core.logging import get_logger
from shuttle_module.shuttle_client import ShuttleClient
from shuttle_module.commands import ShuttleCommandEnum, ShuttleCommand, ShuttleStatus

logger = get_logger()

class ShuttleMonitor:
    """Монитор состояния шаттлов"""
    
    def __init__(self):
        self.running = False
        self.task = None
        self.connected_shuttles: Set[str] = set()
        self.last_check: Dict[str, float] = {}
    
    async def start(self):
        """Запускает монитор шаттлов"""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self._monitor_loop())
        logger.info("Монитор шаттлов запущен")
    
    async def stop(self):
        """Останавливает монитор шаттлов"""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        logger.info("Монитор шаттлов остановлен")
    
    async def _monitor_loop(self):
        """Основной цикл мониторинга шаттлов"""
        config = get_config()
        check_interval = getattr(config, "shuttle_health_check_interval", 30)
        
        while self.running:
            try:
                # Проверяем все шаттлы из конфигурации
                for shuttle_id, shuttle_config in config.shuttles.items():
                    # Пропускаем шаттлы, которые недавно проверялись
                    current_time = time.time()
                    if shuttle_id in self.last_check and current_time - self.last_check[shuttle_id] < check_interval:
                        continue
                    
                    # Проверяем состояние шаттла
                    await self._check_shuttle(shuttle_id)
                    self.last_check[shuttle_id] = current_time
                
                # Ждем до следующей проверки
                await asyncio.sleep(5)  # Проверяем каждые 5 секунд, но не все шаттлы сразу
            except asyncio.CancelledError:
                logger.info("Цикл мониторинга шаттлов отменен")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга шаттлов: {e}")
                await asyncio.sleep(5)  # Ждем немного перед повторной попыткой
    
    async def _check_shuttle(self, shuttle_id: str):
        """Проверяет состояние шаттла и восстанавливает соединение при необходимости"""
        from .shuttle_manager import get_shuttle_manager
        
        shuttle_manager = get_shuttle_manager()
        state = await shuttle_manager.get_shuttle_state(shuttle_id)
        
        if not state:
            logger.warning(f"Не найдено состояние для шаттла {shuttle_id}")
            return
        
        # Если шаттл не подключен или давно не отвечал
        if (state.status == ShuttleStatus.ERROR or 
            state.status == ShuttleStatus.UNKNOWN or 
            time.time() - state.last_seen > 60):  # Если не было активности более 60 секунд
            
            logger.info(f"Шаттл {shuttle_id} не отвечает, пытаемся восстановить соединение")
            
            try:
                # Создаем клиент для шаттла
                shuttle_client = ShuttleClient(shuttle_id)
                
                # Пытаемся подключиться
                connected = await shuttle_client.connect()
                if connected:
                    logger.info(f"Соединение с шаттлом {shuttle_id} восстановлено")
                    
                    # Отправляем команду STATUS для проверки
                    status_command = ShuttleCommand(
                        command_type=ShuttleCommandEnum.STATUS,
                        shuttle_id=shuttle_id
                    )
                    
                    await shuttle_client.send_command(status_command)
                    self.connected_shuttles.add(shuttle_id)
                else:
                    logger.warning(f"Не удалось восстановить соединение с шаттлом {shuttle_id}")
                    if shuttle_id in self.connected_shuttles:
                        self.connected_shuttles.remove(shuttle_id)
                
                # Закрываем соединение (оно будет поддерживаться через ShuttleListener)
                await shuttle_client.disconnect()
            except Exception as e:
                logger.error(f"Ошибка при проверке шаттла {shuttle_id}: {e}")
                if shuttle_id in self.connected_shuttles:
                    self.connected_shuttles.remove(shuttle_id)


# Глобальный экземпляр монитора шаттлов
shuttle_monitor = None


def get_shuttle_monitor() -> ShuttleMonitor:
    """Возвращает глобальный экземпляр монитора шаттлов"""
    global shuttle_monitor
    if shuttle_monitor is None:
        shuttle_monitor = ShuttleMonitor()
    return shuttle_monitor
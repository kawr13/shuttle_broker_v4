import asyncio
import time
from typing import Dict, List, Optional, Any

from core.config import get_config
from core.logging import get_logger
from shuttle_module.shuttle_state import ShuttleState
from storage_module.redis_storage import get_redis_storage

logger = get_logger()


class RedisStorageManager:
    """Менеджер для работы с хранилищем Redis"""
    
    def __init__(self):
        self.redis_storage = get_redis_storage()
        self.running = False
        self.task = None
        self.save_interval = 10  # Интервал сохранения состояний в секундах
    
    async def start(self):
        """Запускает менеджер хранилища"""
        if self.running:
            return
        
        # Инициализируем соединение с Redis
        await self.redis_storage.init()
        
        self.running = True
        self.task = asyncio.create_task(self._save_states_loop())
        logger.info("Менеджер хранилища Redis запущен")
    
    async def stop(self):
        """Останавливает менеджер хранилища"""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        # Закрываем соединение с Redis
        await self.redis_storage.close()
        logger.info("Менеджер хранилища Redis остановлен")
    
    async def save_shuttle_state(self, state: ShuttleState) -> bool:
        """Сохраняет состояние шаттла в Redis"""
        return await self.redis_storage.save_shuttle_state(state)
    
    async def get_shuttle_state(self, shuttle_id: str) -> Optional[ShuttleState]:
        """Получает состояние шаттла из Redis"""
        return await self.redis_storage.get_shuttle_state(shuttle_id)
    
    async def get_all_shuttle_states(self) -> Dict[str, ShuttleState]:
        """Получает состояния всех шаттлов из Redis"""
        return await self.redis_storage.get_all_shuttle_states()
    
    async def save_command_registry(self, registry: Dict[str, Dict[str, Any]]) -> bool:
        """Сохраняет реестр команд в Redis"""
        return await self.redis_storage.save_command_registry(registry)
    
    async def get_command_registry(self) -> Dict[str, Dict[str, Any]]:
        """Получает реестр команд из Redis"""
        return await self.redis_storage.get_command_registry()
    
    async def _save_states_loop(self):
        """Периодически сохраняет состояния шаттлов в Redis"""
        from ..shuttle_module.shuttle_manager import get_shuttle_manager
        
        while self.running:
            try:
                # Получаем состояния всех шаттлов
                shuttle_manager = get_shuttle_manager()
                states = await shuttle_manager.get_all_shuttle_states()
                
                # Сохраняем состояния в Redis
                for shuttle_id, state in states.items():
                    await self.save_shuttle_state(state)
                
                # Сохраняем реестр команд
                from ..shuttle_module.shuttle_manager import get_shuttle_manager
                shuttle_manager = get_shuttle_manager()
                await self.save_command_registry(shuttle_manager.command_registry)
                
                # Ждем до следующего сохранения
                await asyncio.sleep(self.save_interval)
            except asyncio.CancelledError:
                logger.info("Цикл сохранения состояний отменен")
                break
            except Exception as e:
                logger.error(f"Ошибка при сохранении состояний в Redis: {e}")
                await asyncio.sleep(5)  # Ждем немного перед повторной попыткой


# Глобальный экземпляр менеджера хранилища
redis_storage_manager = None


def get_redis_storage_manager() -> RedisStorageManager:
    """Возвращает глобальный экземпляр менеджера хранилища"""
    global redis_storage_manager
    if redis_storage_manager is None:
        redis_storage_manager = RedisStorageManager()
    return redis_storage_manager
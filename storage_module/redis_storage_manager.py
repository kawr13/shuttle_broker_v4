import asyncio
import time
import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

from core.config import get_config
from core.logging import get_logger
from shuttle_module.shuttle_state import ShuttleState
from storage_module.redis_storage import get_redis_storage
from storage_module.redis_pool import get_redis_pool
from utils.task_wrapper import wrap_async
from monitoring.metrics import record_redis_operation, redis_operation_timer

logger = get_logger()


class RedisStorageManager:
    """Менеджер для работы с хранилищем Redis"""
    
    def __init__(self):
        self.redis_storage = get_redis_storage()
        self.redis_pool = get_redis_pool()
        self.running = False
        self.tasks = []
        self.save_interval = 10  # Интервал сохранения состояний в секундах
        self.backup_interval = 3600  # Интервал резервного копирования в секундах (1 час)
        self.backup_dir = os.path.join(os.getcwd(), "backups")
        
        # Создаем директорию для резервных копий, если она не существует
        os.makedirs(self.backup_dir, exist_ok=True)
    
    async def start(self):
        """Запускает менеджер хранилища"""
        if self.running:
            return
        
        # Инициализируем соединение с Redis
        await self.redis_pool.init()
        await self.redis_storage.init()
        
        self.running = True
        
        # Запускаем задачи
        save_task = asyncio.create_task(wrap_async(self._save_states_loop)())
        backup_task = asyncio.create_task(wrap_async(self._backup_states_loop)())
        
        self.tasks = [save_task, backup_task]
        
        logger.info("Менеджер хранилища Redis запущен")
    
    async def stop(self):
        """Останавливает менеджер хранилища"""
        if not self.running:
            return
        
        self.running = False
        
        # Отменяем все задачи
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self.tasks = []
        
        # Закрываем соединения с Redis
        await self.redis_storage.close()
        await self.redis_pool.close()
        
        logger.info("Менеджер хранилища Redis остановлен")
    
    async def save_shuttle_state(self, state: ShuttleState) -> bool:
        """Сохраняет состояние шаттла в Redis"""
        with redis_operation_timer("save_shuttle_state"):
            try:
                result = await self.redis_storage.save_shuttle_state(state)
                record_redis_operation("save_shuttle_state", "success" if result else "failure")
                return result
            except Exception as e:
                record_redis_operation("save_shuttle_state", "error")
                logger.error(f"Ошибка при сохранении состояния шаттла {state.shuttle_id}: {e}")
                return False
    
    async def get_shuttle_state(self, shuttle_id: str) -> Optional[ShuttleState]:
        """Получает состояние шаттла из Redis"""
        with redis_operation_timer("get_shuttle_state"):
            try:
                result = await self.redis_storage.get_shuttle_state(shuttle_id)
                record_redis_operation("get_shuttle_state", "success")
                return result
            except Exception as e:
                record_redis_operation("get_shuttle_state", "error")
                logger.error(f"Ошибка при получении состояния шаттла {shuttle_id}: {e}")
                return None
    
    async def get_all_shuttle_states(self) -> Dict[str, ShuttleState]:
        """Получает состояния всех шаттлов из Redis"""
        with redis_operation_timer("get_all_shuttle_states"):
            try:
                result = await self.redis_storage.get_all_shuttle_states()
                record_redis_operation("get_all_shuttle_states", "success")
                return result
            except Exception as e:
                record_redis_operation("get_all_shuttle_states", "error")
                logger.error(f"Ошибка при получении состояний всех шаттлов: {e}")
                return {}
    
    async def save_command_registry(self, registry: Dict[str, Dict[str, Any]]) -> bool:
        """Сохраняет реестр команд в Redis"""
        with redis_operation_timer("save_command_registry"):
            try:
                result = await self.redis_storage.save_command_registry(registry)
                record_redis_operation("save_command_registry", "success" if result else "failure")
                return result
            except Exception as e:
                record_redis_operation("save_command_registry", "error")
                logger.error(f"Ошибка при сохранении реестра команд: {e}")
                return False
    
    async def get_command_registry(self) -> Dict[str, Dict[str, Any]]:
        """Получает реестр команд из Redis"""
        with redis_operation_timer("get_command_registry"):
            try:
                result = await self.redis_storage.get_command_registry()
                record_redis_operation("get_command_registry", "success")
                return result
            except Exception as e:
                record_redis_operation("get_command_registry", "error")
                logger.error(f"Ошибка при получении реестра команд: {e}")
                return {}
    
    async def _save_states_loop(self):
        """Периодически сохраняет состояния шаттлов в Redis"""
        from shuttle_module.shuttle_manager import get_shuttle_manager
        from monitoring.metrics import update_queue_size
        
        while self.running:
            try:
                # Проверяем доступность Redis
                if not await self.redis_pool.health_check():
                    logger.warning("Redis недоступен, пропускаем сохранение состояний")
                    await asyncio.sleep(5)
                    continue
                
                # Получаем состояния всех шаттлов
                shuttle_manager = get_shuttle_manager()
                states = await shuttle_manager.get_all_shuttle_states()
                
                # Сохраняем состояния в Redis
                for shuttle_id, state in states.items():
                    await self.save_shuttle_state(state)
                    
                    # Обновляем метрики
                    if hasattr(shuttle_manager, 'command_queues') and shuttle_id in shuttle_manager.command_queues:
                        queue_size = shuttle_manager.command_queues[shuttle_id].qsize()
                        update_queue_size(shuttle_id, queue_size)
                
                # Сохраняем реестр команд
                await self.save_command_registry(shuttle_manager.command_registry)
                
                # Ждем до следующего сохранения
                await asyncio.sleep(self.save_interval)
            except asyncio.CancelledError:
                logger.info("Цикл сохранения состояний отменен")
                break
            except Exception as e:
                logger.error(f"Ошибка при сохранении состояний в Redis: {e}")
                logger.error(f"Стек вызовов: {e.__traceback__}")
                await asyncio.sleep(5)  # Ждем немного перед повторной попыткой
    
    async def _backup_states_loop(self):
        """Периодически создает резервные копии состояний шаттлов"""
        while self.running:
            try:
                # Получаем состояния всех шаттлов
                states = await self.get_all_shuttle_states()
                
                if states:
                    # Создаем имя файла с текущей датой и временем
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_file = os.path.join(self.backup_dir, f"shuttle_states_{timestamp}.json")
                    
                    # Сохраняем состояния в JSON-файл
                    with open(backup_file, "w") as f:
                        json.dump({k: v.to_dict() for k, v in states.items()}, f, indent=2)
                    
                    logger.info(f"Создана резервная копия состояний шаттлов: {backup_file}")
                    
                    # Удаляем старые резервные копии (оставляем последние 10)
                    self._cleanup_old_backups()
                
                # Ждем до следующего резервного копирования
                await asyncio.sleep(self.backup_interval)
            except asyncio.CancelledError:
                logger.info("Цикл резервного копирования состояний отменен")
                break
            except Exception as e:
                logger.error(f"Ошибка при создании резервной копии состояний: {e}")
                await asyncio.sleep(60)  # Ждем минуту перед повторной попыткой
    
    def _cleanup_old_backups(self):
        """Удаляет старые резервные копии, оставляя только последние 10"""
        try:
            # Получаем список файлов резервных копий
            backup_files = [os.path.join(self.backup_dir, f) for f in os.listdir(self.backup_dir)
                           if f.startswith("shuttle_states_") and f.endswith(".json")]
            
            # Сортируем по времени изменения (от старых к новым)
            backup_files.sort(key=os.path.getmtime)
            
            # Удаляем старые файлы, оставляя последние 10
            files_to_delete = backup_files[:-10] if len(backup_files) > 10 else []
            
            for file_path in files_to_delete:
                os.remove(file_path)
                logger.info(f"Удалена старая резервная копия: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка при очистке старых резервных копий: {e}")


# Глобальный экземпляр менеджера хранилища
redis_storage_manager = None


def get_redis_storage_manager() -> RedisStorageManager:
    """Возвращает глобальный экземпляр менеджера хранилища"""
    global redis_storage_manager
    if redis_storage_manager is None:
        redis_storage_manager = RedisStorageManager()
    return redis_storage_manager
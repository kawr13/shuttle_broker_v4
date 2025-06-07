import json
from typing import Dict, Optional, Any, List

import redis.asyncio as redis

from core.config import get_config
from core.logging import get_logger
from shuttle_module.shuttle_state import ShuttleState

logger = get_logger()


class RedisStorage:
    """Хранилище данных в Redis"""
    
    def __init__(self):
        config = get_config()
        self.redis = redis.Redis(
            host=config.redis.host,
            port=config.redis.port,
            db=config.redis.db,
            password=config.redis.password,
            decode_responses=True
        )
        self.shuttle_state_prefix = "shuttle_state:"
        self.config_key = "gateway_config"
    
    async def init(self):
        """Инициализирует соединение с Redis"""
        try:
            await self.redis.ping()
            logger.info("Соединение с Redis установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка при подключении к Redis: {e}")
            return False
    
    async def close(self):
        """Закрывает соединение с Redis"""
        await self.redis.close()
        logger.info("Соединение с Redis закрыто")
    
    async def save_shuttle_state(self, state: ShuttleState) -> bool:
        """Сохраняет состояние шаттла в Redis"""
        key = f"{self.shuttle_state_prefix}{state.shuttle_id}"
        try:
            await self.redis.set(key, json.dumps(state.to_dict()))
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении состояния шаттла {state.shuttle_id} в Redis: {e}")
            return False
    
    async def get_shuttle_state(self, shuttle_id: str) -> Optional[ShuttleState]:
        """Получает состояние шаттла из Redis"""
        key = f"{self.shuttle_state_prefix}{shuttle_id}"
        try:
            data = await self.redis.get(key)
            if data:
                return ShuttleState.from_dict(json.loads(data))
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении состояния шаттла {shuttle_id} из Redis: {e}")
            return None
    
    async def get_all_shuttle_states(self) -> Dict[str, ShuttleState]:
        """Получает состояния всех шаттлов из Redis"""
        result = {}
        try:
            # Получаем все ключи с префиксом shuttle_state:
            keys = []
            async for key in self.redis.scan_iter(match=f"{self.shuttle_state_prefix}*"):
                keys.append(key)
            
            # Получаем данные для всех ключей
            if keys:
                values = await self.redis.mget(keys)
                for key, value in zip(keys, values):
                    if value:
                        shuttle_id = key.replace(self.shuttle_state_prefix, "")
                        result[shuttle_id] = ShuttleState.from_dict(json.loads(value))
            
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении состояний шаттлов из Redis: {e}")
            return {}
    
    async def save_config(self, config_data: Dict[str, Any]) -> bool:
        """Сохраняет конфигурацию в Redis"""
        try:
            await self.redis.set(self.config_key, json.dumps(config_data))
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении конфигурации в Redis: {e}")
            return False
    
    async def get_config(self) -> Optional[Dict[str, Any]]:
        """Получает конфигурацию из Redis"""
        try:
            data = await self.redis.get(self.config_key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении конфигурации из Redis: {e}")
            return None
    
    async def save_command_registry(self, registry: Dict[str, Dict[str, Any]]) -> bool:
        """Сохраняет реестр команд в Redis"""
        try:
            await self.redis.set("command_registry", json.dumps(registry))
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении реестра команд в Redis: {e}")
            return False
    
    async def get_command_registry(self) -> Dict[str, Dict[str, Any]]:
        """Получает реестр команд из Redis"""
        try:
            data = await self.redis.get("command_registry")
            if data:
                return json.loads(data)
            return {}
        except Exception as e:
            logger.error(f"Ошибка при получении реестра команд из Redis: {e}")
            return {}


# Глобальный экземпляр хранилища Redis
redis_storage = None


def get_redis_storage() -> RedisStorage:
    """Возвращает глобальный экземпляр хранилища Redis"""
    global redis_storage
    if redis_storage is None:
        redis_storage = RedisStorage()
    return redis_storage
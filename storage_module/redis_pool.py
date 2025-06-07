import asyncio
from typing import Optional

import redis.asyncio as aioredis

from core.config import get_config
from core.logging import get_logger
from utils.circuit_breaker import redis_circuit, CircuitOpenError

logger = get_logger()


class RedisPool:
    """Пул подключений к Redis"""
    
    def __init__(self):
        self.pool = None
        self.minsize = 5
        self.maxsize = 10
    
    async def init(self):
        """Инициализирует пул подключений к Redis"""
        if self.pool is not None:
            return
        
        config = get_config()
        try:
            # Используем circuit breaker для защиты от отказов Redis
            async def create_pool():
                return aioredis.Redis(
                    host=config.redis.host,
                    port=config.redis.port,
                    db=config.redis.db,
                    password=config.redis.password,
                    decode_responses=True,
                    max_connections=self.maxsize
                )
            
            self.pool = await redis_circuit.execute(create_pool)
            logger.info(f"Создан пул подключений к Redis ({self.minsize}-{self.maxsize})")
            return True
        except CircuitOpenError:
            logger.error("Redis недоступен (circuit breaker открыт)")
            return False
        except Exception as e:
            logger.error(f"Ошибка при создании пула подключений к Redis: {e}")
            return False
    
    async def close(self):
        """Закрывает пул подключений к Redis"""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            logger.info("Пул подключений к Redis закрыт")
    
    async def get_connection(self) -> Optional[aioredis.Redis]:
        """Возвращает подключение из пула"""
        if self.pool is None:
            await self.init()
        return self.pool
    
    async def health_check(self) -> bool:
        """Проверяет доступность Redis"""
        try:
            conn = await self.get_connection()
            if conn is None:
                return False
            
            # Используем circuit breaker для защиты от отказов Redis
            async def ping():
                return await conn.ping()
            
            result = await redis_circuit.execute(ping)
            return result
        except CircuitOpenError:
            logger.error("Redis недоступен (circuit breaker открыт)")
            return False
        except Exception as e:
            logger.error(f"Ошибка при проверке доступности Redis: {e}")
            return False


# Глобальный экземпляр пула подключений к Redis
redis_pool = None


def get_redis_pool() -> RedisPool:
    """Возвращает глобальный экземпляр пула подключений к Redis"""
    global redis_pool
    if redis_pool is None:
        redis_pool = RedisPool()
    return redis_pool
import time
import asyncio
from typing import Callable, Any, Coroutine, TypeVar, Optional

from core.logging import get_logger

logger = get_logger()

T = TypeVar('T')


class CircuitOpenError(Exception):
    """Исключение, возникающее при попытке выполнить операцию с открытым Circuit Breaker"""
    pass


class CircuitBreaker:
    """
    Реализация паттерна Circuit Breaker для защиты от каскадных отказов
    при взаимодействии с внешними сервисами.
    """
    
    def __init__(self, name: str, max_failures: int = 3, reset_timeout: int = 60, half_open_timeout: int = 5):
        """
        Инициализирует Circuit Breaker.
        
        Args:
            name: Имя Circuit Breaker для логирования
            max_failures: Максимальное количество ошибок до открытия цепи
            reset_timeout: Время в секундах до попытки восстановления после открытия цепи
            half_open_timeout: Время в секундах для проверки в полуоткрытом состоянии
        """
        self.name = name
        self.failures = 0
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    async def execute(self, func: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs) -> T:
        """
        Выполняет функцию с защитой Circuit Breaker.
        
        Args:
            func: Асинхронная функция для выполнения
            *args: Аргументы для функции
            **kwargs: Именованные аргументы для функции
        
        Returns:
            Результат выполнения функции
        
        Raises:
            CircuitOpenError: Если цепь открыта и время сброса не истекло
            Exception: Любое исключение, возникшее при выполнении функции
        """
        # Проверяем состояние цепи
        if self.state == "OPEN":
            if time.time() - self.last_failure_time >= self.reset_timeout:
                # Переходим в полуоткрытое состояние для проверки
                logger.info(f"Circuit {self.name} transitioning from OPEN to HALF_OPEN")
                self.state = "HALF_OPEN"
            else:
                # Цепь все еще открыта
                logger.warning(f"Circuit {self.name} is OPEN, rejecting request")
                raise CircuitOpenError(f"Circuit {self.name} is open")
        
        try:
            # Выполняем функцию
            result = await func(*args, **kwargs)
            
            # Если успешно и цепь была в полуоткрытом состоянии, закрываем ее
            if self.state == "HALF_OPEN":
                logger.info(f"Circuit {self.name} transitioning from HALF_OPEN to CLOSED")
                self.reset()
            
            return result
        except Exception as e:
            # Увеличиваем счетчик ошибок
            self.failures += 1
            self.last_failure_time = time.time()
            
            # Если достигли максимального количества ошибок, открываем цепь
            if self.state == "CLOSED" and self.failures >= self.max_failures:
                logger.warning(f"Circuit {self.name} transitioning from CLOSED to OPEN after {self.failures} failures")
                self.state = "OPEN"
            
            # Если цепь была в полуоткрытом состоянии, возвращаем ее в открытое состояние
            elif self.state == "HALF_OPEN":
                logger.warning(f"Circuit {self.name} transitioning from HALF_OPEN back to OPEN after failure")
                self.state = "OPEN"
            
            # Пробрасываем исключение дальше
            raise
    
    def reset(self):
        """Сбрасывает состояние Circuit Breaker в закрытое"""
        self.failures = 0
        self.state = "CLOSED"
        logger.info(f"Circuit {self.name} reset to CLOSED state")


# Глобальные экземпляры Circuit Breaker для различных сервисов
wms_circuit = CircuitBreaker("wms_api")
redis_circuit = CircuitBreaker("redis")


async def with_circuit_breaker(circuit: CircuitBreaker, func: Callable[..., Coroutine[Any, Any, T]], *args, **kwargs) -> T:
    """
    Удобная функция для выполнения операции с защитой Circuit Breaker.
    
    Args:
        circuit: Экземпляр Circuit Breaker
        func: Асинхронная функция для выполнения
        *args: Аргументы для функции
        **kwargs: Именованные аргументы для функции
    
    Returns:
        Результат выполнения функции
    """
    return await circuit.execute(func, *args, **kwargs)
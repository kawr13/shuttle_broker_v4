import asyncio
import functools
import traceback
from typing import Callable, Any, Coroutine

from core.logging import get_logger

logger = get_logger()


async def task_wrapper(task_func: Callable[..., Coroutine], *args, **kwargs) -> Any:
    """
    Обертка для асинхронных задач, которая обрабатывает все исключения
    и предотвращает падение приложения из-за необработанных исключений.
    
    Args:
        task_func: Асинхронная функция для выполнения
        *args: Аргументы для функции
        **kwargs: Именованные аргументы для функции
    
    Returns:
        Результат выполнения функции или None в случае исключения
    """
    try:
        return await task_func(*args, **kwargs)
    except asyncio.CancelledError:
        # Пробрасываем CancelledError, так как это нормальный способ отмены задачи
        raise
    except Exception as e:
        # Логируем исключение с полным стеком вызовов
        logger.error(f"Critical error in {task_func.__name__}: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None


def wrap_async(func: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
    """
    Декоратор для оборачивания асинхронных функций в task_wrapper.
    
    Args:
        func: Асинхронная функция для оборачивания
    
    Returns:
        Обернутая асинхронная функция
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await task_wrapper(func, *args, **kwargs)
    return wrapper
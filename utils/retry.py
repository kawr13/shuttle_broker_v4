import asyncio
import random
from typing import Callable, TypeVar, Any, Optional

from core.logging import get_logger

T = TypeVar('T')
logger = get_logger()


async def retry_async(
    func: Callable[..., Any],
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    jitter: float = 0.1,
    endpoint: Optional[str] = None,
    retry_on: tuple = (Exception,),
    **kwargs
) -> Any:
    """
    Выполняет асинхронную функцию с экспоненциальной задержкой между повторными попытками.
    
    Args:
        func: Асинхронная функция для выполнения
        *args: Аргументы для функции
        max_retries: Максимальное количество повторных попыток
        base_delay: Начальная задержка в секундах
        max_delay: Максимальная задержка в секундах
        jitter: Коэффициент случайности для предотвращения одновременных повторных попыток
        endpoint: Название эндпоинта для логирования
        retry_on: Типы исключений, при которых нужно повторять попытки
        **kwargs: Именованные аргументы для функции
    
    Returns:
        Результат выполнения функции
    
    Raises:
        Exception: Последнее исключение, если все попытки не удались
    """
    retries = 0
    last_exception = None
    
    func_name = endpoint or func.__name__
    
    while retries <= max_retries:
        try:
            return await func(*args, **kwargs)
        except retry_on as e:
            last_exception = e
            retries += 1
            
            if retries > max_retries:
                logger.error(f"Все попытки выполнения {func_name} не удались после {max_retries} попыток")
                raise last_exception
            
            # Экспоненциальная задержка с добавлением случайности
            delay = min(base_delay * (2 ** (retries - 1)), max_delay)
            jitter_value = random.uniform(-jitter * delay, jitter * delay)
            delay = delay + jitter_value
            
            logger.warning(f"Попытка {retries}/{max_retries} для {func_name} не удалась: {e}. "
                          f"Повторная попытка через {delay:.2f} секунд")
            await asyncio.sleep(delay)
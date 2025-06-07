import logging
import sys
from typing import Optional

from .config import get_config


def setup_logging(log_level: Optional[str] = None, log_file: Optional[str] = None):
    """Настраивает логирование"""
    config = get_config()
    
    # Используем параметры из аргументов или из конфигурации
    level = log_level or config.logging.level
    file_path = log_file or config.logging.file_path
    
    # Преобразуем строковый уровень логирования в константу
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    # Настраиваем базовую конфигурацию логирования
    handlers = []
    
    # Добавляем обработчик для вывода в консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    handlers.append(console_handler)
    
    # Добавляем обработчик для записи в файл, если указан путь
    if file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        handlers.append(file_handler)
    
    # Настраиваем корневой логгер
    logging.basicConfig(
        level=numeric_level,
        handlers=handlers,
        force=True
    )
    
    # Создаем и возвращаем логгер для приложения
    logger = logging.getLogger('shuttle_gateway')
    logger.setLevel(numeric_level)
    
    return logger


# Глобальный логгер
logger = None


def get_logger():
    """Возвращает глобальный логгер"""
    global logger
    if logger is None:
        logger = setup_logging()
    return logger
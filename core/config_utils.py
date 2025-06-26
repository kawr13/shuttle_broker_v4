import os
import yaml
import logging
from typing import Dict, Any, Optional

from core.config import get_config, ShuttleConfig

logger = logging.getLogger(__name__)

def save_config_to_file(config_file: str = 'config.yaml') -> bool:
    """
    Сохраняет текущую конфигурацию в файл
    
    Args:
        config_file: Путь к файлу конфигурации
        
    Returns:
        bool: True, если конфигурация успешно сохранена, иначе False
    """
    try:
        config = get_config()
        config_dict = config.to_dict()
        
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Конфигурация успешно сохранена в файл {config_file}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении конфигурации в файл {config_file}: {e}")
        return False

def add_shuttle_to_config(shuttle_id: str, shuttle_ip: str, stock_name: str = 'Главный') -> bool:
    """
    Добавляет шаттл в конфигурацию и сохраняет её в файл
    
    Args:
        shuttle_id: ID шаттла
        shuttle_ip: IP-адрес шаттла
        stock_name: Название склада, на который назначается шаттл
        
    Returns:
        bool: True, если шаттл успешно добавлен и конфигурация сохранена, иначе False
    """
    try:
        config = get_config()
        
        # Проверяем, что шаттл еще не добавлен
        if shuttle_id in config.shuttles:
            logger.warning(f"Шаттл {shuttle_id} уже существует в конфигурации")
            return False
        
        # Создаем конфигурацию шаттла
        shuttle_config = ShuttleConfig(
            host=shuttle_ip,
            command_port=2000,
            response_port=5000,
            shuttle_health_check_interval=10
        )
        
        # Добавляем шаттл в конфигурацию
        config.shuttles[shuttle_id] = shuttle_config
        
        # Добавляем шаттл на склад
        if stock_name in config.stock_to_shuttle:
            if shuttle_id not in config.stock_to_shuttle[stock_name]:
                config.stock_to_shuttle[stock_name].append(shuttle_id)
        else:
            config.stock_to_shuttle[stock_name] = [shuttle_id]
        
        # Сохраняем конфигурацию в файл
        if save_config_to_file():
            logger.info(f"Шаттл {shuttle_id} с IP {shuttle_ip} добавлен в конфигурацию и сохранен в файл")
            return True
        else:
            # Если не удалось сохранить, откатываем изменения
            if shuttle_id in config.shuttles:
                del config.shuttles[shuttle_id]
            if stock_name in config.stock_to_shuttle and shuttle_id in config.stock_to_shuttle[stock_name]:
                config.stock_to_shuttle[stock_name].remove(shuttle_id)
            return False
    except Exception as e:
        logger.error(f"Ошибка при добавлении шаттла {shuttle_id} в конфигурацию: {e}")
        return False
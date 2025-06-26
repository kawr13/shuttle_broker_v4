import os
import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def save_shuttle_to_config(shuttle_id: str, shuttle_ip: str, config_file: str = 'config.yaml', stock_name: str = 'Главный') -> bool:
    """
    Сохраняет шаттл в файл конфигурации
    
    Args:
        shuttle_id: ID шаттла
        shuttle_ip: IP-адрес шаттла
        config_file: Путь к файлу конфигурации
        stock_name: Название склада
        
    Returns:
        bool: True, если шаттл успешно сохранен, иначе False
    """
    try:
        # Проверяем, существует ли файл конфигурации
        if not os.path.exists(config_file):
            logger.error(f"Файл конфигурации {config_file} не найден")
            return False
        
        # Загружаем конфигурацию из файла
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Проверяем, что шаттл еще не добавлен
        if 'shuttles' in config and shuttle_id in config['shuttles']:
            logger.info(f"Шаттл {shuttle_id} уже существует в конфигурации")
            return True
        
        # Добавляем шаттл в конфигурацию
        if 'shuttles' not in config:
            config['shuttles'] = {}
        
        config['shuttles'][shuttle_id] = {
            'host': shuttle_ip,
            'command_port': 2000,
            'response_port': 5000,
            'shuttle_health_check_interval': 10
        }
        
        # Добавляем шаттл на склад
        if 'stock_to_shuttle' not in config:
            config['stock_to_shuttle'] = {}
        
        if stock_name not in config['stock_to_shuttle']:
            config['stock_to_shuttle'][stock_name] = []
        
        if shuttle_id not in config['stock_to_shuttle'][stock_name]:
            config['stock_to_shuttle'][stock_name].append(shuttle_id)
        
        # Сохраняем конфигурацию в файл
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Шаттл {shuttle_id} с IP {shuttle_ip} добавлен в конфигурацию и сохранен в файл {config_file}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении шаттла {shuttle_id} в конфигурацию: {e}")
        return False
import os
import yaml
import logging
from typing import Dict, Any, Optional

from core.config import get_config, ShuttleConfig

logger = logging.getLogger(__name__)

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
        # Загружаем текущую конфигурацию из файла
        config_file = 'config.yaml'
        if not os.path.exists(config_file):
            logger.error(f"Файл конфигурации {config_file} не найден")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # Проверяем, что шаттл еще не добавлен
        if 'shuttles' in config_data and shuttle_id in config_data['shuttles']:
            logger.info(f"Шаттл {shuttle_id} уже существует в конфигурации")
            return True
        
        # Добавляем шаттл в конфигурацию
        if 'shuttles' not in config_data:
            config_data['shuttles'] = {}
        
        config_data['shuttles'][shuttle_id] = {
            'host': shuttle_ip,
            'command_port': 2000,
            'response_port': 5000,
            'shuttle_health_check_interval': 10
        }
        
        # Добавляем шаттл на склад
        if 'stock_to_shuttle' not in config_data:
            config_data['stock_to_shuttle'] = {}
        
        if stock_name not in config_data['stock_to_shuttle']:
            config_data['stock_to_shuttle'][stock_name] = []
        
        if shuttle_id not in config_data['stock_to_shuttle'][stock_name]:
            config_data['stock_to_shuttle'][stock_name].append(shuttle_id)
        
        # Сохраняем конфигурацию в файл
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Шаттл {shuttle_id} с IP {shuttle_ip} добавлен в конфигурацию и сохранен в файл {config_file}")
        
        # Обновляем конфигурацию в памяти
        config = get_config()
        config.shuttles[shuttle_id] = ShuttleConfig(
            host=shuttle_ip,
            command_port=2000,
            response_port=5000,
            shuttle_health_check_interval=10
        )
        
        if stock_name in config.stock_to_shuttle:
            if shuttle_id not in config.stock_to_shuttle[stock_name]:
                config.stock_to_shuttle[stock_name].append(shuttle_id)
        else:
            config.stock_to_shuttle[stock_name] = [shuttle_id]
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при добавлении шаттла {shuttle_id} в конфигурацию: {e}")
        return False
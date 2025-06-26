#!/usr/bin/env python3
"""
Скрипт для тестирования сохранения шаттлов в файл конфигурации
"""
import sys
import logging
import yaml
from core.config import add_shuttle_to_config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_save_shuttle")

def test_save_shuttle(shuttle_id, shuttle_ip, stock_name='Главный'):
    """Тестирует сохранение шаттла в файл конфигурации"""
    logger.info(f"Сохранение шаттла {shuttle_id} с IP {shuttle_ip} в файл конфигурации...")
    
    # Сохраняем шаттл в файл конфигурации
    result = add_shuttle_to_config(shuttle_id, shuttle_ip, stock_name)
    
    if result:
        logger.info(f"Шаттл {shuttle_id} успешно сохранен в файл конфигурации")
    else:
        logger.error(f"Не удалось сохранить шаттл {shuttle_id} в файл конфигурации")
    
    # Проверяем, что шаттл действительно сохранен в файле
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        if 'shuttles' in config_data and shuttle_id in config_data['shuttles']:
            logger.info(f"Шаттл {shuttle_id} найден в файле конфигурации")
            logger.info(f"Конфигурация шаттла: {config_data['shuttles'][shuttle_id]}")
            
            # Проверяем, что шаттл добавлен на склад
            if stock_name in config_data['stock_to_shuttle'] and shuttle_id in config_data['stock_to_shuttle'][stock_name]:
                logger.info(f"Шаттл {shuttle_id} добавлен на склад {stock_name}")
            else:
                logger.warning(f"Шаттл {shuttle_id} не добавлен на склад {stock_name}")
        else:
            logger.error(f"Шаттл {shuttle_id} не найден в файле конфигурации")
    except Exception as e:
        logger.error(f"Ошибка при проверке файла конфигурации: {e}")

def main():
    """Основная функция"""
    if len(sys.argv) < 3:
        print("Использование: test_save_shuttle.py <shuttle_id> <shuttle_ip> [stock_name]")
        return 1
    
    shuttle_id = sys.argv[1]
    shuttle_ip = sys.argv[2]
    stock_name = sys.argv[3] if len(sys.argv) > 3 else 'Главный'
    
    test_save_shuttle(shuttle_id, shuttle_ip, stock_name)
    return 0

if __name__ == "__main__":
    sys.exit(main())
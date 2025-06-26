#!/usr/bin/env python3
"""
Скрипт для проверки шаттлов в конфигурации
"""
import sys
import yaml
import logging
from typing import Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("check_shuttles")

def check_shuttles():
    """Проверяет шаттлы в конфигурации"""
    try:
        # Загружаем конфигурацию из файла
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # Проверяем наличие шаттлов
        if 'shuttles' not in config_data or not config_data['shuttles']:
            logger.error("В конфигурации нет шаттлов")
            return False
        
        # Выводим информацию о шаттлах
        shuttles = config_data['shuttles']
        logger.info(f"Найдено {len(shuttles)} шаттлов в конфигурации:")
        for shuttle_id, shuttle_config in shuttles.items():
            logger.info(f"- {shuttle_id}: {shuttle_config['host']}")
        
        # Проверяем наличие складов
        if 'stock_to_shuttle' not in config_data or not config_data['stock_to_shuttle']:
            logger.error("В конфигурации нет складов")
            return False
        
        # Выводим информацию о складах
        stocks = config_data['stock_to_shuttle']
        logger.info(f"Найдено {len(stocks)} складов в конфигурации:")
        for stock_name, stock_shuttles in stocks.items():
            logger.info(f"- {stock_name}: {', '.join(stock_shuttles)}")
        
        # Проверяем, что все шаттлы назначены на склады
        all_stock_shuttles = []
        for stock_shuttles in stocks.values():
            all_stock_shuttles.extend(stock_shuttles)
        
        unassigned_shuttles = set(shuttles.keys()) - set(all_stock_shuttles)
        if unassigned_shuttles:
            logger.warning(f"Шаттлы без назначения на склад: {', '.join(unassigned_shuttles)}")
        
        # Проверяем, что все шаттлы на складах существуют в конфигурации
        nonexistent_shuttles = set(all_stock_shuttles) - set(shuttles.keys())
        if nonexistent_shuttles:
            logger.error(f"Несуществующие шаттлы на складах: {', '.join(nonexistent_shuttles)}")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке шаттлов: {e}")
        return False

def main():
    """Основная функция"""
    success = check_shuttles()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
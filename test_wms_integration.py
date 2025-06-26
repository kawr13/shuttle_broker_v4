#!/usr/bin/env python3
"""
Тестирование интеграции с WMS API
"""
import asyncio
import argparse
import sys
import logging
from datetime import datetime

from core.config import load_config, get_config
from core.logging import setup_logging, get_logger
from wms_module.wms_client import WmsClient
from wms_module.wms_integration import WmsIntegration

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('wms_test.log')
    ]
)
logger = logging.getLogger("test_wms_integration")

async def test_wms_connection():
    """Тестирует подключение к WMS API"""
    try:
        config = get_config()
        client = WmsClient()
        
        logger.info(f"Тестирование подключения к WMS API: {client.api_url}")
        logger.info(f"Пользователь: {client.username}")
        
        # Получаем команды из отгрузок
        logger.info("Получение команд из отгрузок...")
        shipment_commands = await client.get_shipment_commands()
        logger.info(f"Получено {len(shipment_commands)} команд из отгрузок")
        
        # Получаем команды из перемещений
        logger.info("Получение команд из перемещений...")
        transfer_commands = await client.get_transfer_commands()
        logger.info(f"Получено {len(transfer_commands)} команд из перемещений")
        
        # Выводим детали команд
        if shipment_commands:
            logger.info("Детали команд из отгрузок:")
            for cmd in shipment_commands[:3]:  # Выводим только первые 3 команды
                logger.info(f"  ID: {cmd.get('externalId')}, Тип: {cmd.get('type')}")
                
                # Получаем детали команды
                cmd_details = await client.get_command_details(cmd.get('externalId'), "shipment")
                if cmd_details:
                    logger.info(f"  Детали: {cmd_details}")
        
        if transfer_commands:
            logger.info("Детали команд из перемещений:")
            for cmd in transfer_commands[:3]:  # Выводим только первые 3 команды
                logger.info(f"  ID: {cmd.get('externalId')}, Тип: {cmd.get('type')}")
                
                # Получаем детали команды
                cmd_details = await client.get_command_details(cmd.get('externalId'), "transfer")
                if cmd_details:
                    logger.info(f"  Детали: {cmd_details}")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при тестировании подключения к WMS API: {e}")
        return False

async def test_wms_integration():
    """Тестирует интеграцию с WMS API"""
    try:
        # Создаем и запускаем интеграцию
        integration = WmsIntegration()
        await integration.start()
        
        logger.info("Интеграция с WMS API запущена")
        logger.info("Ожидание 30 секунд для обработки команд...")
        
        # Ждем 30 секунд для обработки команд
        await asyncio.sleep(30)
        
        # Останавливаем интеграцию
        await integration.stop()
        logger.info("Интеграция с WMS API остановлена")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при тестировании интеграции с WMS API: {e}")
        return False

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование интеграции с WMS API")
    parser.add_argument("--config", help="Путь к файлу конфигурации")
    parser.add_argument("--connection", action="store_true", help="Тестировать только подключение к WMS API")
    parser.add_argument("--integration", action="store_true", help="Тестировать полную интеграцию с WMS API")
    
    args = parser.parse_args()
    
    # Загружаем конфигурацию
    config = load_config(args.config)
    
    # Настраиваем логирование
    logger = setup_logging()
    
    if args.connection:
        await test_wms_connection()
    elif args.integration:
        await test_wms_integration()
    else:
        # По умолчанию тестируем и подключение, и интеграцию
        logger.info("Тестирование подключения к WMS API...")
        connection_success = await test_wms_connection()
        
        if connection_success:
            logger.info("Тестирование интеграции с WMS API...")
            await test_wms_integration()
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
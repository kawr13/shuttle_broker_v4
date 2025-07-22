#!/usr/bin/env python3
"""
Скрипт для мониторинга состояния соединений с шаттлами
"""
import asyncio
import logging
import sys
import json
from shuttle.shuttle_client import ShuttleClient
from shuttle.connection_manager import ConnectionManager

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger(__name__)

async def monitor_connections():
    """Мониторинг состояния соединений с шаттлами"""
    client = ShuttleClient("shuttles.json")
    
    logger.info("Запуск мониторинга соединений с шаттлами")
    
    # Получаем список шаттлов
    shuttles = client.shuttles
    
    if not shuttles:
        logger.error("Шаттлы не найдены в конфигурации")
        return
    
    logger.info(f"Найдено {len(shuttles)} шаттлов в конфигурации")
    
    # Отправляем команду STATUS каждому шаттлу для установки соединения
    for ip in shuttles:
        logger.info(f"Установка соединения с шаттлом {ip}")
        success = await client.send_command(ip, "STATUS")
        
        if success:
            logger.info(f"✅ Соединение с {ip} установлено")
        else:
            logger.error(f"❌ Не удалось установить соединение с {ip}")
    
    # Мониторинг состояния соединений
    try:
        while True:
            logger.info("Проверка состояния соединений...")
            
            # Получаем информацию о соединениях
            connections = client.connection_manager.connections
            
            logger.info(f"Активных соединений: {len(connections)}")
            
            # Выводим информацию о каждом соединении
            for ip, conn in connections.items():
                writer = conn.get('writer')
                is_closing = writer.is_closing() if writer else True
                last_activity = conn.get('last_activity', 0)
                
                status = "активно" if not is_closing else "закрывается"
                logger.info(f"Соединение с {ip}: {status}, последняя активность: {last_activity}")
            
            # Ждем 10 секунд перед следующей проверкой
            await asyncio.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("Мониторинг остановлен")
    finally:
        # Закрываем все соединения
        await client.close()
        logger.info("Все соединения закрыты")

if __name__ == "__main__":
    asyncio.run(monitor_connections())
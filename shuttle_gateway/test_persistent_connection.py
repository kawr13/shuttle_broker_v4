#!/usr/bin/env python3
"""
Скрипт для тестирования постоянных соединений с шаттлами
"""
import asyncio
import logging
import sys
from shuttle.shuttle_client import ShuttleClient
from shuttle.connection_manager import ConnectionManager

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger(__name__)

async def test_persistent_connection(ip: str):
    """Тестировать постоянное соединение с шаттлом"""
    client = ShuttleClient("shuttles.json")
    
    logger.info(f"Тестирование постоянного соединения с шаттлом {ip}")
    
    # Отправляем несколько команд через одно соединение
    commands = ["STATUS", "BATTERY", "WDH", "WLH"]
    
    for command in commands:
        logger.info(f"Отправка команды '{command}'")
        success = await client.send_command(ip, command)
        
        if success:
            logger.info(f"✅ Команда {command} отправлена успешно")
        else:
            logger.error(f"❌ Ошибка отправки команды {command}")
        
        # Пауза между командами
        await asyncio.sleep(1)
    
    # Ждем немного, чтобы увидеть, что соединение остается активным
    logger.info("Ожидание 10 секунд...")
    await asyncio.sleep(10)
    
    # Отправляем еще одну команду для проверки, что соединение все еще активно
    logger.info("Отправка еще одной команды STATUS для проверки соединения")
    success = await client.send_command(ip, "STATUS")
    
    if success:
        logger.info("✅ Соединение активно, команда отправлена успешно")
    else:
        logger.error("❌ Соединение потеряно")
    
    # Закрываем соединения
    logger.info("Закрытие соединений")
    await client.close()

async def main():
    if len(sys.argv) < 2:
        print("Использование: python test_persistent_connection.py <IP>")
        print("Пример: python test_persistent_connection.py 10.181.80.135")
        return
    
    ip = sys.argv[1]
    await test_persistent_connection(ip)

if __name__ == "__main__":
    asyncio.run(main())
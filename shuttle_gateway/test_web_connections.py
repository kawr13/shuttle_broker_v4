#!/usr/bin/env python3
"""
Скрипт для тестирования работы веб-сервера с постоянными соединениями
"""
import asyncio
import aiohttp
import logging
import sys
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger(__name__)

async def test_web_connections(ip: str, web_server_url: str = "http://localhost:8000"):
    """Тестировать работу веб-сервера с постоянными соединениями"""
    logger.info(f"Тестирование работы веб-сервера с шаттлом {ip}")
    
    async with aiohttp.ClientSession() as session:
        # Получаем список шаттлов
        logger.info("Получение списка шаттлов...")
        async with session.get(f"{web_server_url}/api/shuttles") as response:
            if response.status == 200:
                data = await response.json()
                shuttles = data.get("shuttles", [])
                logger.info(f"Найдено {len(shuttles)} шаттлов")
                
                # Проверяем, есть ли наш шаттл в списке
                shuttle_found = False
                for shuttle in shuttles:
                    if shuttle["ip"] == ip:
                        shuttle_found = True
                        logger.info(f"Шаттл {ip} найден в списке: {shuttle['name']}")
                        break
                
                if not shuttle_found:
                    logger.error(f"Шаттл {ip} не найден в списке")
                    return
            else:
                logger.error(f"Ошибка получения списка шаттлов: {response.status}")
                return
        
        # Отправляем несколько команд через веб-сервер
        commands = ["STATUS", "BATTERY", "WDH", "WLH"]
        
        for command in commands:
            logger.info(f"Отправка команды '{command}' через веб-сервер")
            
            payload = {
                "ip": ip,
                "command": command
            }
            
            async with session.post(f"{web_server_url}/api/command", json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "success":
                        logger.info(f"✅ Команда {command} отправлена успешно")
                    else:
                        logger.error(f"❌ Ошибка отправки команды: {data.get('message')}")
                else:
                    logger.error(f"❌ Ошибка отправки команды: {response.status}")
            
            # Пауза между командами
            await asyncio.sleep(1)
        
        # Получаем ответы от шаттла
        logger.info("Получение ответов от шаттла...")
        async with session.get(f"{web_server_url}/api/shuttle/{ip}/responses") as response:
            if response.status == 200:
                data = await response.json()
                responses = data.get("responses", [])
                logger.info(f"Получено {len(responses)} ответов от шаттла")
                
                # Выводим последние ответы
                for resp in responses[-5:]:
                    logger.info(f"Ответ: {resp.get('text')}")
            else:
                logger.error(f"Ошибка получения ответов: {response.status}")

async def main():
    if len(sys.argv) < 2:
        print("Использование: python test_web_connections.py <IP> [WEB_SERVER_URL]")
        print("Пример: python test_web_connections.py 10.181.80.135 http://localhost:8000")
        return
    
    ip = sys.argv[1]
    web_server_url = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8000"
    
    await test_web_connections(ip, web_server_url)

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import logging
import os
import sys

from wms.wms_client import WMSClient
from shuttle.shuttle_client import ShuttleClient
from shuttle.shuttle_monitor import ShuttleMonitor
from web_server import WebServer
# from telegram_bot import ShuttleBot

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('shuttle_gateway.log')
    ]
)

logger = logging.getLogger(__name__)

# Конфигурация телеграм-бота
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
WEB_SERVER_URL = os.environ.get('WEB_SERVER_URL', 'http://localhost:8080')

async def main():
    """Главная функция запуска всех модулей"""
    logger.info("Запуск шлюза управления шаттлами")
    
    try:
        # Инициализация компонентов
        wms_client = WMSClient()
        shuttle_client = ShuttleClient("shuttles.json")
        shuttle_client.wms_client = wms_client  # Устанавливаем ссылку для обновления статусов
        shuttle_monitor = ShuttleMonitor(shuttle_client)
        
        # Инициализация веб-сервера
        web_server = WebServer(shuttle_client)
        shuttle_client.web_server = web_server  # Устанавливаем ссылку на веб-сервер
        
        # Запуск веб-сервера
        web_runner = await web_server.start()
        
        # Запуск телеграм-бота если есть токен
        telegram_task = None
        if TELEGRAM_TOKEN:
            logger.info("Запуск телеграм-бота")
            # telegram_bot = ShuttleBot(TELEGRAM_TOKEN, WEB_SERVER_URL)
            # telegram_task = asyncio.create_task(telegram_bot.start())
        else:
            logger.warning("Токен телеграм-бота не указан, бот не будет запущен")
        
        # Запуск всех модулей параллельно
        tasks = [
            wms_client.poll_wms(shuttle_client),
            shuttle_client.listen_shuttles(),
            shuttle_monitor.monitor_shuttle_states()
        ]
        
        if telegram_task:
            tasks.append(telegram_task)
            
        await asyncio.gather(*tasks, return_exceptions=True)
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        # Закрываем все соединения при завершении работы
        if 'shuttle_client' in locals():
            logger.info("Закрытие соединений с шаттлами")
            await shuttle_client.close()
        logger.info("Завершение работы шлюза")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка программы...")
    except Exception as e:
        print(f"Ошибка запуска: {e}")
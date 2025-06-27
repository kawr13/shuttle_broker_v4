import asyncio
import logging

from wms.wms_client import WMSClient
from shuttle.shuttle_client import ShuttleClient
from shuttle.shuttle_monitor import ShuttleMonitor

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

async def main():
    """Главная функция запуска всех модулей"""
    logger.info("Запуск шлюза управления шаттлами")
    
    try:
        # Инициализация компонентов
        wms_client = WMSClient()
        shuttle_client = ShuttleClient("shuttles.json")
        shuttle_client.wms_client = wms_client  # Устанавливаем ссылку для обновления статусов
        shuttle_monitor = ShuttleMonitor(shuttle_client)
        
        # Запуск всех модулей параллельно
        await asyncio.gather(
            wms_client.poll_wms(shuttle_client),
            shuttle_client.listen_shuttles(),
            shuttle_monitor.monitor_shuttle_states(),
            return_exceptions=True
        )
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        logger.info("Завершение работы шлюза")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка программы...")
    except Exception as e:
        print(f"Ошибка запуска: {e}")
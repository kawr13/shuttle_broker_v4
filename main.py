import asyncio
import argparse
import os
import signal
import sys
from typing import Optional

from core.config import load_config, get_config
from core.logging import setup_logging, get_logger
from shuttle_module.shuttle_manager import get_shuttle_manager
from wms_module.wms_integration import get_wms_integration
from storage_module.redis_storage import get_redis_storage
from shuttle_module.shuttle_monitor import get_shuttle_monitor


async def main(config_file: Optional[str] = None):
    """Основная функция запуска шлюза"""
    # Загружаем конфигурацию
    config = load_config(config_file)
    
    # Настраиваем логирование
    logger = setup_logging()
    logger.info("Запуск шлюза WMS-Шаттл (Версия 3.0)...")
    
    # Инициализируем менеджер хранилища Redis
    from storage_module.redis_storage_manager import get_redis_storage_manager
    redis_storage_manager = get_redis_storage_manager()
    await redis_storage_manager.start()
    
    # Инициализируем слушатель шаттлов
    from shuttle_module.shuttle_listener import get_shuttle_listener
    shuttle_listener = get_shuttle_listener()
    await shuttle_listener.start()
    
    # Инициализируем менеджер шаттлов
    shuttle_manager = get_shuttle_manager()
    await shuttle_manager.start()
    
    # Загружаем состояния шаттлов из Redis
    try:
        states = await redis_storage_manager.get_all_shuttle_states()
        if states:
            logger.info(f"Загружено {len(states)} состояний шаттлов из Redis")
            for shuttle_id, state in states.items():
                logger.info(f"Загружено состояние шаттла {shuttle_id}: статус={state.status}, ячейка={state.current_cell}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке состояний шаттлов из Redis: {e}")
    
    # Инициализируем монитор шаттлов
    shuttle_monitor = get_shuttle_monitor()
    await shuttle_monitor.start()
    logger.info(f"Монитор шаттлов запущен (интервал проверки: {config.shuttle_health_check_interval} сек)")
    # Инициализируем интеграцию с WMS, если она включена
    if config.wms:
        wms_integration = get_wms_integration()
        await wms_integration.start()
        logger.info(f"Интеграция с WMS API запущена (интервал опроса: {config.wms.poll_interval} сек)")
    
    # Настраиваем обработку сигналов завершения
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
    
    # Ожидаем завершения
    try:
        while True:
            await asyncio.sleep(3600)  # Просто держим процесс активным
    except asyncio.CancelledError:
        pass


async def shutdown():
    """Корректно завершает работу шлюза"""
    logger = get_logger()
    logger.info("Остановка шлюза WMS-Шаттл (Версия 3.0)...")
    
    # Останавливаем интеграцию с WMS, если она запущена
    config = get_config()
    if config.wms:
        wms_integration = get_wms_integration()
        await wms_integration.stop()
        logger.info("Интеграция с WMS API остановлена")
    
    
    from shuttle_module.shuttle_monitor import get_shuttle_monitor
    shuttle_monitor = get_shuttle_monitor()
    await shuttle_monitor.stop()
    logger.info("Монитор шаттлов остановлен")
    
    # Останавливаем менеджер шаттлов
    shuttle_manager = get_shuttle_manager()
    await shuttle_manager.stop()
    
    # Останавливаем слушатель шаттлов
    from shuttle_module.shuttle_listener import get_shuttle_listener
    shuttle_listener = get_shuttle_listener()
    await shuttle_listener.stop()
    
    # Останавливаем менеджер хранилища Redis
    from storage_module.redis_storage_manager import get_redis_storage_manager
    redis_storage_manager = get_redis_storage_manager()
    await redis_storage_manager.stop()
    logger.info("Менеджер хранилища Redis остановлен")
    
    # Останавливаем цикл событий
    asyncio.get_event_loop().stop()


if __name__ == "__main__":
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description="Шлюз WMS-Шаттл")
    parser.add_argument("--config", help="Путь к файлу конфигурации")
    args = parser.parse_args()
    
    # Запускаем основную функцию
    asyncio.run(main(args.config))
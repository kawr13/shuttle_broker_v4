#!/usr/bin/env python3
"""
Тестовый скрипт для проверки функциональности автоматического обнаружения шаттлов
"""
import asyncio
import time
from core.config import load_config
from core.logging import setup_logging, get_logger
from shuttle_module.shuttle_discovery import get_shuttle_discovery


async def test_discovery():
    """Тестирует модуль автоматического обнаружения шаттлов"""
    # Настраиваем логирование
    logger = setup_logging()
    logger.info("Запуск теста модуля автоматического обнаружения шаттлов")
    
    # Загружаем конфигурацию
    try:
        config = load_config("config.yaml")
        logger.info("Конфигурация загружена успешно")
    except Exception as e:
        logger.error(f"Ошибка при загрузке конфигурации: {e}")
        return
    
    # Инициализируем модуль обнаружения
    shuttle_discovery = get_shuttle_discovery()
    
    # Запускаем модуль
    await shuttle_discovery.start()
    logger.info("Модуль обнаружения запущен")
    
    # Ждем некоторое время для сканирования
    logger.info("Ожидание результатов сканирования (60 секунд)...")
    await asyncio.sleep(60)
    
    # Получаем результаты
    discovered = shuttle_discovery.get_discovered_shuttles()
    
    if discovered:
        logger.info(f"Обнаружено {len(discovered)} шаттлов:")
        for shuttle_name, shuttle_info in discovered.items():
            logger.info(f"  {shuttle_name}:")
            logger.info(f"    IP: {shuttle_info.ip}")
            logger.info(f"    Статус: {shuttle_info.status or 'Неизвестен'}")
            logger.info(f"    Батарея: {shuttle_info.battery_level or 'Неизвестно'}")
            logger.info(f"    Местоположение: {shuttle_info.location or 'Неизвестно'}")
            
            discovered_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(shuttle_info.discovered_at))
            logger.info(f"    Обнаружен: {discovered_time}")
            logger.info("")
    else:
        logger.info("Новые шаттлы не обнаружены")
    
    # Останавливаем модуль
    await shuttle_discovery.stop()
    logger.info("Модуль обнаружения остановлен")


if __name__ == "__main__":
    asyncio.run(test_discovery())
#!/usr/bin/env python3
"""
Скрипт для применения исправления терминатора в ShuttleListener
"""
import os
import shutil
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("apply_listener_terminator_fix")

def apply_listener_terminator_fix():
    """Применяет исправление терминатора в ShuttleListener"""
    try:
        # Путь к файлам
        listener_path = os.path.join("shuttle_module", "shuttle_listener.py")
        listener_fixed_path = os.path.join("shuttle_module", "shuttle_listener_terminator_fix.py")
        listener_backup_path = os.path.join("shuttle_module", "shuttle_listener.py.bak")
        
        # Проверяем, существуют ли файлы
        if not os.path.exists(listener_path):
            logger.error(f"Файл {listener_path} не найден")
            return False
        
        if not os.path.exists(listener_fixed_path):
            logger.error(f"Файл {listener_fixed_path} не найден")
            return False
        
        # Создаем резервную копию
        logger.info(f"Создание резервной копии {listener_path} -> {listener_backup_path}")
        shutil.copy2(listener_path, listener_backup_path)
        
        # Заменяем файл
        logger.info(f"Замена {listener_path} на {listener_fixed_path}")
        shutil.copy2(listener_fixed_path, listener_path)
        
        logger.info("Исправление терминатора в ShuttleListener успешно применено")
        return True
    except Exception as e:
        logger.error(f"Ошибка при применении исправления: {e}")
        return False

if __name__ == "__main__":
    apply_listener_terminator_fix()
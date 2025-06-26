#!/usr/bin/env python3
"""
Скрипт для применения обновленного слушателя шаттлов
"""
import os
import shutil
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("apply_listener_update")

def apply_listener_update():
    """Применяет обновленный слушатель шаттлов"""
    try:
        # Путь к файлам
        listener_path = os.path.join("shuttle_module", "shuttle_listener.py")
        listener_updated_path = os.path.join("shuttle_module", "shuttle_listener_updated.py")
        listener_backup_path = os.path.join("shuttle_module", "shuttle_listener.py.bak")
        
        # Проверяем, существуют ли файлы
        if not os.path.exists(listener_path):
            logger.error(f"Файл {listener_path} не найден")
            return False
        
        if not os.path.exists(listener_updated_path):
            logger.error(f"Файл {listener_updated_path} не найден")
            return False
        
        # Создаем резервную копию
        logger.info(f"Создание резервной копии {listener_path} -> {listener_backup_path}")
        shutil.copy2(listener_path, listener_backup_path)
        
        # Заменяем файл
        logger.info(f"Замена {listener_path} на {listener_updated_path}")
        shutil.copy2(listener_updated_path, listener_path)
        
        logger.info("Обновленный слушатель шаттлов успешно применен")
        return True
    except Exception as e:
        logger.error(f"Ошибка при применении обновленного слушателя: {e}")
        return False

if __name__ == "__main__":
    apply_listener_update()
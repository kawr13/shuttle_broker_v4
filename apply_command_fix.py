#!/usr/bin/env python3
"""
Скрипт для применения исправления формата команд
"""
import os
import shutil
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("apply_command_fix")

def apply_command_fix():
    """Применяет исправление формата команд"""
    try:
        # Путь к файлам
        commands_path = os.path.join("shuttle_module", "commands.py")
        commands_fixed_path = os.path.join("shuttle_module", "commands_fixed.py")
        commands_backup_path = os.path.join("shuttle_module", "commands.py.bak")
        
        # Проверяем, существуют ли файлы
        if not os.path.exists(commands_path):
            logger.error(f"Файл {commands_path} не найден")
            return False
        
        if not os.path.exists(commands_fixed_path):
            logger.error(f"Файл {commands_fixed_path} не найден")
            return False
        
        # Создаем резервную копию
        logger.info(f"Создание резервной копии {commands_path} -> {commands_backup_path}")
        shutil.copy2(commands_path, commands_backup_path)
        
        # Заменяем файл
        logger.info(f"Замена {commands_path} на {commands_fixed_path}")
        shutil.copy2(commands_fixed_path, commands_path)
        
        logger.info("Исправление формата команд успешно применено")
        return True
    except Exception as e:
        logger.error(f"Ошибка при применении исправления: {e}")
        return False

if __name__ == "__main__":
    apply_command_fix()
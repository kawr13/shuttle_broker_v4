#!/usr/bin/env python3
"""
Скрипт для применения всех исправлений терминаторов команд
"""
import os
import sys
import logging
from apply_command_fix import apply_command_fix
from apply_listener_terminator_fix import apply_listener_terminator_fix

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("apply_terminator_fixes")

def apply_all_fixes():
    """Применяет все исправления терминаторов команд"""
    try:
        # Применяем исправление формата команд
        logger.info("Применение исправления формата команд...")
        if not apply_command_fix():
            logger.error("Не удалось применить исправление формата команд")
            return False
        
        # Применяем исправление терминатора в ShuttleListener
        logger.info("Применение исправления терминатора в ShuttleListener...")
        if not apply_listener_terminator_fix():
            logger.error("Не удалось применить исправление терминатора в ShuttleListener")
            return False
        
        logger.info("Все исправления успешно применены")
        return True
    except Exception as e:
        logger.error(f"Ошибка при применении исправлений: {e}")
        return False

if __name__ == "__main__":
    success = apply_all_fixes()
    sys.exit(0 if success else 1)
#!/usr/bin/env python3
"""
Скрипт для применения патча к ShuttleManager
"""
import inspect
import sys
import os

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shuttle_module.shuttle_manager import ShuttleManager, get_shuttle_manager
from shuttle_module.shuttle_manager_patch import get_free_shuttle

def apply_patch():
    """Применяет патч к ShuttleManager"""
    print("Применение патча к ShuttleManager...")
    
    # Заменяем метод get_free_shuttle
    setattr(ShuttleManager, 'get_free_shuttle', get_free_shuttle)
    
    print("Патч успешно применен!")
    print("Теперь ShuttleManager будет использовать автоматически обнаруженные шаттлы")

if __name__ == "__main__":
    apply_patch()
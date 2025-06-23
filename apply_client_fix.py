#!/usr/bin/env python3
"""
Скрипт для применения исправления к ShuttleClient
"""
import os
import shutil
import sys

def apply_fix():
    """Применяет исправление к ShuttleClient"""
    # Пути к файлам
    original_file = "/mnt/storage/two_projects/strong_brokker_sh/shuttle_gateway_v2/shuttle_module/shuttle_client.py"
    fixed_file = "/mnt/storage/two_projects/strong_brokker_sh/shuttle_gateway_v2/shuttle_module/shuttle_client_fixed.py"
    backup_file = "/mnt/storage/two_projects/strong_brokker_sh/shuttle_gateway_v2/shuttle_module/shuttle_client.py.bak"
    
    # Проверяем, существуют ли файлы
    if not os.path.exists(original_file):
        print(f"Ошибка: Файл {original_file} не найден")
        return False
    
    if not os.path.exists(fixed_file):
        print(f"Ошибка: Файл {fixed_file} не найден")
        return False
    
    # Создаем резервную копию
    try:
        shutil.copy2(original_file, backup_file)
        print(f"Создана резервная копия: {backup_file}")
    except Exception as e:
        print(f"Ошибка при создании резервной копии: {e}")
        return False
    
    # Заменяем оригинальный файл исправленным
    try:
        shutil.copy2(fixed_file, original_file)
        print(f"Файл {original_file} успешно заменен исправленной версией")
    except Exception as e:
        print(f"Ошибка при замене файла: {e}")
        # Восстанавливаем из резервной копии
        try:
            shutil.copy2(backup_file, original_file)
            print(f"Восстановлен оригинальный файл из резервной копии")
        except Exception as e2:
            print(f"Ошибка при восстановлении из резервной копии: {e2}")
        return False
    
    print("Исправление успешно применено!")
    print("Перезапустите шлюз для применения изменений")
    return True

if __name__ == "__main__":
    success = apply_fix()
    sys.exit(0 if success else 1)
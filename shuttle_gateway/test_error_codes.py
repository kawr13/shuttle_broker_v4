#!/usr/bin/env python3
"""
Скрипт для тестирования расшифровки кодов ошибок шаттлов
"""
import sys
from shuttle.error_codes import get_error_description

def test_error_codes():
    """Тестировать расшифровку кодов ошибок"""
    test_codes = [
        "F_CODE=1",
        "F_CODE=2",
        "F_CODE=3",
        "F_CODE=4",
        "F_CODE=5",
        "F_CODE=6",
        "F_CODE=8",
        "F_CODE=9",
        "F_CODE=1Аккумулятор разряжен",
        "F_CODE=10",  # Неизвестный код
        "1",          # Просто код
        "UNKNOWN"     # Неизвестная строка
    ]
    
    print("🧪 Тестирование расшифровки кодов ошибок шаттлов:")
    print("-" * 50)
    
    for code in test_codes:
        description = get_error_description(code)
        print(f"Код: {code} -> {description}")

if __name__ == "__main__":
    test_error_codes()
#!/usr/bin/env python3
"""
Скрипт для проверки конфигурации
"""
import sys
import json
from core.config import load_config, get_config

def main():
    """Основная функция"""
    print("Проверка конфигурации...")
    
    # Загружаем конфигурацию
    config = load_config()
    
    # Выводим информацию о конфигурации
    print("\nОсновные настройки:")
    print(f"- Порт слушателя шаттлов: {config.shuttle_listener_port}")
    print(f"- Интервал проверки состояния шаттлов: {config.shuttle_health_check_interval} сек")
    print(f"- Максимальный размер очереди команд: {config.command_queue_max_size}")
    print(f"- Количество обработчиков команд: {config.command_processor_workers}")
    
    # Выводим информацию о шаттлах
    print("\nНастройки шаттлов:")
    for shuttle_id, shuttle_config in config.shuttles.items():
        print(f"- {shuttle_id}: {shuttle_config.host}:{shuttle_config.command_port}")
    
    # Выводим информацию о складах
    print("\nНастройки складов:")
    for stock_name, shuttles in config.stock_to_shuttle.items():
        print(f"- {stock_name}: {', '.join(shuttles)}")
    
    # Выводим информацию о WMS
    print("\nНастройки WMS:")
    if config.wms:
        print(f"- API URL: {config.wms.api_url}")
        print(f"- Пользователь: {config.wms.username}")
        print(f"- Интервал опроса: {config.wms.poll_interval} сек")
        print(f"- Webhook URL: {config.wms.webhook_url}")
    else:
        print("- Конфигурация WMS отсутствует")
    
    # Выводим информацию о Redis
    print("\nНастройки Redis:")
    print(f"- Хост: {config.redis.host}")
    print(f"- Порт: {config.redis.port}")
    print(f"- База данных: {config.redis.db}")
    
    # Выводим информацию о логировании
    print("\nНастройки логирования:")
    print(f"- Уровень: {config.logging.level}")
    print(f"- Файл: {config.logging.file_path}")
    
    # Проверяем наличие обязательных настроек
    print("\nПроверка обязательных настроек:")
    
    # Проверяем наличие шаттлов
    if not config.shuttles:
        print("- ОШИБКА: Не настроены шаттлы")
    else:
        print("- OK: Шаттлы настроены")
    
    # Проверяем наличие складов
    if not config.stock_to_shuttle:
        print("- ОШИБКА: Не настроены склады")
    else:
        print("- OK: Склады настроены")
    
    # Проверяем наличие WMS
    if not config.wms:
        print("- ПРЕДУПРЕЖДЕНИЕ: Не настроена интеграция с WMS")
    else:
        print("- OK: Интеграция с WMS настроена")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
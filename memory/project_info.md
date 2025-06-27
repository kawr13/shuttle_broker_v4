# Shuttle Gateway Project Info

## Статус проекта
✅ **ЗАВЕРШЕН** - Базовая реализация готова

## Структура проекта
```
shuttle_gateway/
├── config.py              # Конфигурация (порты, URL, приоритеты)
├── shuttles.json          # Конфигурация шаттлов (IP, ячейки, склады)
├── wms/
│   └── wms_client.py      # WMS клиент (опрос документов/задач)
├── shuttle/
│   ├── shuttle_client.py  # Основной клиент шаттлов
│   └── shuttle_monitor.py # Мониторинг состояний
├── main.py                # Точка входа
├── test_shuttle.py        # Тестовый скрипт
└── requirements.txt       # Зависимости
```

## Ключевые особенности
- Асинхронная архитектура (asyncio)
- Автоматическое добавление новых шаттлов
- Приоритетная очередь команд
- Мониторинг состояний (батарея, ошибки, сервоприводы)
- Интеграция с WMS для обновления статусов задач
- Логирование всех операций

## Порты
- Команды шаттлам: 2000
- Ответы от шаттлов: 8181
- WMS API: 8080

## Поддерживаемые команды
HOME, PALLET_IN, PALLET_OUT, FIFO, FILO, STACK_IN, STACK_OUT, 
COUNT, STATUS, BATTERY, WDH, WLH, MRCD, MOVE_TO_CELL, CHANGE_WAREHOUSE

## Запуск
```bash
cd shuttle_gateway
python main.py
```
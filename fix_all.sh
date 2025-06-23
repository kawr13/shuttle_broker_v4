#!/bin/bash
# Скрипт для применения всех исправлений

echo "Применение исправлений для шлюза шаттлов..."

# Применяем исправление к ShuttleListener
echo "1. Применение исправления к ShuttleListener..."
./apply_listener_fix.py
if [ $? -ne 0 ]; then
    echo "Ошибка при применении исправления к ShuttleListener"
    exit 1
fi

# Применяем исправление к ShuttleClient
echo "2. Применение исправления к ShuttleClient..."
./apply_client_fix.py
if [ $? -ne 0 ]; then
    echo "Ошибка при применении исправления к ShuttleClient"
    exit 1
fi

echo "Все исправления успешно применены!"
echo "Перезапустите шлюз для применения изменений:"
echo "  sudo systemctl restart shuttle-gateway"
echo "или"
echo "  python main.py --config config.yaml"
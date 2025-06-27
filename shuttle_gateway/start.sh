#!/bin/bash
# Скрипт запуска шлюза шаттлов

echo "🚀 Запуск шлюза управления шаттлами..."

# Проверка зависимостей
if ! python3 -c "import aiohttp" 2>/dev/null; then
    echo "📦 Установка зависимостей..."
    pip3 install -r requirements.txt
fi

# Запуск основного приложения
echo "🔄 Запуск основного процесса..."
python3 main.py
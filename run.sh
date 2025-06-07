#!/bin/bash

# Запуск шлюза WMS-Шаттл (Версия 3.0)

# Переходим в директорию проекта
cd "$(dirname "$0")"

# Проверяем наличие виртуального окружения
if [ ! -d ".venv" ]; then
    echo "Создаем виртуальное окружение..."
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
else
    source .venv/bin/activate
fi

# Запускаем шлюз
echo "Запуск шлюза WMS-Шаттл (Версия 3.0)..."
python main.py --config config.yaml
#!/bin/bash
# Скрипт для запуска шлюза и эмуляции нескольких шаттлов

# Переходим в директорию проекта
cd "$(dirname "$0")"

# Проверяем наличие виртуального окружения
if [ -d "venv" ]; then
    echo "Активация виртуального окружения..."
    source venv/bin/activate
fi

# Запускаем шлюз в фоновом режиме
echo "Запуск шлюза..."
python main.py &
GATEWAY_PID=$!

# Ждем, пока шлюз запустится
echo "Ожидание запуска шлюза..."
sleep 5

# Запускаем эмуляторы шаттлов
echo "Запуск эмуляторов шаттлов..."
python emulate_shuttle.py --id test_shuttle_10 --ip 10.181.80.210 --duration 30 &
EMULATOR_PID_1=$!

python emulate_shuttle.py --id test_shuttle_11 --ip 10.181.80.211 --duration 30 &
EMULATOR_PID_2=$!

python emulate_shuttle.py --id test_shuttle_12 --ip 10.181.80.212 --duration 30 &
EMULATOR_PID_3=$!

# Ждем завершения эмуляторов
echo "Ожидание завершения эмуляторов..."
wait $EMULATOR_PID_1
wait $EMULATOR_PID_2
wait $EMULATOR_PID_3

# Останавливаем шлюз
echo "Остановка шлюза..."
kill $GATEWAY_PID
wait $GATEWAY_PID

# Проверяем содержимое файла конфигурации
echo "Проверка файла конфигурации..."
grep -A 1 "test_shuttle_1" config.yaml

# Деактивируем виртуальное окружение
if [ -d "venv" ]; then
    deactivate
fi

echo "Готово!"
exit 0
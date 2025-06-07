#!/bin/bash

# Скрипт для тестирования команд шаттлам

# Переходим в директорию проекта
cd "$(dirname "$0")"

# Активируем виртуальное окружение
source .venv/bin/activate

# Проверяем аргументы
if [ $# -lt 2 ]; then
    echo "Использование: $0 <shuttle_id> <command> [params]"
    echo "Пример: $0 virtual_shuttle_1 STATUS"
    echo "Пример: $0 virtual_shuttle_1 FIFO 3"
    echo ""
    echo "Доступные команды:"
    echo "  PALLET_IN - загрузка паллеты"
    echo "  PALLET_OUT - выгрузка паллеты"
    echo "  FIFO - операция FIFO (First In, First Out)"
    echo "  FILO - операция FILO (First In, Last Out)"
    echo "  STACK_IN - загрузка в стек"
    echo "  STACK_OUT - выгрузка из стека"
    echo "  HOME - возврат в домашнюю позицию"
    echo "  COUNT - подсчет паллет"
    echo "  STATUS - запрос статуса"
    echo "  BATTERY - запрос уровня батареи"
    echo "  WDH - запрос часов работы"
    echo "  WLH - запрос часов под нагрузкой"
    echo "  MRCD - подтверждение получения сообщения"
    exit 1
fi

SHUTTLE_ID=$1
COMMAND=$2
PARAMS=$3

# Отправляем команду
if [ -z "$PARAMS" ]; then
    python cli.py send $SHUTTLE_ID $COMMAND
else
    python cli.py send $SHUTTLE_ID $COMMAND --params $PARAMS
fi
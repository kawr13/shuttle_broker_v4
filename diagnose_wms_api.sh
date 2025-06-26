#!/bin/bash
# Скрипт для диагностики WMS API

echo "🔍 Запуск диагностики WMS API..."
echo

# Запуск диагностики через Python скрипт
python test_api_wms.py --diagnose

echo
echo "📡 Проверка доступности WMS сервера через curl..."

# Проверка с /exec
echo "Проверка http://10.181.80.28:8080/exec:"
curl -s -I -u "1000:1000" "http://10.181.80.28:8080/exec" | head -5

echo
echo "Проверка http://10.181.80.28:8080/:"
curl -s -I -u "1000:1000" "http://10.181.80.28:8080/" | head -5

echo
echo "Проверка запроса с действием:"
curl -s -I -u "1000:1000" "http://10.181.80.28:8080/exec?action=IncomeApi.getObjectsShipment" | head -5

echo
echo "🔍 Диагностика завершена"
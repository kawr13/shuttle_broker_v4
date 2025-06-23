#!/bin/bash
# Скрипт для автоматического обновления конфигурации шаттлов

# Путь к файлу конфигурации
CONFIG_FILE="config.yaml"

# Создаем резервную копию конфигурации
cp $CONFIG_FILE ${CONFIG_FILE}.bak

echo "Обновление конфигурации шаттлов..."
python patch_config.py --auto --config $CONFIG_FILE

if [ $? -eq 0 ]; then
    echo "Конфигурация успешно обновлена!"
    echo "Перезапустите сервис для применения изменений"
else
    echo "Ошибка при обновлении конфигурации"
    echo "Восстанавливаем резервную копию..."
    cp ${CONFIG_FILE}.bak $CONFIG_FILE
fi
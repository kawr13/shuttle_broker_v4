# Скрипты для прямого подключения к шаттлам

## Описание
Созданы скрипты для прямого подключения к шаттлам по ID и отправки команд:

1. **`shuttle.py`** - 🌟 **УНИВЕРСАЛЬНЫЙ ИНСТРУМЕНТ** (рекомендуется)
2. **`shuttle_direct_client.py`** - полнофункциональный клиент с расширенными возможностями
3. **`send_command.py`** - упрощенный скрипт для быстрой отправки команд
4. **`list_shuttles.py`** - показать все доступные шаттлы

## Использование

### 🌟 Универсальный инструмент (shuttle.py) - РЕКОМЕНДУЕТСЯ

```bash
# Показать все доступные шаттлы
shuttle.py list

# Отправить команду STATUS (упрощенно)
shuttle.py send shuttle_140 STATUS

# Отправить команду с расширенными опциями
shuttle.py cmd shuttle_140 STATUS --timeout 15
shuttle.py cmd shuttle_140 HOME --no-listen
shuttle.py cmd shuttle_140 BATTERY -t 5
```

### Полный клиент (shuttle_direct_client.py)

```bash
# Отправить команду STATUS шаттлу shuttle_140 и ждать ответ 10 секунд
python shuttle_direct_client.py shuttle_140 STATUS

# Отправить команду с таймаутом 5 секунд
python shuttle_direct_client.py shuttle_140 STATUS --timeout 5

# Только отправить команду, не слушать ответ
python shuttle_direct_client.py shuttle_140 STATUS --no-listen

# Отправить команду HOME
python shuttle_direct_client.py shuttle_140 HOME

# Отправить команду BATTERY
python shuttle_direct_client.py shuttle_140 BATTERY
```

### Упрощенный клиент (send_command.py)

```bash
# Отправить команду STATUS
python send_command.py shuttle_140 STATUS

# Отправить команду HOME
python send_command.py shuttle_140 HOME

# Отправить команду BATTERY
python send_command.py shuttle_140 BATTERY
```

## Как это работает

1. **Загрузка конфигурации**: Скрипт читает `config.yaml` и находит IP адрес шаттла по его ID
2. **Отправка команды**: Подключается к шаттлу на порт 2000 (command_port) и отправляет команду с терминатором `\r\n`
3. **Прослушивание ответа**: Запускает сервер на порту 8181 и ждет ответ от конкретного шаттла
4. **Фильтрация**: Принимает ответы только от IP адреса целевого шаттла, игнорируя остальные

## Примеры команд для шаттлов

- `STATUS` - запросить статус шаттла
- `HOME` - отправить шаттл домой
- `BATTERY` - запросить уровень батареи
- `LOC` - запросить местоположение
- `MRCD` - подтверждение получения сообщения

## Доступные шаттлы

Для просмотра всех доступных шаттлов:

```bash
# Универсальный способ (рекомендуется)
shuttle.py list

# Или напрямую
python list_shuttles.py

# Или через Python
python -c "import yaml; data=yaml.safe_load(open('config.yaml')); [print(f'{k}: {v[\"host\"]}') for k,v in data['shuttles'].items()]"
```

## Примеры использования

### 🌟 С универсальным инструментом (рекомендуется)
```bash
# Показать все шаттлы
shuttle.py list

# Проверить статус шаттла shuttle_141
shuttle.py send shuttle_141 STATUS

# Отправить шаттл shuttle_146 домой
shuttle.py send shuttle_146 HOME

# Проверить батарею с расширенным таймаутом
shuttle.py cmd shuttle_150 BATTERY --timeout 15

# Только отправить команду без ожидания ответа
shuttle.py cmd shuttle_155 STATUS --no-listen
```

### С отдельными скриптами
```bash
# Проверить статус шаттла shuttle_141
python send_command.py shuttle_141 STATUS

# Отправить шаттл shuttle_146 домой
python send_command.py shuttle_146 HOME

# Проверить батарею с расширенным таймаутом
python shuttle_direct_client.py shuttle_150 BATTERY --timeout 15

# Только отправить команду без ожидания ответа
python shuttle_direct_client.py shuttle_155 STATUS --no-listen
```

## Особенности

- ✅ Автоматическое добавление терминатора `\r\n` к командам
- ✅ Фильтрация ответов по IP адресу шаттла
- ✅ Настраиваемый таймаут ожидания ответа
- ✅ Подробное логирование процесса
- ✅ Обработка ошибок подключения и таймаутов
- ✅ Поддержка всех шаттлов из конфигурации

## Устранение неполадок

1. **Шаттл не найден**: Убедитесь, что шаттл существует в `config.yaml`
2. **Таймаут подключения**: Проверьте доступность IP адреса шаттла
3. **Нет ответа**: Увеличьте таймаут или проверьте, что шаттл активен
4. **Порт занят**: Убедитесь, что порт 8181 свободен
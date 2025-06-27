# Протокол шаттлов - Форматы сообщений

## Поддерживаемые форматы ответов шаттлов:

### 1. Формат с окончанием _DONE
- `PALLET_IN_DONE` → command: PALLET_IN, status: DONE
- `FILO-040_DONE` → command: FILO, status: DONE, task_id: 040

### 2. Формат KEY=VALUE  
- `STATUS=NOT_READY` → command: STATUS, status: NOT_READY
- `LOC=NONE_RFID` → command: LOC, status: NONE_RFID

### 3. Стандартный формат (пробелы)
- `COMMAND STATUS TASK_ID`
- `COMMAND STATUS`

## Автоматические ответы
- На каждое сообщение от шаттла автоматически отправляется команда `MRCD`
- Задержка перед отправкой MRCD: 0.1 секунды

## Обработка задач
- При получении статуса DONE с task_id → обновление статуса в WMS
- Логирование всех этапов парсинга для отладки
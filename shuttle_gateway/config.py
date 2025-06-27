# Конфигурация шлюза для управления шаттлами

# WMS настройки
WMS_API_URL = "http://10.181.80.28:8080/exec"
USERNAME = "1000"
PASSWORD = "1000"

# Порты для шаттлов
SHUTTLE_COMMAND_PORT = 2000
SHUTTLE_RESPONSE_PORT = 8181

# Интервалы опроса (секунды)
POLL_INTERVAL = 300  # Опрос WMS
STATE_POLL_INTERVAL = 600  # Опрос состояния шаттлов
SHUTTLE_READ_TIMEOUT = 5  # Тайм-аут чтения сообщений шаттлов
RETRY_INTERVAL = 5  # Интервал повтора при сбоях

# Приоритеты команд (чем меньше число, тем выше приоритет)
COMMAND_PRIORITIES = {
    "HOME": 1,
    "PALLET_IN": 2,
    "PALLET_OUT": 2,
    "MOVE_TO_CELL": 2,
    "CHANGE_WAREHOUSE": 2,
    "FIFO": 3,
    "FILO": 3,
    "STACK_IN": 3,
    "STACK_OUT": 3,
    "COUNT": 4,
    "STATUS": 5,
    "BATTERY": 5,
    "WDH": 5,
    "WLH": 5,
    "MRCD": 5
}
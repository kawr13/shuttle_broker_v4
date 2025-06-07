import time
from typing import Dict, Any, Optional
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# Метрики для команд
COMMAND_COUNTER = Counter(
    'shuttle_commands_total', 
    'Total number of commands processed',
    ['shuttle_id', 'command_type', 'status']
)

COMMAND_DURATION = Histogram(
    'shuttle_command_duration_seconds',
    'Command processing duration in seconds',
    ['shuttle_id', 'command_type'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)

# Метрики для шаттлов
SHUTTLE_STATUS = Gauge(
    'shuttle_status',
    'Current shuttle status (0=unknown, 1=free, 2=busy, 3=error)',
    ['shuttle_id']
)

SHUTTLE_BATTERY = Gauge(
    'shuttle_battery_level',
    'Current shuttle battery level in percent',
    ['shuttle_id']
)

SHUTTLE_CONNECTION = Gauge(
    'shuttle_connection_status',
    'Shuttle connection status (1=connected, 0=disconnected)',
    ['shuttle_id']
)

# Метрики для очередей
QUEUE_SIZE = Gauge(
    'command_queue_size',
    'Current size of command queue',
    ['shuttle_id']
)

# Метрики для WMS
WMS_API_REQUESTS = Counter(
    'wms_api_requests_total',
    'Total number of WMS API requests',
    ['endpoint', 'status']
)

WMS_API_DURATION = Histogram(
    'wms_api_request_duration_seconds',
    'WMS API request duration in seconds',
    ['endpoint'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

# Метрики для Redis
REDIS_OPERATIONS = Counter(
    'redis_operations_total',
    'Total number of Redis operations',
    ['operation', 'status']
)

REDIS_OPERATION_DURATION = Histogram(
    'redis_operation_duration_seconds',
    'Redis operation duration in seconds',
    ['operation'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0]
)

# Метрики для системы в целом
SYSTEM_INFO = Gauge(
    'shuttle_gateway_info',
    'Information about the Shuttle Gateway',
    ['version']
)

# Класс для измерения времени выполнения операций
class Timer:
    def __init__(self, metric: Histogram, labels: Dict[str, str]):
        self.metric = metric
        self.labels = labels
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.metric.labels(**self.labels).observe(duration)


def start_metrics_server(port: int = 9090):
    """Запускает HTTP-сервер для метрик Prometheus"""
    start_http_server(port)
    SYSTEM_INFO.labels(version="3.0").set(1)


def update_shuttle_status(shuttle_id: str, status_name: str):
    """Обновляет метрику статуса шаттла"""
    status_map = {
        "UNKNOWN": 0,
        "FREE": 1,
        "BUSY": 2,
        "ERROR": 3,
        "NOT_READY": 4,
        "AWAITING_MRCD": 5,
        "MOVING": 6,
        "LOADING": 7,
        "UNLOADING": 8,
        "CHARGING": 9,
        "LOW_BATTERY": 10
    }
    status_value = status_map.get(status_name, 0)
    SHUTTLE_STATUS.labels(shuttle_id=shuttle_id).set(status_value)


def update_shuttle_battery(shuttle_id: str, battery_level: Optional[str]):
    """Обновляет метрику уровня заряда батареи шаттла"""
    if battery_level is None:
        return
    
    try:
        # Пытаемся извлечь числовое значение из строки
        level_str = battery_level.replace('%', '').strip('<>')
        level = float(level_str)
        SHUTTLE_BATTERY.labels(shuttle_id=shuttle_id).set(level)
    except (ValueError, AttributeError):
        pass


def update_shuttle_connection(shuttle_id: str, connected: bool):
    """Обновляет метрику состояния подключения шаттла"""
    SHUTTLE_CONNECTION.labels(shuttle_id=shuttle_id).set(1 if connected else 0)


def update_queue_size(shuttle_id: str, size: int):
    """Обновляет метрику размера очереди команд"""
    QUEUE_SIZE.labels(shuttle_id=shuttle_id).set(size)


def record_command(shuttle_id: str, command_type: str, status: str):
    """Записывает метрику выполнения команды"""
    COMMAND_COUNTER.labels(shuttle_id=shuttle_id, command_type=command_type, status=status).inc()


def record_wms_request(endpoint: str, status: str):
    """Записывает метрику запроса к WMS API"""
    WMS_API_REQUESTS.labels(endpoint=endpoint, status=status).inc()


def record_redis_operation(operation: str, status: str):
    """Записывает метрику операции с Redis"""
    REDIS_OPERATIONS.labels(operation=operation, status=status).inc()


def command_timer(shuttle_id: str, command_type: str) -> Timer:
    """Возвращает таймер для измерения времени выполнения команды"""
    return Timer(COMMAND_DURATION, {"shuttle_id": shuttle_id, "command_type": command_type})


def wms_request_timer(endpoint: str) -> Timer:
    """Возвращает таймер для измерения времени запроса к WMS API"""
    return Timer(WMS_API_DURATION, {"endpoint": endpoint})


def redis_operation_timer(operation: str) -> Timer:
    """Возвращает таймер для измерения времени операции с Redis"""
    return Timer(REDIS_OPERATION_DURATION, {"operation": operation})
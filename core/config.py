import os
import json
import yaml
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, field, asdict


@dataclass
class ShuttleConfig:
    host: str
    command_port: int = 2000  # Порт шаттла, на который шлюз отправляет команды
    response_port: int = 5000  # Порт шаттла, с которого шаттл отправляет сообщения
    shuttle_health_check_interval: int = 10  # Интервал проверки состояния шаттла в секундах
    
    def to_dict(self):
        return asdict(self)


@dataclass
class WmsConfig:
    api_url: str
    username: str
    password: str
    poll_interval: int = 60  # в секундах
    webhook_url: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file_path: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class GatewayConfig:
    shuttles: Dict[str, ShuttleConfig] = field(default_factory=dict)
    stock_to_shuttle: Dict[str, List[str]] = field(default_factory=dict)
    wms: Optional[WmsConfig] = None
    redis: RedisConfig = field(default_factory=RedisConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    command_queue_max_size: int = 1000
    command_processor_workers: int = 2
    tcp_connect_timeout: float = 5.0
    tcp_read_timeout: float = 20.0
    tcp_write_timeout: float = 5.0
    shuttle_listener_port: int = 8181
    shuttle_health_check_interval: int = 30  # Интервал проверки состояния шаттлов в секундах  # Порт шлюза, на котором он слушает сообщения от шаттлов
    
    def to_dict(self):
        return {
            "shuttles": {k: v.to_dict() for k, v in self.shuttles.items()},
            "stock_to_shuttle": self.stock_to_shuttle,
            "wms": self.wms.to_dict() if self.wms else None,
            "redis": self.redis.to_dict(),
            "logging": self.logging.to_dict(),
            "command_queue_max_size": self.command_queue_max_size,
            "command_processor_workers": self.command_processor_workers,
            "tcp_connect_timeout": self.tcp_connect_timeout,
            "tcp_read_timeout": self.tcp_read_timeout,
            "tcp_write_timeout": self.tcp_write_timeout,
            "shuttle_listener_port": self.shuttle_listener_port,
            "shuttle_health_check_interval": self.shuttle_health_check_interval
        }
    
    def save_to_file(self, file_path: str):
        """Сохраняет конфигурацию в файл"""
        with open(file_path, 'w') as f:
            if file_path.endswith('.json'):
                json.dump(self.to_dict(), f, indent=2)
            elif file_path.endswith('.yaml') or file_path.endswith('.yml'):
                yaml.dump(self.to_dict(), f)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'GatewayConfig':
        """Загружает конфигурацию из файла"""
        with open(file_path, 'r') as f:
            if file_path.endswith('.json'):
                data = json.load(f)
            elif file_path.endswith('.yaml') or file_path.endswith('.yml'):
                data = yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
        
        # Создаем объекты конфигурации
        shuttles = {
            k: ShuttleConfig(**v) for k, v in data.get('shuttles', {}).items()
        }
        
        wms = None
        if data.get('wms'):
            wms = WmsConfig(**data['wms'])
        
        redis_config = RedisConfig(**data.get('redis', {}))
        logging_config = LoggingConfig(**data.get('logging', {}))
        
        return cls(
            shuttles=shuttles,
            stock_to_shuttle=data.get('stock_to_shuttle', {}),
            wms=wms,
            redis=redis_config,
            logging=logging_config,
            command_queue_max_size=data.get('command_queue_max_size', 1000),
            command_processor_workers=data.get('command_processor_workers', 2),
            tcp_connect_timeout=data.get('tcp_connect_timeout', 5.0),
            tcp_read_timeout=data.get('tcp_read_timeout', 20.0),
            tcp_write_timeout=data.get('tcp_write_timeout', 5.0),
            shuttle_listener_port=data.get('shuttle_listener_port', 8181),
            shuttle_health_check_interval=data.get('shuttle_health_check_interval', 30)
        )
    
    @classmethod
    def load_from_env(cls) -> 'GatewayConfig':
        """Загружает конфигурацию из переменных окружения"""
        # Загрузка конфигурации Redis
        redis_config = RedisConfig(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('REDIS_DB', '0')),
            password=os.getenv('REDIS_PASSWORD')
        )
        
        # Загрузка конфигурации логирования
        logging_config = LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'INFO'),
            file_path=os.getenv('LOG_FILE')
        )
        
        # Загрузка конфигурации WMS
        wms_config = None
        if os.getenv('WMS_API_URL'):
            wms_config = WmsConfig(
                api_url=os.getenv('WMS_API_URL'),
                username=os.getenv('WMS_API_USERNAME', ''),
                password=os.getenv('WMS_API_PASSWORD', ''),
                poll_interval=int(os.getenv('WMS_POLL_INTERVAL', '60')),
                webhook_url=os.getenv('WMS_WEBHOOK_URL')
            )
        
        return cls(
            redis=redis_config,
            logging=logging_config,
            wms=wms_config,
            command_queue_max_size=int(os.getenv('COMMAND_QUEUE_MAX_SIZE', '1000')),
            command_processor_workers=int(os.getenv('COMMAND_PROCESSOR_WORKERS', '2')),
            tcp_connect_timeout=float(os.getenv('TCP_CONNECT_TIMEOUT', '5.0')),
            tcp_read_timeout=float(os.getenv('TCP_READ_TIMEOUT', '20.0')),
            tcp_write_timeout=float(os.getenv('TCP_WRITE_TIMEOUT', '5.0')),
            shuttle_listener_port=int(os.getenv('SHUTTLE_LISTENER_PORT', '8181')),
            shuttle_health_check_interval=int(os.getenv('SHUTTLE_HEALTH_CHECK_INTERVAL', '30'))
        )


# Глобальный экземпляр конфигурации
config = None


def load_config(config_file: Optional[str] = None) -> GatewayConfig:
    """Загружает конфигурацию из файла или переменных окружения"""
    global config
    
    if config_file and os.path.exists(config_file):
        config = GatewayConfig.load_from_file(config_file)
    else:
        config = GatewayConfig.load_from_env()
    
    return config


def get_config() -> GatewayConfig:
    """Возвращает текущую конфигурацию"""
    global config
    if config is None:
        config = load_config()
    return config
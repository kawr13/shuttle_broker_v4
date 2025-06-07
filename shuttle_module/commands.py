from enum import Enum
from typing import Optional, Dict, Any


class ShuttleCommandEnum(str, Enum):
    """Команды, которые можно отправить шаттлу"""
    PALLET_IN = "PALLET_IN"
    PALLET_OUT = "PALLET_OUT"
    FIFO = "FIFO"
    FILO = "FILO"
    STACK_IN = "STACK_IN"
    STACK_OUT = "STACK_OUT"
    HOME = "HOME"
    COUNT = "COUNT"
    STATUS = "STATUS"
    BATTERY = "BATTERY"
    WDH = "WDH"
    WLH = "WLH"
    MRCD = "MRCD"


class ShuttleStatus(str, Enum):
    """Статусы шаттла"""
    FREE = "FREE"
    BUSY = "BUSY"
    ERROR = "ERROR"
    NOT_READY = "NOT_READY"
    AWAITING_MRCD = "AWAITING_MRCD"
    UNKNOWN = "UNKNOWN"
    MOVING = "MOVING"
    LOADING = "LOADING"
    UNLOADING = "UNLOADING"
    CHARGING = "CHARGING"
    LOW_BATTERY = "LOW_BATTERY"


class CommandPriority(int, Enum):
    """Приоритеты команд (меньше = выше)"""
    HOME = 1
    STATUS = 2
    BATTERY = 3
    MRCD = 4
    PALLET_OUT = 5
    PALLET_IN = 6
    STACK_OUT = 7
    STACK_IN = 8
    FIFO = 9
    FILO = 10
    COUNT = 11
    WDH = 12
    WLH = 13


class ShuttleCommand:
    """Класс для работы с командами шаттла"""
    
    def __init__(
        self,
        command_type: ShuttleCommandEnum,
        shuttle_id: str,
        params: Optional[str] = None,
        external_id: Optional[str] = None,
        priority: Optional[int] = None,
        document_type: Optional[str] = None,
        cell_id: Optional[str] = None,
        stock_name: Optional[str] = None
    ):
        self.command_type = command_type
        self.shuttle_id = shuttle_id
        self.params = params
        self.external_id = external_id
        self.document_type = document_type
        self.cell_id = cell_id
        self.stock_name = stock_name
        
        # Определяем приоритет команды
        if priority is not None:
            self.priority = priority
        else:
            # Используем приоритет по умолчанию для типа команды
            try:
                self.priority = CommandPriority[command_type.name].value
            except (KeyError, ValueError):
                self.priority = 10  # Средний приоритет по умолчанию
    
    def to_string(self) -> str:
        """Преобразует команду в строку для отправки шаттлу"""
        if self.params:
            if self.command_type in [ShuttleCommandEnum.FIFO, ShuttleCommandEnum.FILO]:
                # Для FIFO и FILO параметр - это число с ведущими нулями
                try:
                    param_value = int(self.params)
                    command_str = f"{self.command_type.value}-{param_value:03d}"
                except ValueError:
                    command_str = f"{self.command_type.value}-{self.params}"
            else:
                command_str = f"{self.command_type.value}-{self.params}"
        else:
            command_str = self.command_type.value
        
        # Добавляем перевод строки, если его нет
        if not command_str.endswith('\n'):
            command_str += '\n'
        
        return command_str
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует команду в словарь для сохранения"""
        return {
            "command_type": self.command_type.value,
            "shuttle_id": self.shuttle_id,
            "params": self.params,
            "external_id": self.external_id,
            "priority": self.priority,
            "document_type": self.document_type,
            "cell_id": self.cell_id,
            "stock_name": self.stock_name
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ShuttleCommand':
        """Создает команду из словаря"""
        return cls(
            command_type=ShuttleCommandEnum[data["command_type"]],
            shuttle_id=data["shuttle_id"],
            params=data.get("params"),
            external_id=data.get("external_id"),
            priority=data.get("priority"),
            document_type=data.get("document_type"),
            cell_id=data.get("cell_id"),
            stock_name=data.get("stock_name")
        )
    
    def __lt__(self, other):
        """Для сравнения команд по приоритету в очереди"""
        if not isinstance(other, ShuttleCommand):
            return NotImplemented
        return self.priority < other.priority
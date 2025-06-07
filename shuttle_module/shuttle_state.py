import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

from .commands import ShuttleStatus


@dataclass
class ShuttleState:
    """Состояние шаттла"""
    shuttle_id: str
    status: ShuttleStatus = ShuttleStatus.UNKNOWN
    current_command: Optional[str] = None
    last_command: Optional[Any] = None
    last_command_time: Optional[float] = None
    last_message: Optional[str] = None
    last_seen: float = field(default_factory=time.time)
    battery_level: Optional[str] = None
    location_data: Optional[str] = None
    pallet_count_data: Optional[str] = None
    wdh_hours: Optional[int] = None
    wlh_hours: Optional[int] = None
    error_code: Optional[str] = None
    external_id: Optional[str] = None
    document_type: Optional[str] = None
    cell_id: Optional[str] = None
    stock_name: Optional[str] = None
    current_cell: Optional[str] = None  # Текущая ячейка шаттла
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует состояние в словарь для сохранения"""
        result = {
            "shuttle_id": self.shuttle_id,
            "status": self.status.value if self.status else None,
            "current_command": self.current_command,
            "last_command_time": self.last_command_time,
            "last_message": self.last_message,
            "last_seen": self.last_seen,
            "battery_level": self.battery_level,
            "location_data": self.location_data,
            "pallet_count_data": self.pallet_count_data,
            "wdh_hours": self.wdh_hours,
            "wlh_hours": self.wlh_hours,
            "error_code": self.error_code,
            "external_id": self.external_id,
            "document_type": self.document_type,
            "cell_id": self.cell_id,
            "stock_name": self.stock_name
        }
        
        # Преобразуем last_command в словарь, если он есть
        if self.last_command:
            result["last_command"] = self.last_command.to_dict()
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ShuttleState':
        """Создает состояние из словаря"""
        # Копируем словарь, чтобы не изменять оригинал
        data_copy = data.copy()
        
        # Преобразуем строковый статус в перечисление
        if "status" in data_copy and data_copy["status"]:
            try:
                data_copy["status"] = ShuttleStatus(data_copy["status"])
            except ValueError:
                data_copy["status"] = ShuttleStatus.UNKNOWN
        
        # Удаляем last_command из словаря, так как он требует специальной обработки
        last_command_dict = data_copy.pop("last_command", None)
        
        # Создаем объект состояния
        state = cls(**data_copy)
        
        # Восстанавливаем last_command, если он есть
        if last_command_dict:
            from .commands import ShuttleCommand
            state.last_command = ShuttleCommand.from_dict(last_command_dict)
        
        return state

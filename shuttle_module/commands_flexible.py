from enum import Enum
from typing import Optional, Dict, Any, Union
import binascii

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
    
    # Доступные форматы терминаторов
    TERMINATORS = {
        "LF": b"\n",           # Unix
        "CRLF": b"\r\n",       # Windows
        "CR": b"\r",           # Mac
        "NULL": b"\0",         # Нулевой байт
        "NONE": b"",           # Без терминатора
        "ETX": b"\x03",        # End of Text
        "EOT": b"\x04",        # End of Transmission
    }
    
    # Формат команды по умолчанию
    DEFAULT_TERMINATOR = "CRLF"
    DEFAULT_PREFIX = ""
    DEFAULT_SEPARATOR = "-"
    DEFAULT_SUFFIX = ""
    DEFAULT_ENCODING = "utf-8"
    
    def __init__(
        self,
        command_type: ShuttleCommandEnum,
        shuttle_id: str,
        params: Optional[str] = None,
        external_id: Optional[str] = None,
        priority: Optional[int] = None,
        document_type: Optional[str] = None,
        cell_id: Optional[str] = None,
        stock_name: Optional[str] = None,
        terminator: str = DEFAULT_TERMINATOR,
        prefix: str = DEFAULT_PREFIX,
        separator: str = DEFAULT_SEPARATOR,
        suffix: str = DEFAULT_SUFFIX,
        encoding: str = DEFAULT_ENCODING,
        raw_command: Optional[bytes] = None
    ):
        self.command_type = command_type
        self.shuttle_id = shuttle_id
        self.params = params
        self.external_id = external_id
        self.document_type = document_type
        self.cell_id = cell_id
        self.stock_name = stock_name
        self.terminator = terminator
        self.prefix = prefix
        self.separator = separator
        self.suffix = suffix
        self.encoding = encoding
        self.raw_command = raw_command
        
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
        if self.raw_command:
            # Если задана сырая команда, возвращаем её как есть
            return self.raw_command.decode(self.encoding, errors='replace')
        
        # Формируем команду
        if self.params:
            if self.command_type in [ShuttleCommandEnum.FIFO, ShuttleCommandEnum.FILO]:
                # Для FIFO и FILO параметр - это число с ведущими нулями
                try:
                    param_value = int(self.params)
                    command_str = f"{self.prefix}{self.command_type.value}{self.separator}{param_value:03d}{self.suffix}"
                except ValueError:
                    command_str = f"{self.prefix}{self.command_type.value}{self.separator}{self.params}{self.suffix}"
            else:
                command_str = f"{self.prefix}{self.command_type.value}{self.separator}{self.params}{self.suffix}"
        else:
            command_str = f"{self.prefix}{self.command_type.value}{self.suffix}"
        
        # Добавляем терминатор
        terminator_bytes = self.TERMINATORS.get(self.terminator, b"\r\n")
        terminator_str = terminator_bytes.decode('latin1')
        
        return command_str + terminator_str
    
    def to_bytes(self) -> bytes:
        """Преобразует команду в байты для отправки шаттлу"""
        if self.raw_command:
            # Если задана сырая команда, возвращаем её как есть
            return self.raw_command
        
        # Получаем строковое представление команды без терминатора
        if self.params:
            if self.command_type in [ShuttleCommandEnum.FIFO, ShuttleCommandEnum.FILO]:
                # Для FIFO и FILO параметр - это число с ведущими нулями
                try:
                    param_value = int(self.params)
                    command_str = f"{self.prefix}{self.command_type.value}{self.separator}{param_value:03d}{self.suffix}"
                except ValueError:
                    command_str = f"{self.prefix}{self.command_type.value}{self.separator}{self.params}{self.suffix}"
            else:
                command_str = f"{self.prefix}{self.command_type.value}{self.separator}{self.params}{self.suffix}"
        else:
            command_str = f"{self.prefix}{self.command_type.value}{self.suffix}"
        
        # Кодируем команду
        command_bytes = command_str.encode(self.encoding)
        
        # Добавляем терминатор
        terminator_bytes = self.TERMINATORS.get(self.terminator, b"\r\n")
        
        return command_bytes + terminator_bytes
    
    def to_hex(self) -> str:
        """Преобразует команду в шестнадцатеричную строку"""
        return self.to_bytes().hex()
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует команду в словарь для сохранения"""
        result = {
            "command_type": self.command_type.value,
            "shuttle_id": self.shuttle_id,
            "params": self.params,
            "external_id": self.external_id,
            "priority": self.priority,
            "document_type": self.document_type,
            "cell_id": self.cell_id,
            "stock_name": self.stock_name,
            "terminator": self.terminator,
            "prefix": self.prefix,
            "separator": self.separator,
            "suffix": self.suffix,
            "encoding": self.encoding
        }
        
        if self.raw_command:
            result["raw_command_hex"] = self.raw_command.hex()
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ShuttleCommand':
        """Создает команду из словаря"""
        raw_command = None
        if "raw_command_hex" in data:
            try:
                raw_command = binascii.unhexlify(data["raw_command_hex"])
            except:
                pass
        
        return cls(
            command_type=ShuttleCommandEnum[data["command_type"]],
            shuttle_id=data["shuttle_id"],
            params=data.get("params"),
            external_id=data.get("external_id"),
            priority=data.get("priority"),
            document_type=data.get("document_type"),
            cell_id=data.get("cell_id"),
            stock_name=data.get("stock_name"),
            terminator=data.get("terminator", cls.DEFAULT_TERMINATOR),
            prefix=data.get("prefix", cls.DEFAULT_PREFIX),
            separator=data.get("separator", cls.DEFAULT_SEPARATOR),
            suffix=data.get("suffix", cls.DEFAULT_SUFFIX),
            encoding=data.get("encoding", cls.DEFAULT_ENCODING),
            raw_command=raw_command
        )
    
    @classmethod
    def from_raw(cls, raw_command: Union[bytes, str], shuttle_id: str, encoding: str = DEFAULT_ENCODING) -> 'ShuttleCommand':
        """Создает команду из сырых данных"""
        if isinstance(raw_command, str):
            raw_bytes = raw_command.encode(encoding)
        else:
            raw_bytes = raw_command
        
        return cls(
            command_type=ShuttleCommandEnum.STATUS,  # Значение по умолчанию
            shuttle_id=shuttle_id,
            raw_command=raw_bytes,
            encoding=encoding
        )
    
    def __lt__(self, other):
        """Для сравнения команд по приоритету в очереди"""
        if not isinstance(other, ShuttleCommand):
            return NotImplemented
        return self.priority < other.priority
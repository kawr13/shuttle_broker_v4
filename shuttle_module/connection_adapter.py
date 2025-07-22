import asyncio
import logging
from typing import Dict, Tuple, Optional

from shuttle_module.connection_manager import get_connection_manager

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Адаптер для интеграции нового менеджера соединений с существующим кодом"""
    
    def __init__(self):
        self.connections: Dict[str, Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
    
    async def get_connection(self, shuttle_id: str, host: str, port: int, timeout: float = 5.0) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Получает соединение с шаттлом"""
        # Проверяем, есть ли уже соединение
        if shuttle_id in self.connections:
            reader, writer = self.connections[shuttle_id]
            if not writer.is_closing():
                return reader, writer
        
        # Устанавливаем новое соединение через менеджер постоянных соединений
        conn_manager = get_connection_manager()
        success = await conn_manager.connect_to_shuttle(host, port)
        if not success:
            raise ConnectionError(f"Не удалось подключиться к шаттлу {shuttle_id} ({host}:{port})")
        
        # Создаем фиктивные reader и writer для совместимости с существующим кодом
        # В реальности все операции будут выполняться через connection_manager
        reader = FakeReader(shuttle_id, host)
        writer = FakeWriter(shuttle_id, host)
        
        self.connections[shuttle_id] = (reader, writer)
        return reader, writer
    
    async def close_connection(self, shuttle_id: str):
        """Закрывает соединение с шаттлом"""
        if shuttle_id in self.connections:
            del self.connections[shuttle_id]

class FakeReader:
    """Фиктивный StreamReader для совместимости с существующим кодом"""
    
    def __init__(self, shuttle_id: str, host: str):
        self.shuttle_id = shuttle_id
        self.host = host
    
    async def read(self, n: int = -1) -> bytes:
        """Эта функция не должна вызываться, т.к. чтение происходит через ShuttleListener"""
        logger.warning(f"Попытка чтения через FakeReader для шаттла {self.shuttle_id}")
        return b""
    
    async def readline(self) -> bytes:
        """Эта функция не должна вызываться, т.к. чтение происходит через ShuttleListener"""
        logger.warning(f"Попытка чтения строки через FakeReader для шаттла {self.shuttle_id}")
        return b""

class FakeWriter:
    """Фиктивный StreamWriter для совместимости с существующим кодом"""
    
    def __init__(self, shuttle_id: str, host: str):
        self.shuttle_id = shuttle_id
        self.host = host
        self._closing = False
    
    def write(self, data: bytes):
        """Перенаправляет запись данных через менеджер соединений"""
        # Преобразуем данные в строку и отправляем через connection_manager
        command = data.decode('utf-8').strip()
        # Создаем задачу для асинхронной отправки
        conn_manager = get_connection_manager()
        asyncio.create_task(conn_manager.send_command(self.host, command))
    
    async def drain(self):
        """Ничего не делает, т.к. отправка происходит через connection_manager"""
        pass
    
    def close(self):
        """Помечает соединение как закрывающееся"""
        self._closing = True
    
    async def wait_closed(self):
        """Ничего не делает, т.к. закрытие происходит через connection_manager"""
        pass
    
    def is_closing(self) -> bool:
        """Возвращает статус закрытия соединения"""
        return self._closing

# Глобальный экземпляр менеджера соединений
_connection_manager = None

def get_connection_manager() -> ConnectionManager:
    """Возвращает глобальный экземпляр менеджера соединений"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
    return _connection_manager
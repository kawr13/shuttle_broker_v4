import asyncio
from typing import Dict, Optional, Set, Tuple
import time

from core.logging import get_logger

logger = get_logger()


class ConnectionManager:
    """Централизованный менеджер соединений к шаттлам"""
    
    def __init__(self):
        self.connections: Dict[str, asyncio.StreamWriter] = {}
        self.readers: Dict[str, asyncio.StreamReader] = {}
        self.connection_times: Dict[str, float] = {}
        self.connecting: Set[str] = set()
        self.lock = asyncio.Lock()
    
    async def get_connection(self, shuttle_id: str, host: str, port: int, timeout: float = 5.0) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """
        Получает соединение с шаттлом или создает новое, если его нет
        """
        async with self.lock:
            # Если соединение уже существует, возвращаем его
            if shuttle_id in self.connections and shuttle_id in self.readers:
                logger.debug(f"Используем существующее соединение с шаттлом {shuttle_id}")
                return self.readers[shuttle_id], self.connections[shuttle_id]
            
            # Если соединение в процессе установки, ждем
            if shuttle_id in self.connecting:
                logger.debug(f"Соединение с шаттлом {shuttle_id} уже устанавливается, ожидаем")
                while shuttle_id in self.connecting:
                    await asyncio.sleep(0.1)
                
                # Проверяем, было ли соединение успешно установлено
                if shuttle_id in self.connections and shuttle_id in self.readers:
                    return self.readers[shuttle_id], self.connections[shuttle_id]
            
            # Устанавливаем новое соединение
            self.connecting.add(shuttle_id)
            try:
                logger.info(f"Устанавливаем новое соединение с шаттлом {shuttle_id} ({host}:{port})")
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=timeout
                )
                
                self.readers[shuttle_id] = reader
                self.connections[shuttle_id] = writer
                self.connection_times[shuttle_id] = time.time()
                
                logger.info(f"Установлено соединение с шаттлом {shuttle_id} ({host}:{port})")
                return reader, writer
            except Exception as e:
                logger.error(f"Ошибка при подключении к шаттлу {shuttle_id}: {e}")
                raise
            finally:
                self.connecting.remove(shuttle_id)
    
    async def close_connection(self, shuttle_id: str):
        """Закрывает соединение с шаттлом"""
        async with self.lock:
            if shuttle_id in self.connections:
                writer = self.connections[shuttle_id]
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    logger.error(f"Ошибка при закрытии соединения с шаттлом {shuttle_id}: {e}")
                
                del self.connections[shuttle_id]
                del self.readers[shuttle_id]
                if shuttle_id in self.connection_times:
                    del self.connection_times[shuttle_id]
                
                logger.info(f"Соединение с шаттлом {shuttle_id} закрыто")
    
    def is_connected(self, shuttle_id: str) -> bool:
        """Проверяет, установлено ли соединение с шаттлом"""
        return shuttle_id in self.connections
    
    def get_connection_time(self, shuttle_id: str) -> Optional[float]:
        """Возвращает время установки соединения с шаттлом"""
        return self.connection_times.get(shuttle_id)
    
    def register_connection(self, shuttle_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Регистрирует существующее соединение в менеджере"""
        self.readers[shuttle_id] = reader
        self.connections[shuttle_id] = writer
        self.connection_times[shuttle_id] = time.time()
        logger.info(f"Соединение с шаттлом {shuttle_id} зарегистрировано в менеджере")


# Глобальный экземпляр менеджера соединений
connection_manager = None


def get_connection_manager() -> ConnectionManager:
    """Возвращает глобальный экземпляр менеджера соединений"""
    global connection_manager
    if connection_manager is None:
        connection_manager = ConnectionManager()
    return connection_manager
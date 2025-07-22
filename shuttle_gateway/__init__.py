"""
Модуль для интеграции веб-сервера с основным приложением
"""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Глобальная переменная для хранения экземпляра веб-сервера
_web_server_runner = None

async def start_web_server() -> Optional[object]:
    """Запускает веб-сервер"""
    global _web_server_runner
    
    try:
        # Импортируем модуль веб-сервера
        from shuttle_gateway.web_server import start_web_server as _start_web_server
        
        # Запускаем веб-сервер
        _web_server_runner = await _start_web_server()
        return _web_server_runner
    except Exception as e:
        logger.error(f"Ошибка при запуске веб-сервера: {e}")
        return None

async def stop_web_server():
    """Останавливает веб-сервер"""
    global _web_server_runner
    
    if _web_server_runner:
        try:
            await _web_server_runner.cleanup()
            logger.info("Веб-сервер остановлен")
        except Exception as e:
            logger.error(f"Ошибка при остановке веб-сервера: {e}")
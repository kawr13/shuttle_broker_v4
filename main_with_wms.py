#!/usr/bin/env python3
"""
Основной скрипт запуска системы с интеграцией WMS
"""
import asyncio
import signal
import sys
from core.config import load_config
from core.logging import get_logger
from shuttle_module.shuttle_manager import get_shuttle_manager
from shuttle_module.shuttle_listener import get_shuttle_listener
from wms_integration_improved import get_wms_integration

logger = get_logger()

class ShuttleGatewayWithWMS:
    """Основной класс системы шлюза с интеграцией WMS"""
    
    def __init__(self):
        self.config = load_config()
        self.shuttle_manager = get_shuttle_manager()
        self.shuttle_listener = get_shuttle_listener()
        self.wms_integration = None
        self.running = False
        
        # Инициализируем WMS интеграцию если настроена
        if self.config.wms:
            try:
                self.wms_integration = get_wms_integration()
                logger.info("WMS интеграция инициализирована")
            except Exception as e:
                logger.error(f"Ошибка инициализации WMS интеграции: {e}")
                self.wms_integration = None
        else:
            logger.warning("WMS не настроена в конфигурации")
    
    async def start(self):
        """Запускает все компоненты системы"""
        if self.running:
            return
        
        self.running = True
        logger.info("🚀 Запуск системы шлюза шаттлов с WMS интеграцией")
        
        try:
            # Запускаем менеджер шаттлов
            logger.info("Запуск менеджера шаттлов...")
            await self.shuttle_manager.start()
            
            # Запускаем слушатель шаттлов
            logger.info("Запуск слушателя шаттлов...")
            await self.shuttle_listener.start()
            
            # Запускаем WMS интеграцию если доступна
            if self.wms_integration:
                logger.info("Запуск WMS интеграции...")
                wms_task = asyncio.create_task(self.wms_integration.start())
            else:
                wms_task = None
            
            logger.info("✅ Все компоненты системы запущены")
            
            # Ждем сигнала остановки
            await self._wait_for_shutdown()
            
        except Exception as e:
            logger.error(f"Ошибка при запуске системы: {e}")
            raise
        finally:
            await self.stop()
    
    async def stop(self):
        """Останавливает все компоненты системы"""
        if not self.running:
            return
        
        self.running = False
        logger.info("🛑 Остановка системы шлюза шаттлов")
        
        try:
            # Останавливаем WMS интеграцию
            if self.wms_integration:
                logger.info("Остановка WMS интеграции...")
                await self.wms_integration.stop()
            
            # Останавливаем слушатель шаттлов
            logger.info("Остановка слушателя шаттлов...")
            await self.shuttle_listener.stop()
            
            # Останавливаем менеджер шаттлов
            logger.info("Остановка менеджера шаттлов...")
            await self.shuttle_manager.stop()
            
            logger.info("✅ Все компоненты системы остановлены")
            
        except Exception as e:
            logger.error(f"Ошибка при остановке системы: {e}")
    
    async def _wait_for_shutdown(self):
        """Ждет сигнала остановки"""
        shutdown_event = asyncio.Event()
        
        def signal_handler(signum, frame):
            logger.info(f"Получен сигнал {signum}, инициируем остановку...")
            shutdown_event.set()
        
        # Регистрируем обработчики сигналов
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Ждем сигнала остановки
        await shutdown_event.wait()
    
    def get_system_status(self) -> dict:
        """Возвращает статус системы"""
        status = {
            'running': self.running,
            'shuttle_manager': {
                'running': self.shuttle_manager.running if self.shuttle_manager else False,
                'shuttles_count': len(self.shuttle_manager.shuttles) if self.shuttle_manager else 0
            },
            'shuttle_listener': {
                'running': self.shuttle_listener.running if self.shuttle_listener else False,
                'connections': len(self.shuttle_listener.connections) if self.shuttle_listener else 0
            },
            'wms_integration': {
                'enabled': self.wms_integration is not None,
                'documents_tracked': len(self.wms_integration.documents_state) if self.wms_integration else 0
            }
        }
        
        return status

async def main():
    """Основная функция"""
    gateway = ShuttleGatewayWithWMS()
    
    try:
        await gateway.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания от пользователя")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
        sys.exit(1)
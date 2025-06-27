import asyncio
import json
import logging
from typing import Dict
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shuttle.shuttle_client import ShuttleClient
from config import STATE_POLL_INTERVAL

logger = logging.getLogger(__name__)

class ShuttleMonitor:
    def __init__(self, shuttle_client: ShuttleClient):
        self.shuttle_client = shuttle_client
        self.shuttle_states = {}  # {ip: {battery: ..., errors: ..., servos: ..., cell: ..., warehouse: ...}}
    
    async def get_shuttle_state(self, ip: str) -> Dict:
        """Получить состояние шаттла"""
        state = {"ip": ip, "battery": None, "errors": None, "servos": None}
        
        # Получить данные о батарее
        if await self.shuttle_client.send_command(ip, "BATTERY"):
            # Ответ будет обработан в listen_shuttles
            pass
        
        # Получить статус
        if await self.shuttle_client.send_command(ip, "STATUS"):
            pass
        
        # Получить данные MRCD
        if await self.shuttle_client.send_command(ip, "MRCD"):
            pass
        
        # Добавить информацию о местоположении
        if ip in self.shuttle_client.shuttles:
            shuttle_info = self.shuttle_client.shuttles[ip]
            state["cell"] = shuttle_info.get("cell", "Unknown")
            state["warehouse"] = shuttle_info.get("warehouse", "Unknown")
        
        return state
    
    def update_shuttle_state(self, ip: str, key: str, value: str):
        """Обновить состояние шаттла"""
        if ip not in self.shuttle_states:
            self.shuttle_states[ip] = {
                "battery": None, "errors": None, "servos": None,
                "cell": "Unknown", "warehouse": "Unknown"
            }
        
        self.shuttle_states[ip][key] = value
        
        # Обновить местоположение из конфигурации
        if ip in self.shuttle_client.shuttles:
            shuttle_info = self.shuttle_client.shuttles[ip]
            self.shuttle_states[ip]["cell"] = shuttle_info.get("cell", "Unknown")
            self.shuttle_states[ip]["warehouse"] = shuttle_info.get("warehouse", "Unknown")
    
    def log_shuttle_state(self, ip: str):
        """Логировать состояние шаттла"""
        if ip in self.shuttle_states:
            state = self.shuttle_states[ip]
            logger.info(
                f"Состояние шаттла {ip}: "
                f"батарея={state.get('battery', 'N/A')}, "
                f"ошибки={state.get('errors', 'N/A')}, "
                f"сервоприводы={state.get('servos', 'N/A')}, "
                f"ячейка={state.get('cell', 'Unknown')}, "
                f"склад={state.get('warehouse', 'Unknown')}"
            )
    
    async def monitor_shuttle_states(self):
        """Основной цикл мониторинга состояний шаттлов"""
        logger.info("Запуск мониторинга состояний шаттлов")
        
        while True:
            try:
                logger.debug("Начало цикла мониторинга шаттлов")
                
                # Обновить список шаттлов
                self.shuttle_client.load_shuttles_config()
                
                # Опросить каждый шаттл
                for ip in self.shuttle_client.shuttles:
                    try:
                        # Отправить команды для получения состояния
                        await self.shuttle_client.send_command(ip, "BATTERY")
                        await asyncio.sleep(0.5)
                        
                        await self.shuttle_client.send_command(ip, "STATUS")
                        await asyncio.sleep(0.5)
                        
                        await self.shuttle_client.send_command(ip, "MRCD")
                        await asyncio.sleep(0.5)
                        
                        # Логировать текущее состояние
                        self.log_shuttle_state(ip)
                        
                    except Exception as e:
                        logger.error(f"Ошибка опроса шаттла {ip}: {e}")
                
                await asyncio.sleep(STATE_POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга шаттлов: {e}")
                await asyncio.sleep(STATE_POLL_INTERVAL)
    
    def export_states_to_json(self, filename: str = "shuttle_states.json"):
        """Экспортировать состояния в JSON файл"""
        try:
            with open(filename, 'w') as f:
                json.dump(self.shuttle_states, f, indent=2)
            logger.info(f"Состояния шаттлов экспортированы в {filename}")
        except Exception as e:
            logger.error(f"Ошибка экспорта состояний: {e}")
    
    def get_all_states(self) -> Dict:
        """Получить все состояния шаттлов"""
        return self.shuttle_states.copy()
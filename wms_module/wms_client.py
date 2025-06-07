import asyncio
import base64
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import aiohttp

from core.config import get_config
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommandEnum
from utils.retry import retry_async

logger = get_logger()


class WmsClient:
    """Клиент для взаимодействия с WMS API"""
    
    def __init__(self):
        config = get_config()
        if not config.wms:
            raise ValueError("WMS configuration is not set")
        
        self.api_url = config.wms.api_url.rstrip('/')
        self.username = config.wms.username
        self.password = config.wms.password
        self.poll_interval = config.wms.poll_interval
        self.webhook_url = config.wms.webhook_url
        
        self.last_poll_time = datetime.now() - timedelta(minutes=30)  # Начинаем с получения команд за последние 30 минут
        self.processed_commands = set()
        
        # Маппинг команд WMS на команды шаттлов
        self.command_mapping = {
            "PALLET_IN": ShuttleCommandEnum.PALLET_IN,
            "PALLET_OUT": ShuttleCommandEnum.PALLET_OUT,
            "FIFO": ShuttleCommandEnum.FIFO,
            "FILO": ShuttleCommandEnum.FILO,
            "STACK_IN": ShuttleCommandEnum.STACK_IN,
            "STACK_OUT": ShuttleCommandEnum.STACK_OUT,
            "HOME": ShuttleCommandEnum.HOME,
            "COUNT": ShuttleCommandEnum.COUNT,
            "STATUS": ShuttleCommandEnum.STATUS
        }
    
    def _get_auth_header(self) -> Dict[str, str]:
        """Формирует заголовок авторизации"""
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = auth_string.encode('ascii')
        base64_bytes = base64.b64encode(auth_bytes)
        base64_auth = base64_bytes.decode('ascii')
        
        return {
            "Authorization": f"Basic {base64_auth}",
            "Content-Type": "application/json"
        }
    
    async def get_shipment_commands(self) -> List[Dict]:
        """Получает команды из отгрузок"""
        start_time = self.last_poll_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        url = f"{self.api_url}/exec?action=IncomeApi.getShipmentStatusesPeriod&p={start_time}&p={end_time}"
        
        auth_header = self._get_auth_header()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=auth_header, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("shipment", [])
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при получении отгрузок: {response.status}, {error_text}")
                        return []
        except Exception as e:
            logger.error(f"Ошибка при получении отгрузок: {e}")
            return []
    
    async def get_transfer_commands(self) -> List[Dict]:
        """Получает команды из перемещений"""
        start_time = self.last_poll_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        url = f"{self.api_url}/exec?action=IncomeApi.getTransferStatusesPeriod&p={start_time}&p={end_time}"
        
        auth_header = self._get_auth_header()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=auth_header, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("transfer", [])
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при получении перемещений: {response.status}, {error_text}")
                        return []
        except Exception as e:
            logger.error(f"Ошибка при получении перемещений: {e}")
            return []
    
    async def get_command_details(self, external_id: str, command_type: str) -> Optional[Dict]:
        """Получает детали команды по её ID"""
        action = ""
        if command_type == "shipment":
            action = "IncomeApi.getObject&p=shipment"
        elif command_type == "transfer":
            action = "IncomeApi.getObject&p=transfer"
        
        url = f"{self.api_url}/exec?action={action}&p={external_id}"
        
        auth_header = self._get_auth_header()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=auth_header, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if command_type == "shipment":
                            return data.get("shipment", [{}])[0]
                        elif command_type == "transfer":
                            return data.get("transfer", [{}])[0]
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при получении деталей команды {external_id}: {response.status}, {error_text}")
                        return None
        except Exception as e:
            logger.error(f"Ошибка при получении деталей команды {external_id}: {e}")
            return None
    
    async def update_status(self, external_id: str, document_type: str, status: str) -> bool:
        """Обновляет статус команды в WMS"""
        url = f"{self.api_url}/exec?action=IncomeApi.insertUpdate"
        
        # Формируем тело запроса в зависимости от типа документа
        body = {}
        if document_type == "shipment":
            body = {
                "shipment": [
                    {
                        "externalId": external_id,
                        "shipmentLine": [
                            {
                                "externalId": external_id,
                                "quantityShipped": 1,  # Предполагаем, что количество = 1
                                "status": status
                            }
                        ]
                    }
                ]
            }
        elif document_type == "transfer":
            body = {
                "transfer": [
                    {
                        "externalId": external_id,
                        "transferLine": [
                            {
                                "externalId": external_id,
                                "quantityTransferred": 1,  # Предполагаем, что количество = 1
                                "status": status
                            }
                        ]
                    }
                ]
            }
        
        auth_header = self._get_auth_header()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=auth_header, json=body, timeout=10) as response:
                    if response.status == 200:
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка при обновлении статуса в WMS: {response.status}, {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса в WMS: {e}")
            return False
    
    async def send_webhook(self, shuttle_id: str, message: str, status: str, error_code: Optional[str] = None,
                          external_id: Optional[str] = None) -> bool:
        """Отправляет webhook в WMS"""
        if not self.webhook_url:
            return False
        
        payload = {
            "shuttle_id": shuttle_id,
            "message": message,
            "status": status,
            "error_code": error_code,
            "externaIID": external_id,
            "timestamp": time.time()
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload, timeout=10) as response:
                    if response.status >= 200 and response.status < 300:
                        logger.info(f"Webhook успешно отправлен в WMS для {shuttle_id}: {message}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка отправки webhook в WMS: {response.status}, {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Ошибка при отправке webhook в WMS: {e}")
            return False
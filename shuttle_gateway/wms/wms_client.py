import asyncio
import aiohttp
import logging
import base64
from typing import Dict, List, Optional
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import WMS_API_URL, USERNAME, PASSWORD, POLL_INTERVAL

logger = logging.getLogger(__name__)

class WMSClient:
    def __init__(self):
        self.auth_header = self._create_auth_header()
        self.session = None
        
    def _create_auth_header(self) -> str:
        credentials = f"{USERNAME}:{PASSWORD}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def get_documents(self) -> List[Dict]:
        """Получить документы из WMS"""
        session = await self._get_session()
        url = f"{WMS_API_URL}?action=IncomeApi.getObjectsShipment"
        headers = {"Authorization": self.auth_header}
        
        try:
            logger.debug("Отправка запроса к WMS")
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                logger.debug(f"Получен ответ от WMS: {data}")
                return data.get("data", [])
                
        except aiohttp.ClientConnectionError as e:
            logger.error(f"Ошибка соединения с WMS: {e}")
            return []
        except aiohttp.ClientResponseError as e:
            logger.error(f"Ошибка ответа WMS (код {e.status}): {e}")
            return []
        except Exception as e:
            logger.error(f"Неизвестная ошибка при запросе к WMS: {e}")
            return []
    
    async def get_tasks(self, external_id: str) -> List[Dict]:
        """Получить задачи для документа"""
        session = await self._get_session()
        url = f"{WMS_API_URL}?action=IncomeApi.taskExecutesDoc&p={external_id}"
        headers = {"Authorization": self.auth_header}
        
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("data", [])
                
        except Exception as e:
            logger.error(f"Ошибка получения задач для документа {external_id}: {e}")
            return []
    
    async def update_task_status(self, task_id: str, status: str = "completed") -> bool:
        """Обновить статус задачи в WMS"""
        session = await self._get_session()
        url = f"{WMS_API_URL}?action=IncomeApi.insertUpdate"
        headers = {"Authorization": self.auth_header, "Content-Type": "application/json"}
        payload = {"task": [{"externalId": task_id, "status": status}]}
        
        try:
            async with session.post(url, headers=headers, json=payload) as response:
                response.raise_for_status()
                logger.debug(f"Статус задачи {task_id} обновлен на {status}")
                return True
        except Exception as e:
            logger.error(f"Ошибка обновления статуса задачи {task_id}: {e}")
            return False
    
    async def poll_wms(self, shuttle_client=None):
        """Основной цикл опроса WMS"""
        logger.info("Запуск цикла опроса WMS")
        
        while True:
            try:
                logger.debug("Начало цикла опроса WMS")
                documents = await self.get_documents()
                
                if not documents:
                    logger.debug("Документы не найдены")
                else:
                    for doc in documents:
                        external_id = doc.get("externalId")
                        status = doc.get("idStatus")
                        
                        logger.debug(f"Обработка документа: {external_id} (статус: {status})")
                        
                        if status in ["selectionCreated", "selectionWork"]:
                            tasks = await self.get_tasks(external_id)
                            if not tasks:
                                logger.debug("Задачи не найдены")
                            else:
                                # Передаем задачи в shuttle_client если он доступен
                                if shuttle_client:
                                    await shuttle_client.process_tasks(tasks)
                
                await asyncio.sleep(POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"Ошибка в цикле опроса WMS: {e}")
                await asyncio.sleep(POLL_INTERVAL)
    
    async def close(self):
        """Закрыть сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
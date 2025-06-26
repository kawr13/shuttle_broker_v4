#!/usr/bin/env python3
"""
Улучшенная интеграция с WMS для автоматического создания команд шаттлам
"""
import asyncio
import aiohttp
import base64
import logging
import json
import yaml
from datetime import datetime
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from aiohttp import ClientSession, ClientTimeout

# Импорты из существующей системы
from core.config import get_config
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommand, ShuttleCommandEnum
from shuttle_module.shuttle_manager import get_shuttle_manager

logger = get_logger()

@dataclass
class DocumentState:
    """Состояние документа в системе"""
    external_id: str
    status: str
    last_check: datetime
    tasks_created: Set[str]
    
    def to_dict(self):
        return {
            'external_id': self.external_id,
            'status': self.status,
            'last_check': self.last_check.isoformat(),
            'tasks_created': list(self.tasks_created)
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            external_id=data['external_id'],
            status=data['status'],
            last_check=datetime.fromisoformat(data['last_check']),
            tasks_created=set(data['tasks_created'])
        )

class WMSIntegration:
    """Интеграция с WMS для автоматического управления шаттлами"""
    
    def __init__(self):
        self.config = get_config()
        self.wms_config = self.config.wms
        if not self.wms_config:
            raise ValueError("WMS конфигурация не найдена в config.yaml")
        
        self.auth_header = f"Basic {base64.b64encode(f'{self.wms_config.username}:{self.wms_config.password}'.encode()).decode()}"
        self.documents_state: Dict[str, DocumentState] = {}
        self.state_file = 'wms_documents_state.json'
        self.running = False
        
        # Загружаем сохраненное состояние
        self._load_state()
    
    def _load_state(self):
        """Загружает сохраненное состояние документов"""
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for doc_id, doc_data in data.items():
                    self.documents_state[doc_id] = DocumentState.from_dict(doc_data)
            logger.info(f"Загружено состояние {len(self.documents_state)} документов")
        except FileNotFoundError:
            logger.info("Файл состояния не найден, начинаем с пустого состояния")
        except Exception as e:
            logger.error(f"Ошибка при загрузке состояния: {e}")
    
    def _save_state(self):
        """Сохраняет текущее состояние документов"""
        try:
            data = {doc_id: doc.to_dict() for doc_id, doc in self.documents_state.items()}
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Ошибка при сохранении состояния: {e}")
    
    async def start(self):
        """Запускает интеграцию с WMS"""
        if self.running:
            return
        
        self.running = True
        logger.info("Запуск интеграции с WMS")
        
        # Запускаем основной цикл опроса
        await self._polling_loop()
    
    async def stop(self):
        """Останавливает интеграцию с WMS"""
        self.running = False
        self._save_state()
        logger.info("Интеграция с WMS остановлена")
    
    async def _polling_loop(self):
        """Основной цикл опроса WMS"""
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
            while self.running:
                try:
                    logger.debug("Начало цикла опроса WMS")
                    
                    # Получаем документы из WMS
                    documents = await self._get_documents_from_wms(session)
                    
                    if documents:
                        logger.info(f"Получено {len(documents)} документов из WMS")
                        await self._process_documents(session, documents)
                    else:
                        logger.debug("Документы не найдены")
                    
                    # Сохраняем состояние
                    self._save_state()
                    
                    # Ждем до следующего опроса
                    await asyncio.sleep(self.wms_config.poll_interval)
                    
                except Exception as e:
                    logger.error(f"Ошибка в цикле опроса WMS: {e}")
                    await asyncio.sleep(60)  # Ждем минуту перед повторной попыткой
    
    async def _get_documents_from_wms(self, session: ClientSession) -> List[dict]:
        """Получает документы из WMS"""
        try:
            url = f"{self.wms_config.api_url}?action=IncomeApi.getObjectsShipment"
            headers = {"Authorization": self.auth_header}
            
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get('getObjectsShipment', [])
                
        except Exception as e:
            logger.error(f"Ошибка при получении документов из WMS: {e}")
            return []
    
    async def _get_tasks_for_document(self, session: ClientSession, external_id: str) -> List[dict]:
        """Получает задачи для конкретного документа"""
        try:
            url = f"{self.wms_config.api_url}?action=IncomeApi.taskExecutesDoc&p={external_id}"
            headers = {"Authorization": self.auth_header}
            
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data if isinstance(data, list) else []
                
        except Exception as e:
            logger.error(f"Ошибка при получении задач для документа {external_id}: {e}")
            return []
    
    async def _process_documents(self, session: ClientSession, documents: List[dict]):
        """Обрабатывает список документов"""
        for doc in documents:
            try:
                await self._process_single_document(session, doc)
            except Exception as e:
                logger.error(f"Ошибка при обработке документа {doc.get('externalId', 'unknown')}: {e}")
    
    async def _process_single_document(self, session: ClientSession, doc: dict):
        """Обрабатывает один документ"""
        external_id = doc.get('externalId')
        if not external_id:
            logger.warning("Документ без externalId, пропускаем")
            return
        
        current_status = doc.get('idStatus', 'unknown')
        
        # Проверяем, нужно ли обрабатывать этот документ
        if current_status not in ["selectionCreated", "selectionWork"]:
            logger.debug(f"Документ {external_id} имеет статус {current_status}, пропускаем")
            return
        
        # Обновляем или создаем состояние документа
        if external_id not in self.documents_state:
            self.documents_state[external_id] = DocumentState(
                external_id=external_id,
                status=current_status,
                last_check=datetime.now(),
                tasks_created=set()
            )
            logger.info(f"Новый документ добавлен в отслеживание: {external_id}")
        else:
            self.documents_state[external_id].status = current_status
            self.documents_state[external_id].last_check = datetime.now()
        
        # Получаем задачи для документа
        tasks = await self._get_tasks_for_document(session, external_id)
        
        if tasks:
            logger.info(f"Найдено {len(tasks)} задач для документа {external_id}")
            await self._process_tasks(external_id, tasks)
        else:
            logger.debug(f"Задачи для документа {external_id} не найдены")
    
    async def _process_tasks(self, doc_id: str, tasks: List[dict]):
        """Обрабатывает задачи документа"""
        doc_state = self.documents_state[doc_id]
        
        for task in tasks:
            task_id = task.get('externalId')
            if not task_id:
                continue
            
            # Проверяем, не создавали ли мы уже команду для этой задачи
            if task_id in doc_state.tasks_created:
                logger.debug(f"Команда для задачи {task_id} уже создана")
                continue
            
            # Создаем команду для шаттла
            success = await self._create_shuttle_command(doc_id, task)
            
            if success:
                doc_state.tasks_created.add(task_id)
                logger.info(f"Создана команда для задачи {task_id} документа {doc_id}")
    
    async def _create_shuttle_command(self, doc_id: str, task: dict) -> bool:
        """Создает команду для шаттла на основе задачи"""
        try:
            # Определяем тип команды
            command_type = self._determine_command_type(task)
            
            # Определяем склад и находим свободный шаттл
            stock_name = task.get('stockName', 'Главный')
            
            shuttle_manager = get_shuttle_manager()
            shuttle_id = await shuttle_manager.get_free_shuttle(
                stock_name=stock_name,
                cell_id=task.get('cellId'),
                command=command_type.value,
                external_id=task.get('externalId')
            )
            
            if not shuttle_id:
                logger.warning(f"Не найден свободный шаттл для задачи {task.get('externalId')}")
                return False
            
            # Создаем команду
            command = ShuttleCommand(
                command_type=command_type,
                shuttle_id=shuttle_id,
                external_id=task.get('externalId'),
                document_type=doc_id,
                cell_id=task.get('cellId'),
                stock_name=stock_name,
                priority=self._get_task_priority(task)
            )
            
            # Отправляем команду
            command_id = await shuttle_manager.send_command(command)
            logger.info(f"Отправлена команда {command_type.value} шаттлу {shuttle_id} для задачи {task.get('externalId')}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при создании команды для задачи {task.get('externalId')}: {e}")
            return False
    
    def _determine_command_type(self, task: dict) -> ShuttleCommandEnum:
        """Определяет тип команды на основе задачи"""
        task_type = task.get('type', '').upper()
        operation = task.get('operation', '').upper()
        
        # Логика определения команды
        if 'IN' in task_type or 'RECEIVE' in operation:
            return ShuttleCommandEnum.PALLET_IN
        elif 'OUT' in task_type or 'SHIP' in operation:
            return ShuttleCommandEnum.PALLET_OUT
        elif 'MOVE' in task_type:
            return ShuttleCommandEnum.MOVE
        else:
            # По умолчанию запрашиваем статус
            return ShuttleCommandEnum.STATUS
    
    def _get_task_priority(self, task: dict) -> int:
        """Определяет приоритет задачи"""
        priority_map = {
            'urgent': 1,
            'high': 3,
            'normal': 5,
            'low': 7
        }
        
        task_priority = task.get('priority', 'normal').lower()
        return priority_map.get(task_priority, 5)
    
    def get_documents_status(self) -> Dict[str, dict]:
        """Возвращает статус всех отслеживаемых документов"""
        return {
            doc_id: {
                'status': doc.status,
                'last_check': doc.last_check.isoformat(),
                'tasks_created': len(doc.tasks_created)
            }
            for doc_id, doc in self.documents_state.items()
        }

# Глобальный экземпляр интеграции
wms_integration = None

def get_wms_integration() -> WMSIntegration:
    """Возвращает глобальный экземпляр интеграции с WMS"""
    global wms_integration
    if wms_integration is None:
        wms_integration = WMSIntegration()
    return wms_integration

async def main():
    """Основная функция для запуска интеграции"""
    integration = get_wms_integration()
    
    try:
        await integration.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    finally:
        await integration.stop()

if __name__ == "__main__":
    asyncio.run(main())
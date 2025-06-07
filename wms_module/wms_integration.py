import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from core.config import get_config
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommand
from shuttle_module.shuttle_manager import get_shuttle_manager
from utils.retry import retry_async
from wms_module.wms_client import WmsClient

logger = get_logger()


class WmsIntegration:
    """Интеграция с WMS API"""
    
    def __init__(self):
        self.client = WmsClient()
        self.running = False
        self.task = None
    
    async def start(self):
        """Запускает интеграцию с WMS"""
        if self.running:
            return
        
        self.running = True
        self.task = asyncio.create_task(self._poll_loop())
        logger.info("Интеграция с WMS запущена")
    
    async def stop(self):
        """Останавливает интеграцию с WMS"""
        if not self.running:
            return
        
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Интеграция с WMS остановлена")
    
    async def _poll_loop(self):
        """Основной цикл опроса WMS"""
        logger.info("Запущен цикл опроса WMS API")
        
        while self.running:
            try:
                logger.info(f"Опрашиваем WMS API ({self.client.api_url})...")
                
                # Получаем новые команды
                await self._fetch_and_process_commands()
                
                # Обновляем статусы выполненных команд
                await self._update_command_statuses()
                
                # Обновляем время последнего опроса
                self.client.last_poll_time = datetime.now()
                
                logger.info(f"Опрос WMS API завершен, следующий опрос через {self.client.poll_interval} секунд")
                
                # Ждем до следующего опроса
                await asyncio.sleep(self.client.poll_interval)
            except asyncio.CancelledError:
                logger.info("Цикл опроса WMS API отменен")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле опроса WMS: {e}")
                await asyncio.sleep(10)  # Короткая пауза перед повторной попыткой
    
    async def _fetch_and_process_commands(self):
        """Получает и обрабатывает команды из WMS"""
        try:
            # Получаем команды из разных источников
            logger.info("Получаем команды из отгрузок...")
            shipment_commands = await retry_async(
                self.client.get_shipment_commands,
                max_retries=3,
                endpoint="get_shipment_commands"
            )
            logger.info(f"Получено {len(shipment_commands)} команд из отгрузок")
            
            logger.info("Получаем команды из перемещений...")
            transfer_commands = await retry_async(
                self.client.get_transfer_commands,
                max_retries=3,
                endpoint="get_transfer_commands"
            )
            logger.info(f"Получено {len(transfer_commands)} команд из перемещений")
            
            # Обрабатываем полученные команды
            if shipment_commands:
                logger.info(f"Обрабатываем {len(shipment_commands)} команд из отгрузок")
                await self._process_commands(shipment_commands, "shipment")
            
            if transfer_commands:
                logger.info(f"Обрабатываем {len(transfer_commands)} команд из перемещений")
                await self._process_commands(transfer_commands, "transfer")
            
        except Exception as e:
            logger.error(f"Ошибка при получении команд из WMS: {e}")
    
    async def _process_commands(self, commands: List[Dict], command_type: str):
        """Обрабатывает полученные команды"""
        shuttle_manager = get_shuttle_manager()
        
        for command in commands:
            external_id = command.get("externalId")
            
            # Проверяем, не обрабатывали ли мы уже эту команду
            if external_id in self.client.processed_commands:
                continue
            
            # Получаем детали команды с использованием механизма повторных попыток
            try:
                command_details = await retry_async(
                    self.client.get_command_details,
                    external_id, 
                    command_type,
                    max_retries=3,
                    endpoint=f"get_command_details_{command_type}"
                )
                if not command_details:
                    continue
            except Exception as e:
                logger.error(f"Не удалось получить детали команды {external_id}: {e}")
                continue
            
            # Извлекаем строки команды
            lines = []
            if command_type == "shipment":
                lines = command_details.get("shipmentLine", [])
            elif command_type == "transfer":
                lines = command_details.get("transferLine", [])
            
            # Обрабатываем каждую строку как отдельную команду для шаттла
            for line in lines:
                line_external_id = line.get("externalId")
                shuttle_command_str = line.get("shuttleCommand")
                
                if not shuttle_command_str or shuttle_command_str not in self.client.command_mapping:
                    continue
                
                # Определяем склад и ячейку
                stock_name = command_details.get("warehouse", "")
                cell_id = line.get("cell", "")
                
                # Определяем параметры команды
                params = line.get("params", "")
                
                # Определяем команду шаттла
                shuttle_cmd_type = self.client.command_mapping[shuttle_command_str]
                
                # Находим свободный шаттл
                shuttle_id = await shuttle_manager.get_free_shuttle(
                    stock_name=stock_name,
                    cell_id=cell_id,
                    command=shuttle_cmd_type.value,
                    external_id=line_external_id
                )
                
                if shuttle_id:
                    # Создаем команду
                    shuttle_cmd = ShuttleCommand(
                        command_type=shuttle_cmd_type,
                        shuttle_id=shuttle_id,
                        params=params,
                        external_id=line_external_id,
                        document_type=command_type,
                        cell_id=cell_id,
                        stock_name=stock_name
                    )
                    
                    # Отправляем команду
                    try:
                        command_id = await shuttle_manager.send_command(shuttle_cmd)
                        logger.info(f"Команда {shuttle_cmd_type.value} для шаттла {shuttle_id} добавлена в очередь (WMS externalId: {line_external_id})")
                        
                        # Добавляем в список обработанных
                        self.client.processed_commands.add(line_external_id)
                    except Exception as e:
                        logger.error(f"Не удалось добавить команду {shuttle_cmd_type.value} в очередь для шаттла {shuttle_id}: {e}")
                else:
                    logger.warning(f"Не найден свободный шаттл для команды {shuttle_cmd_type.value} на складе {stock_name}, ячейка {cell_id}")
    
    async def _update_command_statuses(self):
        """Обновляет статусы выполненных команд в WMS"""
        from ..shuttle_module.shuttle_manager import get_shuttle_manager
        shuttle_manager = get_shuttle_manager()
        
        # Получаем состояния всех шаттлов
        shuttle_states = await shuttle_manager.get_all_shuttle_states()
        
        # Обрабатываем каждый шаттл
        for shuttle_id, state in shuttle_states.items():
            # Пропускаем шаттлы без external_id или document_type
            if not state.external_id or not state.document_type:
                continue
            
            # Пропускаем шаттлы, которые не завершили выполнение команды
            if state.status not in ["FREE", "ERROR"]:
                continue
            
            # Определяем статус для WMS
            wms_status = "done"
            if state.status == "ERROR":
                wms_status = "error"
            
            # Обновляем статус в WMS
            try:
                success = await retry_async(
                    self.client.update_status,
                    state.external_id,
                    state.document_type,
                    wms_status,
                    max_retries=3,
                    endpoint=f"update_status_{state.document_type}"
                )
                
                if success:
                    logger.info(f"Статус команды для шаттла {shuttle_id} обновлен в WMS (externalId: {state.external_id}, тип: {state.document_type})")
                    
                    # Очищаем external_id и document_type, чтобы не обновлять статус повторно
                    state.external_id = None
                    state.document_type = None
                else:
                    logger.error(f"Не удалось обновить статус команды для шаттла {shuttle_id} в WMS (externalId: {state.external_id}, тип: {state.document_type})")
            except Exception as e:
                logger.error(f"Ошибка при обновлении статуса команды для шаттла {shuttle_id} в WMS: {e}")


# Глобальный экземпляр интеграции с WMS
wms_integration = None


def get_wms_integration() -> WmsIntegration:
    """Возвращает глобальный экземпляр интеграции с WMS"""
    global wms_integration
    if wms_integration is None:
        wms_integration = WmsIntegration()
    return wms_integration
"""
Патч для ShuttleManager, добавляющий поддержку автоматически обнаруженных шаттлов
в метод get_free_shuttle
"""
from typing import Optional

from core.config import get_config
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommand, ShuttleStatus

logger = get_logger()

async def get_free_shuttle(self, stock_name: str, cell_id: Optional[str] = None, 
                          command: Optional[str] = None, external_id: Optional[str] = None) -> Optional[str]:
    """
    Находит свободный шаттл для выполнения команды
    
    Args:
        stock_name: Название склада
        cell_id: ID ячейки (опционально)
        command: Тип команды (опционально)
        external_id: Внешний ID команды (опционально)
    
    Returns:
        ID шаттла или None, если свободный шаттл не найден
    """
    config = get_config()
    
    # Если указан external_id и команда HOME, ищем шаттл по external_id
    if command == ShuttleCommand.HOME.value and external_id:
        for shuttle_id, shuttle in self.shuttles.items():
            state = shuttle.get_state()
            if state.external_id == external_id:
                return shuttle_id
        logger.error(f"Шаттл с external_id {external_id} не найден")
        return None
    
    # Получаем шаттлы для указанного склада
    shuttles = config.stock_to_shuttle.get(stock_name, [])
    
    # Если указана ячейка, фильтруем шаттлы по ячейке
    if cell_id:
        # Здесь можно добавить логику фильтрации по ячейке,
        # если в будущем будет реализовано хранение информации о ячейках
        pass
    
    # Проверяем, является ли команда высокоприоритетной
    high_priority = command in [
        ShuttleCommand.HOME.value,
        ShuttleCommand.STATUS.value,
        ShuttleCommand.MRCD.value
    ]
    
    # Ищем свободный шаттл в конфигурации
    for shuttle_id in shuttles:
        if shuttle_id not in self.shuttles:
            continue
        
        state = self.shuttles[shuttle_id].get_state()
        
        # Для высокоприоритетных команд возвращаем любой шаттл
        if high_priority:
            return shuttle_id
        
        # Для обычных команд возвращаем только свободный шаттл
        if state.status == ShuttleStatus.FREE:
            return shuttle_id
    
    # Если не нашли в конфигурации, ищем среди автоматически обнаруженных шаттлов
    for shuttle_id, shuttle in self.shuttles.items():
        # Пропускаем шаттлы, которые уже проверили выше
        if shuttle_id in shuttles:
            continue
            
        # Проверяем только шаттлы с именами вида shuttle_XXX
        if not shuttle_id.startswith("shuttle_"):
            continue
            
        state = shuttle.get_state()
        
        # Для высокоприоритетных команд возвращаем любой шаттл
        if high_priority:
            logger.info(f"Выбран автоматически обнаруженный шаттл {shuttle_id} для высокоприоритетной команды")
            return shuttle_id
        
        # Для обычных команд возвращаем только свободный шаттл
        if state.status == ShuttleStatus.FREE:
            logger.info(f"Выбран автоматически обнаруженный шаттл {shuttle_id} для команды")
            return shuttle_id
    
    return None
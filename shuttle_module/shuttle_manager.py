import asyncio
import time
from typing import Dict, List, Optional, Any, Callable

from core.config import get_config
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommand, ShuttleStatus
from shuttle_module.shuttle_client import ShuttleClient
from shuttle_module.shuttle_state import ShuttleState

logger = get_logger()


class ShuttleManager:
    """Менеджер для управления шаттлами"""
    
    def __init__(self):
        self.shuttles: Dict[str, ShuttleClient] = {}
        self.command_queues: Dict[str, asyncio.PriorityQueue] = {}
        self.command_locks: Dict[str, asyncio.Lock] = {}
        self.command_registry: Dict[str, Dict[str, Any]] = {}
        self.worker_tasks: List[asyncio.Task] = []
        self.running = False
    
    async def start(self):
        """Запускает менеджер шаттлов"""
        if self.running:
            return
        
        self.running = True
        
        # Загружаем конфигурацию
        config = get_config()
        
        # Инициализируем шаттлы
        for shuttle_id, shuttle_config in config.shuttles.items():
            self.shuttles[shuttle_id] = ShuttleClient(shuttle_id, shuttle_config)
            self.command_queues[shuttle_id] = asyncio.PriorityQueue(maxsize=config.command_queue_max_size)
            self.command_locks[shuttle_id] = asyncio.Lock()
        
        # Запускаем воркеры для обработки команд
        for i in range(config.command_processor_workers):
            task = asyncio.create_task(self._command_processor_worker(i + 1))
            self.worker_tasks.append(task)
        
        logger.info(f"Менеджер шаттлов запущен ({len(self.shuttles)} шаттлов, {len(self.worker_tasks)} воркеров)")
    
    async def stop(self):
        """Останавливает менеджер шаттлов"""
        if not self.running:
            return
        
        self.running = False
        
        # Отменяем воркеры
        for task in self.worker_tasks:
            task.cancel()
        
        # Ждем завершения воркеров
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        
        # Закрываем соединения с шаттлами
        for shuttle in self.shuttles.values():
            await shuttle.disconnect()
        
        logger.info("Менеджер шаттлов остановлен")
    
    async def send_command(self, command: ShuttleCommand) -> str:
        """
        Отправляет команду шаттлу или добавляет ее в очередь
        
        Returns:
            ID команды для возможности отмены
        """
        shuttle_id = command.shuttle_id
        
        # Проверяем, существует ли шаттл
        if shuttle_id not in self.shuttles:
            raise ValueError(f"Шаттл {shuttle_id} не найден")
        
        # Генерируем ID команды
        command_id = f"{shuttle_id}_{command.command_type.value}_{int(time.time()*1000)}"
        
        # Команда HOME всегда обрабатывается немедленно и прерывает текущие операции
        if command.command_type.value == "HOME":
            logger.info(f"Получена команда HOME для шаттла {shuttle_id}, обрабатываем немедленно")
            async with self.command_locks[shuttle_id]:
                # Отправляем команду напрямую
                success = await self._process_command(command)
                if success:
                    logger.info(f"Команда HOME для шаттла {shuttle_id} обработана немедленно")
                    
                    # Регистрируем команду
                    self.command_registry[command_id] = {
                        "command": command,
                        "status": "completed",
                        "timestamp": time.time(),
                        "completed_at": time.time()
                    }
                else:
                    logger.error(f"Не удалось обработать команду HOME для шаттла {shuttle_id}")
                    
                    # Регистрируем команду
                    self.command_registry[command_id] = {
                        "command": command,
                        "status": "failed",
                        "timestamp": time.time(),
                        "error": "Failed to process command"
                    }
                
                return command_id
        
        # Другие команды с высоким приоритетом обрабатываются немедленно
        if command.priority <= 4:  # STATUS, BATTERY, MRCD
            async with self.command_locks[shuttle_id]:
                success = await self._process_command(command)
                if success:
                    logger.info(f"Команда {command.command_type.value} для шаттла {shuttle_id} обработана немедленно")
                    
                    # Регистрируем команду
                    self.command_registry[command_id] = {
                        "command": command,
                        "status": "completed",
                        "timestamp": time.time(),
                        "completed_at": time.time()
                    }
                else:
                    logger.error(f"Не удалось обработать команду {command.command_type.value} для шаттла {shuttle_id}")
                    
                    # Регистрируем команду
                    self.command_registry[command_id] = {
                        "command": command,
                        "status": "failed",
                        "timestamp": time.time(),
                        "error": "Failed to process command"
                    }
                
                return command_id
        
        # Обычные команды добавляются в очередь
        try:
            await self.command_queues[shuttle_id].put((command.priority, command_id, command))
            
            # Регистрируем команду
            self.command_registry[command_id] = {
                "command": command,
                "status": "queued",
                "timestamp": time.time()
            }
            
            logger.info(f"Команда {command.command_type.value} для шаттла {shuttle_id} добавлена в очередь с приоритетом {command.priority}")
            return command_id
        except asyncio.QueueFull:
            logger.error(f"Очередь команд для шаттла {shuttle_id} заполнена")
            
            # Регистрируем команду
            self.command_registry[command_id] = {
                "command": command,
                "status": "failed",
                "timestamp": time.time(),
                "error": "Queue full"
            }
            
            raise RuntimeError(f"Очередь команд для шаттла {shuttle_id} заполнена")
    
    async def cancel_command(self, command_id: str) -> bool:
        """
        Отменяет команду, если она еще не выполнена
        
        Returns:
            True если команда была отменена, False в противном случае
        """
        # Проверяем, существует ли команда
        if command_id not in self.command_registry:
            logger.warning(f"Команда {command_id} не найдена для отмены")
            return False
        
        # Получаем информацию о команде
        command_info = self.command_registry[command_id]
        
        # Проверяем, можно ли отменить команду
        if command_info["status"] != "queued":
            logger.warning(f"Команда {command_id} не может быть отменена, т.к. имеет статус {command_info['status']}")
            return False
        
        # Получаем шаттл и команду
        command = command_info["command"]
        shuttle_id = command.shuttle_id
        
        # Блокируем доступ к очереди
        async with self.command_locks[shuttle_id]:
            # Создаем временную очередь
            temp_queue = asyncio.PriorityQueue()
            
            # Перемещаем все команды, кроме отменяемой, во временную очередь
            while not self.command_queues[shuttle_id].empty():
                try:
                    priority, cmd_id, cmd = await self.command_queues[shuttle_id].get()
                    if cmd_id != command_id:
                        await temp_queue.put((priority, cmd_id, cmd))
                except asyncio.QueueEmpty:
                    break
            
            # Возвращаем команды обратно в очередь
            while not temp_queue.empty():
                try:
                    item = await temp_queue.get()
                    await self.command_queues[shuttle_id].put(item)
                except asyncio.QueueEmpty:
                    break
        
        # Обновляем статус команды
        self.command_registry[command_id]["status"] = "cancelled"
        self.command_registry[command_id]["cancelled_at"] = time.time()
        
        logger.info(f"Команда {command_id} для шаттла {shuttle_id} отменена")
        return True
    
    async def get_shuttle_state(self, shuttle_id: str) -> Optional[ShuttleState]:
        """Возвращает текущее состояние шаттла"""
        if shuttle_id not in self.shuttles:
            return None
        
        return self.shuttles[shuttle_id].get_state()
    
    async def get_all_shuttle_states(self) -> Dict[str, ShuttleState]:
        """Возвращает состояния всех шаттлов"""
        return {shuttle_id: shuttle.get_state() for shuttle_id, shuttle in self.shuttles.items()}
    
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
        
        # Ищем свободный шаттл
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
        
        return None
    
    async def _command_processor_worker(self, worker_id: int):
        """Воркер для обработки команд из очереди"""
        logger.info(f"Воркер обработки команд {worker_id} запущен")
        
        while self.running:
            try:
                # Проверяем все шаттлы
                for shuttle_id in self.shuttles:
                    # Пропускаем шаттлы с заблокированными очередями
                    if self.command_locks[shuttle_id].locked():
                        continue
                    
                    # Получаем состояние шаттла
                    state = self.shuttles[shuttle_id].get_state()
                    
                    # Пропускаем занятые шаттлы
                    if state.status != ShuttleStatus.FREE:
                        continue
                    
                    # Пытаемся получить команду из очереди
                    try:
                        priority, command_id, command = self.command_queues[shuttle_id].get_nowait()
                        
                        # Проверяем, не была ли команда отменена
                        if command_id in self.command_registry and self.command_registry[command_id]["status"] == "cancelled":
                            logger.info(f"Пропуск отмененной команды {command_id}")
                            self.command_queues[shuttle_id].task_done()
                            continue
                        
                        # Обновляем статус команды
                        if command_id in self.command_registry:
                            self.command_registry[command_id]["status"] = "processing"
                        
                        # Обрабатываем команду
                        async with self.command_locks[shuttle_id]:
                            success = await self._process_command(command)
                            
                            # Обновляем статус команды
                            if command_id in self.command_registry:
                                self.command_registry[command_id]["status"] = "completed" if success else "failed"
                                self.command_registry[command_id]["completed_at"] = time.time()
                        
                        # Помечаем задачу как выполненную
                        self.command_queues[shuttle_id].task_done()
                    except asyncio.QueueEmpty:
                        pass
                
                # Ждем перед следующей итерацией
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                logger.info(f"Воркер обработки команд {worker_id} остановлен")
                break
            except Exception as e:
                logger.error(f"Ошибка в воркере обработки команд {worker_id}: {e}")
                await asyncio.sleep(1)  # Ждем перед повторной попыткой
    
    async def _process_command(self, command: ShuttleCommand) -> bool:
        """Обрабатывает команду"""
        shuttle_id = command.shuttle_id
        
        # Проверяем, существует ли шаттл
        if shuttle_id not in self.shuttles:
            logger.error(f"Шаттл {shuttle_id} не найден")
            return False
        
        # Получаем клиент шаттла
        shuttle = self.shuttles[shuttle_id]
        
        # Отправляем команду шаттлу
        success = await shuttle.send_command(command)
        
        if success:
            # Обновляем состояние шаттла
            state = shuttle.get_state()
            state.external_id = command.external_id
            state.document_type = command.document_type
            state.cell_id = command.cell_id
            state.stock_name = command.stock_name
            
            logger.info(f"Команда {command.command_type.value} успешно отправлена шаттлу {shuttle_id}")
        else:
            logger.error(f"Не удалось отправить команду {command.command_type.value} шаттлу {shuttle_id}")
        
        return success


# Глобальный экземпляр менеджера шаттлов
shuttle_manager = None


def get_shuttle_manager() -> ShuttleManager:
    """Возвращает глобальный экземпляр менеджера шаттлов"""
    global shuttle_manager
    if shuttle_manager is None:
        shuttle_manager = ShuttleManager()
    return shuttle_manager
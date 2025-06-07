import asyncio
import json
import os
import time
from typing import Dict, Any, Optional, List

from core.logging import get_logger

logger = get_logger()


class DeadLetterQueue:
    """
    Реализация Dead Letter Queue для хранения неудачных команд
    с возможностью их повторной обработки.
    """
    
    def __init__(self, queue_dir: str = "dead_letter_queue"):
        """
        Инициализирует Dead Letter Queue.
        
        Args:
            queue_dir: Директория для хранения неудачных команд
        """
        self.queue_dir = queue_dir
        os.makedirs(queue_dir, exist_ok=True)
    
    async def add_failed_command(self, command: Dict[str, Any], error: str) -> str:
        """
        Добавляет неудачную команду в очередь.
        
        Args:
            command: Команда в виде словаря
            error: Описание ошибки
        
        Returns:
            ID команды в очереди
        """
        # Создаем уникальный ID для команды
        command_id = f"{int(time.time())}_{command.get('shuttle_id', 'unknown')}_{command.get('command_type', 'unknown')}"
        
        # Добавляем информацию об ошибке
        command_data = {
            "command": command,
            "error": error,
            "timestamp": time.time(),
            "attempts": 0
        }
        
        # Сохраняем команду в файл
        file_path = os.path.join(self.queue_dir, f"{command_id}.json")
        with open(file_path, "w") as f:
            json.dump(command_data, f, indent=2)
        
        logger.info(f"Команда {command_id} добавлена в Dead Letter Queue")
        return command_id
    
    async def get_failed_commands(self) -> List[Dict[str, Any]]:
        """
        Возвращает список всех неудачных команд.
        
        Returns:
            Список неудачных команд
        """
        commands = []
        
        # Получаем список файлов в директории
        for file_name in os.listdir(self.queue_dir):
            if not file_name.endswith(".json"):
                continue
            
            file_path = os.path.join(self.queue_dir, file_name)
            try:
                with open(file_path, "r") as f:
                    command_data = json.load(f)
                
                # Добавляем ID команды
                command_data["id"] = file_name.replace(".json", "")
                commands.append(command_data)
            except Exception as e:
                logger.error(f"Ошибка при чтении команды из Dead Letter Queue: {e}")
        
        return commands
    
    async def retry_command(self, command_id: str) -> Optional[Dict[str, Any]]:
        """
        Возвращает команду для повторной обработки и увеличивает счетчик попыток.
        
        Args:
            command_id: ID команды
        
        Returns:
            Команда для повторной обработки или None, если команда не найдена
        """
        file_path = os.path.join(self.queue_dir, f"{command_id}.json")
        if not os.path.exists(file_path):
            logger.warning(f"Команда {command_id} не найдена в Dead Letter Queue")
            return None
        
        try:
            # Читаем команду из файла
            with open(file_path, "r") as f:
                command_data = json.load(f)
            
            # Увеличиваем счетчик попыток
            command_data["attempts"] += 1
            
            # Сохраняем обновленную команду
            with open(file_path, "w") as f:
                json.dump(command_data, f, indent=2)
            
            logger.info(f"Команда {command_id} извлечена из Dead Letter Queue для повторной обработки (попытка {command_data['attempts']})")
            return command_data["command"]
        except Exception as e:
            logger.error(f"Ошибка при извлечении команды {command_id} из Dead Letter Queue: {e}")
            return None
    
    async def remove_command(self, command_id: str) -> bool:
        """
        Удаляет команду из очереди.
        
        Args:
            command_id: ID команды
        
        Returns:
            True, если команда успешно удалена, иначе False
        """
        file_path = os.path.join(self.queue_dir, f"{command_id}.json")
        if not os.path.exists(file_path):
            logger.warning(f"Команда {command_id} не найдена в Dead Letter Queue")
            return False
        
        try:
            os.remove(file_path)
            logger.info(f"Команда {command_id} удалена из Dead Letter Queue")
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении команды {command_id} из Dead Letter Queue: {e}")
            return False


# Глобальный экземпляр Dead Letter Queue
dlq = None


def get_dead_letter_queue() -> DeadLetterQueue:
    """Возвращает глобальный экземпляр Dead Letter Queue"""
    global dlq
    if dlq is None:
        dlq = DeadLetterQueue()
    return dlq
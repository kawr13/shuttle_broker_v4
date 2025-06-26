import asyncio
import json
from typing import Dict, Optional
from pathlib import Path

from core.config import get_config
from core.logging import get_logger

logger = get_logger()


class ShuttleDiscovery:
    """Класс для автоматического обнаружения и регистрации шаттлов"""
    
    def __init__(self):
        self.next_shuttle_number = 1
        self._load_next_shuttle_number()
    
    def _load_next_shuttle_number(self):
        """Загружает следующий номер шаттла из файла"""
        try:
            config_path = Path("config.yaml").parent / "shuttle_counter.json"
            if config_path.exists():
                with open(config_path, 'r') as f:
                    data = json.load(f)
                    self.next_shuttle_number = data.get('next_number', 1)
        except Exception as e:
            logger.warning(f"Не удалось загрузить счетчик шаттлов: {e}")
            self.next_shuttle_number = 1
    
    def _save_next_shuttle_number(self):
        """Сохраняет следующий номер шаттла в файл"""
        try:
            config_path = Path("config.yaml").parent / "shuttle_counter.json"
            with open(config_path, 'w') as f:
                json.dump({'next_number': self.next_shuttle_number}, f)
        except Exception as e:
            logger.error(f"Не удалось сохранить счетчик шаттлов: {e}")
    
    async def register_new_shuttle(self, ip: str, command_port: int = 2000, response_port: int = 5000) -> str:
        """Регистрирует новый шаттл в конфигурации"""
        # Генерируем ID для нового шаттла
        shuttle_id = f"shuttle_{self.next_shuttle_number}"
        self.next_shuttle_number += 1
        self._save_next_shuttle_number()
        
        # Добавляем шаттл в конфигурацию
        await self._add_shuttle_to_config(shuttle_id, ip, command_port, response_port)
        
        logger.info(f"Зарегистрирован новый шаттл {shuttle_id} с IP {ip}")
        return shuttle_id
    
    async def _add_shuttle_to_config(self, shuttle_id: str, ip: str, command_port: int, response_port: int):
        """Добавляет шаттл в файл конфигурации"""
        try:
            config_path = Path("config.yaml")
            
            # Читаем текущую конфигурацию
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Добавляем новый шаттл в секцию shuttles
            shuttle_config = f"""  {shuttle_id}:
    host: "{ip}"
    command_port: {command_port}
    response_port: {response_port}
    shuttle_health_check_interval: 10
"""
            
            # Находим секцию shuttles и добавляем новый шаттл
            lines = content.split('\n')
            new_lines = []
            in_shuttles_section = False
            shuttles_section_found = False
            
            for line in lines:
                if line.strip().startswith('shuttles:'):
                    in_shuttles_section = True
                    shuttles_section_found = True
                    new_lines.append(line)
                    continue
                
                if in_shuttles_section:
                    # Если строка не начинается с пробелов, значит секция shuttles закончилась
                    if line and not line.startswith(' ') and not line.startswith('\t'):
                        # Добавляем новый шаттл перед следующей секцией
                        new_lines.append(shuttle_config.rstrip())
                        in_shuttles_section = False
                
                new_lines.append(line)
            
            # Если мы все еще в секции shuttles (она последняя), добавляем шаттл в конец
            if in_shuttles_section:
                new_lines.append(shuttle_config.rstrip())
            
            # Добавляем шаттл в секцию stock_to_shuttle
            self._add_shuttle_to_stock_section(new_lines, shuttle_id)
            
            # Записываем обновленную конфигурацию
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(new_lines))
            
            logger.info(f"Шаттл {shuttle_id} добавлен в конфигурацию")
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении шаттла в конфигурацию: {e}")
    
    def _add_shuttle_to_stock_section(self, lines: list, shuttle_id: str):
        """Добавляет шаттл в секцию stock_to_shuttle"""
        try:
            in_stock_section = False
            in_main_stock = False
            
            for i, line in enumerate(lines):
                if line.strip().startswith('stock_to_shuttle:'):
                    in_stock_section = True
                    continue
                
                if in_stock_section:
                    if line.strip().startswith('Главный:'):
                        in_main_stock = True
                        continue
                    
                    if in_main_stock:
                        # Если строка не начинается с пробелов, секция закончилась
                        if line and not line.startswith(' ') and not line.startswith('\t'):
                            # Добавляем шаттл в список
                            lines.insert(i, f"    - {shuttle_id}")
                            break
                        # Если это последний элемент списка, добавляем после него
                        elif line.strip().startswith('- '):
                            continue
                    
                    # Если вышли из секции stock_to_shuttle
                    if line and not line.startswith(' ') and not line.startswith('\t') and not line.strip().startswith('Главный:'):
                        if in_main_stock:
                            lines.insert(i, f"    - {shuttle_id}")
                        break
            
            # Если дошли до конца файла и все еще в секции
            if in_main_stock:
                lines.append(f"    - {shuttle_id}")
                
        except Exception as e:
            logger.error(f"Ошибка при добавлении шаттла в секцию stock_to_shuttle: {e}")
    
    async def update_shuttle_location(self, shuttle_id: str, location_data: str):
        """Обновляет информацию о местоположении шаттла"""
        logger.info(f"Обновление местоположения шаттла {shuttle_id}: {location_data}")
        
        # Здесь можно добавить логику для сохранения местоположения
        # Например, в Redis или в отдельный файл конфигурации
        try:
            from storage_module.redis_storage_manager import get_redis_storage_manager
            redis_manager = get_redis_storage_manager()
            
            # Сохраняем местоположение в Redis
            await redis_manager.set_shuttle_location(shuttle_id, location_data)
            logger.info(f"Местоположение шаттла {shuttle_id} сохранено в Redis")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении местоположения шаттла {shuttle_id}: {e}")


# Глобальный экземпляр
shuttle_discovery = None


def get_shuttle_discovery() -> ShuttleDiscovery:
    """Возвращает глобальный экземпляр обнаружителя шаттлов"""
    global shuttle_discovery
    if shuttle_discovery is None:
        shuttle_discovery = ShuttleDiscovery()
    return shuttle_discovery
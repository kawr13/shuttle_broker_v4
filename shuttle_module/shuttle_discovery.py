import asyncio
import socket
import time
from typing import Dict, Set, Optional, Tuple
from dataclasses import dataclass

from core.config import get_config, ShuttleConfig
from core.logging import get_logger
from shuttle_module.commands import ShuttleCommand, ShuttleCommandEnum

logger = get_logger()


@dataclass
class DiscoveredShuttle:
    """Информация об обнаруженном шаттле"""
    ip: str
    name: str
    location: Optional[str] = None
    status: Optional[str] = None
    battery_level: Optional[str] = None
    discovered_at: float = 0.0


class ShuttleDiscovery:
    """Модуль автоматического обнаружения шаттлов в сети"""
    
    def __init__(self):
        self.running = False
        self.discovered_shuttles: Dict[str, DiscoveredShuttle] = {}
        self.known_shuttles: Set[str] = set()
        self.discovery_task = None
        self.scan_interval = 30  # Интервал сканирования сети в секундах
        
    async def start(self):
        """Запускает модуль обнаружения шаттлов"""
        if self.running:
            return
            
        self.running = True
        
        # Загружаем известные шаттлы из конфигурации
        config = get_config()
        for shuttle_id in config.shuttles:
            self.known_shuttles.add(shuttle_id)
        
        # Запускаем задачу обнаружения
        self.discovery_task = asyncio.create_task(self._discovery_loop())
        logger.info("Модуль автоматического обнаружения шаттлов запущен")
        
    async def stop(self):
        """Останавливает модуль обнаружения"""
        if not self.running:
            return
            
        self.running = False
        
        if self.discovery_task:
            self.discovery_task.cancel()
            try:
                await self.discovery_task
            except asyncio.CancelledError:
                pass
                
        logger.info("Модуль автоматического обнаружения шаттлов остановлен")
        
    async def _discovery_loop(self):
        """Основной цикл обнаружения шаттлов"""
        while self.running:
            try:
                await self._scan_network()
                await asyncio.sleep(self.scan_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле обнаружения шаттлов: {e}")
                await asyncio.sleep(5)
                
    async def _scan_network(self):
        """Сканирует сеть на предмет новых шаттлов"""
        logger.debug("Начинаем сканирование сети для поиска шаттлов")
        
        # Получаем диапазон IP для сканирования
        ip_ranges = self._get_scan_ranges()
        
        # Сканируем каждый диапазон
        for ip_range in ip_ranges:
            await self._scan_ip_range(ip_range)
            
    def _get_scan_ranges(self) -> list:
        """Определяет диапазоны IP для сканирования"""
        ranges = []
        
        # Получаем локальные сети
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            
            # Определяем подсеть на основе локального IP
            ip_parts = local_ip.split('.')
            if len(ip_parts) == 4:
                base_ip = f"{ip_parts[0]}.{ip_parts[1]}.{ip_parts[2]}"
                ranges.append(base_ip)
                
        except Exception as e:
            logger.warning(f"Не удалось определить локальную сеть: {e}")
            
        # Добавляем стандартные диапазоны
        ranges.extend([
            "192.168.1",
            "192.168.0", 
            "10.181.80",  # Из примера в README
            "127.0.0"
        ])
        
        return list(set(ranges))  # Убираем дубликаты
        
    async def _scan_ip_range(self, base_ip: str):
        """Сканирует диапазон IP адресов"""
        tasks = []
        
        # Сканируем адреса от 1 до 254
        for i in range(1, 255):
            ip = f"{base_ip}.{i}"
            task = asyncio.create_task(self._check_shuttle_at_ip(ip))
            tasks.append(task)
            
        # Ждем завершения всех проверок
        await asyncio.gather(*tasks, return_exceptions=True)
        
    async def _check_shuttle_at_ip(self, ip: str):
        """Проверяет, есть ли шаттл по указанному IP"""
        try:
            # Пытаемся подключиться к стандартному порту шаттла (2000)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, 2000),
                timeout=2.0
            )
            
            # Отправляем команду STATUS для идентификации
            writer.write(b"STATUS\n")
            await writer.drain()
            
            # Ждем ответ
            try:
                data = await asyncio.wait_for(
                    reader.readuntil(b'\n'),
                    timeout=3.0
                )
                response = data.decode('utf-8').strip()
                
                # Если получили ответ, это может быть шаттл
                if response:
                    await self._process_potential_shuttle(ip, response)
                    
            except asyncio.TimeoutError:
                pass
                
            # Закрываем соединение
            writer.close()
            await writer.wait_closed()
            
        except (ConnectionRefusedError, asyncio.TimeoutError, OSError):
            # Нет шаттла по этому адресу
            pass
        except Exception as e:
            logger.debug(f"Ошибка при проверке IP {ip}: {e}")
            
    async def _process_potential_shuttle(self, ip: str, response: str):
        """Обрабатывает потенциальный шаттл"""
        logger.info(f"Обнаружен потенциальный шаттл на IP {ip}, ответ: {response}")
        
        # Генерируем простое имя для нового шаттла
        shuttle_name = self._generate_shuttle_name(ip)
        
        # Проверяем, не знаем ли мы уже об этом шаттле
        if shuttle_name in self.known_shuttles:
            return
            
        # Создаем запись об обнаруженном шаттле
        discovered = DiscoveredShuttle(
            ip=ip,
            name=shuttle_name,
            discovered_at=time.time()
        )
        
        # Пытаемся получить дополнительную информацию
        await self._gather_shuttle_info(discovered)
        
        # Сохраняем информацию
        self.discovered_shuttles[shuttle_name] = discovered
        
        # Автоматически добавляем шаттл в конфигурацию
        await self._auto_register_shuttle(discovered)
        
    async def _gather_shuttle_info(self, shuttle: DiscoveredShuttle):
        """Собирает дополнительную информацию о шаттле"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(shuttle.ip, 2000),
                timeout=3.0
            )
            
            # Запрашиваем статус
            writer.write(b"STATUS\r\n")
            await writer.drain()
            
            try:
                data = await asyncio.wait_for(reader.readuntil('\r\n'), timeout=2.0)
                response = data.decode('utf-8').strip()
                if response.startswith("STATUS="):
                    shuttle.status = response.split("=", 1)[1]
            except asyncio.TimeoutError:
                pass
                
            # Запрашиваем уровень батареи
            writer.write(b"BATTERY\n")
            await writer.drain()
            
            try:
                data = await asyncio.wait_for(reader.readuntil('\r\n'), timeout=2.0)
                response = data.decode('utf-8').strip()
                if response.startswith("BATTERY="):
                    shuttle.battery_level = response.split("=", 1)[1]
            except asyncio.TimeoutError:
                pass
                
            writer.close()
            await writer.wait_closed()
            
        except Exception as e:
            logger.debug(f"Не удалось собрать информацию о шаттле {shuttle.ip}: {e}")
            
    async def _auto_register_shuttle(self, shuttle: DiscoveredShuttle):
        """Автоматически регистрирует новый шаттл"""
        try:
            # Добавляем шаттл в менеджер
            from shuttle_module.shuttle_manager import get_shuttle_manager
            from shuttle_module.shuttle_client import ShuttleClient
            
            shuttle_manager = get_shuttle_manager()
            
            # Создаем конфигурацию для нового шаттла
            shuttle_config = ShuttleConfig(
                host=shuttle.ip,
                command_port=2000,
                response_port=5000
            )
            
            # Создаем клиент шаттла
            shuttle_client = ShuttleClient(shuttle.name, shuttle_config)
            
            # Добавляем в менеджер
            shuttle_manager.shuttles[shuttle.name] = shuttle_client
            shuttle_manager.command_queues[shuttle.name] = asyncio.PriorityQueue(
                maxsize=get_config().command_queue_max_size
            )
            shuttle_manager.command_locks[shuttle.name] = asyncio.Lock()
            
            # Регистрируем обработчик сообщений
            from shuttle_module.shuttle_listener import get_shuttle_listener
            shuttle_listener = get_shuttle_listener()
            shuttle_listener.register_message_handler(
                shuttle.name, 
                shuttle_client._process_message_from_listener
            )
            
            # Добавляем в список известных шаттлов
            self.known_shuttles.add(shuttle.name)
            
            logger.info(f"Автоматически зарегистрирован новый шаттл: {shuttle.name} ({shuttle.ip})")
            
            # Запрашиваем местоположение нового шаттла
            await self._request_shuttle_location(shuttle.name)
            
        except Exception as e:
            logger.error(f"Ошибка при автоматической регистрации шаттла {shuttle.name}: {e}")
            
    async def _request_shuttle_location(self, shuttle_name: str):
        """Запрашивает местоположение шаттла"""
        try:
            from shuttle_module.shuttle_manager import get_shuttle_manager
            
            shuttle_manager = get_shuttle_manager()
            
            # Отправляем команду STATUS для получения местоположения
            status_command = ShuttleCommand(
                command_type=ShuttleCommandEnum.STATUS,
                shuttle_id=shuttle_name
            )
            
            await shuttle_manager.send_command(status_command)
            logger.info(f"Запрошено местоположение нового шаттла {shuttle_name}")
            
        except Exception as e:
            logger.error(f"Ошибка при запросе местоположения шаттла {shuttle_name}: {e}")
            
    def get_discovered_shuttles(self) -> Dict[str, DiscoveredShuttle]:
        """Возвращает список обнаруженных шаттлов"""
        return self.discovered_shuttles.copy()
        
    def get_shuttle_count(self) -> int:
        """Возвращает количество обнаруженных шаттлов"""
        return len(self.discovered_shuttles)
        
    def _generate_shuttle_name(self, ip: str) -> str:
        """Генерирует простое имя для шаттла"""
        # Используем последний октет IP адреса
        last_octet = ip.split('.')[-1]
        base_name = f"shuttle_{last_octet}"
        
        # Проверяем, не занято ли уже такое имя
        if base_name not in self.known_shuttles and base_name not in self.discovered_shuttles:
            return base_name
            
        # Если занято, добавляем номер
        counter = 1
        while f"{base_name}_{counter}" in self.known_shuttles or f"{base_name}_{counter}" in self.discovered_shuttles:
            counter += 1
        return f"{base_name}_{counter}"


# Глобальный экземпляр модуля обнаружения
shuttle_discovery = None


def get_shuttle_discovery() -> ShuttleDiscovery:
    """Возвращает глобальный экземпляр модуля обнаружения шаттлов"""
    global shuttle_discovery
    if shuttle_discovery is None:
        shuttle_discovery = ShuttleDiscovery()
    return shuttle_discovery
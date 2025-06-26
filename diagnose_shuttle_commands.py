#!/usr/bin/env python3
"""
Скрипт для диагностики проблемы с отправкой команд шаттлам
Сравнивает отправку команд через прямой скрипт и через шлюз
"""
import asyncio
import sys
import yaml
import argparse
import logging
from typing import Optional

# Настраиваем логирование
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('shuttle_debug.log')
    ]
)
logger = logging.getLogger('shuttle_debug')

class ShuttleDebugger:
    def __init__(self, shuttle_id: str):
        self.shuttle_id = shuttle_id
        self.shuttle_ip = None
        self.command_port = 2000
        self.listener_port = 8181
        self.response_received = False
        self.response_data = None
        
    def load_shuttle_config(self) -> bool:
        """Загружает конфигурацию шаттла из config.yaml"""
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            shuttles = config_data.get('shuttles', {})
            if self.shuttle_id not in shuttles:
                logger.error(f"❌ Шаттл {self.shuttle_id} не найден в конфигурации")
                return False
            
            shuttle_config = shuttles[self.shuttle_id]
            self.shuttle_ip = shuttle_config['host']
            self.command_port = shuttle_config.get('command_port', 2000)
            
            logger.info(f"✅ Найден шаттл {self.shuttle_id}: {self.shuttle_ip}:{self.command_port}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке конфигурации: {e}")
            return False
    
    async def send_command_direct(self, command: str) -> bool:
        """Отправляет команду напрямую шаттлу (как в shuttle_direct_client.py)"""
        try:
            logger.info(f"📤 Подключаемся к шаттлу {self.shuttle_ip}:{self.command_port} напрямую")
            
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.shuttle_ip, self.command_port),
                timeout=5.0
            )
            
            # Добавляем терминатор CRLF если его нет
            if not command.endswith('\r\n'):
                command = command.rstrip('\n') + '\r\n'
            
            # Логируем байты для отладки
            logger.debug(f"Отправляем команду в байтах: {command.encode('utf-8')!r}")
            
            logger.info(f"📤 Отправляем команду напрямую: '{command.strip()}'")
            writer.write(command.encode('utf-8'))
            await writer.drain()
            
            # Закрываем соединение
            writer.close()
            await writer.wait_closed()
            
            logger.info(f"✅ Команда отправлена шаттлу {self.shuttle_id} напрямую")
            return True
        except asyncio.TimeoutError:
            logger.error(f"❌ Таймаут подключения к шаттлу {self.shuttle_ip}:{self.command_port}")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке команды напрямую: {e}")
            return False
    
    async def send_command_via_gateway(self, command: str) -> bool:
        """Отправляет команду через API шлюза"""
        try:
            # Здесь можно реализовать отправку через API шлюза
            # Но для простоты мы будем использовать прямое подключение к шлюзу
            
            logger.info(f"📤 Подключаемся к шлюзу для отправки команды шаттлу {self.shuttle_id}")
            
            # Предполагаем, что шлюз слушает на порту 8080
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('localhost', 8080),
                timeout=5.0
            )
            
            # Формируем запрос к API шлюза
            api_request = f"POST /api/shuttle/{self.shuttle_id}/command HTTP/1.1\r\n"
            api_request += "Host: localhost:8080\r\n"
            api_request += "Content-Type: application/json\r\n"
            api_request += f"Content-Length: {len(command) + 10}\r\n"
            api_request += "\r\n"
            api_request += f'{{"command": "{command.strip()}"}}'
            
            logger.debug(f"Отправляем API запрос в байтах: {api_request.encode('utf-8')!r}")
            
            writer.write(api_request.encode('utf-8'))
            await writer.drain()
            
            # Читаем ответ
            response = await reader.read(1024)
            logger.info(f"Ответ от шлюза: {response.decode('utf-8', errors='ignore')}")
            
            # Закрываем соединение
            writer.close()
            await writer.wait_closed()
            
            logger.info(f"✅ Команда отправлена шаттлу {self.shuttle_id} через шлюз")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке команды через шлюз: {e}")
            return False
    
    async def listen_for_response(self, timeout: float = 10.0):
        """Слушает ответ от шаттла на порту 8181"""
        logger.info(f"👂 Слушаем ответ от шаттла {self.shuttle_id} на порту {self.listener_port}")
        
        server = None
        try:
            server = await asyncio.start_server(
                self._handle_response,
                '0.0.0.0',
                self.listener_port
            )
            
            logger.info(f"🔊 Сервер запущен на порту {self.listener_port}")
            
            # Ждем ответ с таймаутом
            try:
                await asyncio.wait_for(self._wait_for_response(), timeout=timeout)
                if self.response_received:
                    logger.info(f"✅ Получен ответ от шаттла {self.shuttle_id}: '{self.response_data}'")
                else:
                    logger.warning(f"⏰ Таймаут ожидания ответа от шаттла {self.shuttle_id}")
            except asyncio.TimeoutError:
                logger.warning(f"⏰ Таймаут ожидания ответа от шаттла {self.shuttle_id}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при прослушивании ответа: {e}")
        finally:
            if server:
                server.close()
                await server.wait_closed()
                logger.info("🔇 Сервер остановлен")
    
    async def _handle_response(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Обрабатывает входящее соединение"""
        peer_name = writer.get_extra_info('peername')
        client_ip = peer_name[0] if peer_name else 'unknown'
        
        # Проверяем, что ответ пришел от нужного шаттла
        if client_ip != self.shuttle_ip:
            logger.warning(f"🚫 Игнорируем соединение от {client_ip} (ожидаем {self.shuttle_ip})")
            writer.close()
            await writer.wait_closed()
            return
        
        logger.info(f"📥 Получено соединение от шаттла {client_ip}")
        
        try:
            # Читаем данные
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            if data:
                message = data.decode('utf-8').strip()
                logger.info(f"📨 Ответ от шаттла {self.shuttle_id}: '{message}'")
                logger.debug(f"Ответ в байтах: {data!r}")
                self.response_data = message
                self.response_received = True
            
        except asyncio.TimeoutError:
            logger.warning(f"⏰ Таймаут чтения данных от шаттла {client_ip}")
        except Exception as e:
            logger.error(f"❌ Ошибка при чтении данных: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _wait_for_response(self):
        """Ждет получения ответа"""
        while not self.response_received:
            await asyncio.sleep(0.1)

async def main():
    parser = argparse.ArgumentParser(description='Диагностика проблемы с отправкой команд шаттлам')
    parser.add_argument('shuttle_id', help='ID шаттла (например: shuttle_140)')
    parser.add_argument('command', help='Команда для отправки (например: STATUS)')
    parser.add_argument('--mode', choices=['direct', 'gateway', 'both'], default='both',
                        help='Режим отправки: напрямую, через шлюз или оба варианта')
    parser.add_argument('--timeout', '-t', type=float, default=10.0, help='Таймаут ожидания ответа в секундах')
    parser.add_argument('--no-listen', action='store_true', help='Не слушать ответ, только отправить команду')
    
    args = parser.parse_args()
    
    logger.info(f"🚀 Запуск диагностики для шаттла {args.shuttle_id}")
    logger.info("=" * 60)
    
    debugger = ShuttleDebugger(args.shuttle_id)
    
    # Загружаем конфигурацию шаттла
    if not debugger.load_shuttle_config():
        return 1
    
    # Отправляем команду напрямую
    if args.mode in ['direct', 'both']:
        logger.info("=" * 60)
        logger.info("ОТПРАВКА НАПРЯМУЮ")
        logger.info("=" * 60)
        if not await debugger.send_command_direct(args.command):
            logger.error("❌ Не удалось отправить команду напрямую")
        
        # Слушаем ответ если нужно
        if not args.no_listen:
            await debugger.listen_for_response(args.timeout)
            debugger.response_received = False  # Сбрасываем флаг для следующего теста
    
    # Отправляем команду через шлюз
    if args.mode in ['gateway', 'both']:
        logger.info("=" * 60)
        logger.info("ОТПРАВКА ЧЕРЕЗ ШЛЮЗ")
        logger.info("=" * 60)
        if not await debugger.send_command_via_gateway(args.command):
            logger.error("❌ Не удалось отправить команду через шлюз")
        
        # Слушаем ответ если нужно
        if not args.no_listen:
            await debugger.listen_for_response(args.timeout)
    
    logger.info("=" * 60)
    logger.info("✅ Диагностика завершена")
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
#!/usr/bin/env python3
"""
Скрипт для прямого подключения к шаттлу по ID и отправки команд
"""
import asyncio
import sys
import yaml
import argparse
from typing import Optional

class ShuttleDirectClient:
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
                print(f"❌ Шаттл {self.shuttle_id} не найден в конфигурации")
                return False
            
            shuttle_config = shuttles[self.shuttle_id]
            self.shuttle_ip = shuttle_config['host']
            self.command_port = shuttle_config.get('command_port', 2000)
            
            print(f"✅ Найден шаттл {self.shuttle_id}: {self.shuttle_ip}:{self.command_port}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при загрузке конфигурации: {e}")
            return False
    
    async def send_command(self, command: str) -> bool:
        """Отправляет команду шаттлу"""
        try:
            print(f"📤 Подключаемся к шаттлу {self.shuttle_ip}:{self.command_port}")
            
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.shuttle_ip, self.command_port),
                timeout=5.0
            )
            
            # Добавляем терминатор CRLF если его нет
            if not command.endswith('\r\n'):
                command = command.rstrip('\n') + '\r\n'
            
            print(f"📤 Отправляем команду: '{command.strip()}'")
            writer.write(command.encode('utf-8'))
            await writer.drain()
            
            # Закрываем соединение
            writer.close()
            await writer.wait_closed()
            
            print(f"✅ Команда отправлена шаттлу {self.shuttle_id}")
            return True
            
        except asyncio.TimeoutError:
            print(f"❌ Таймаут подключения к шаттлу {self.shuttle_ip}:{self.command_port}")
            return False
        except Exception as e:
            print(f"❌ Ошибка при отправке команды: {e}")
            return False
    
    async def listen_for_response(self, timeout: float = 10.0):
        """Слушает ответ от шаттла на порту 8181"""
        print(f"👂 Слушаем ответ от шаттла {self.shuttle_id} на порту {self.listener_port}")
        
        server = None
        try:
            server = await asyncio.start_server(
                self._handle_response,
                '0.0.0.0',
                self.listener_port
            )
            
            print(f"🔊 Сервер запущен на порту {self.listener_port}")
            
            # Ждем ответ с таймаутом
            try:
                await asyncio.wait_for(self._wait_for_response(), timeout=timeout)
                if self.response_received:
                    print(f"✅ Получен ответ от шаттла {self.shuttle_id}: '{self.response_data}'")
                else:
                    print(f"⏰ Таймаут ожидания ответа от шаттла {self.shuttle_id}")
            except asyncio.TimeoutError:
                print(f"⏰ Таймаут ожидания ответа от шаттла {self.shuttle_id}")
                
        except Exception as e:
            print(f"❌ Ошибка при прослушивании ответа: {e}")
        finally:
            if server:
                server.close()
                await server.wait_closed()
                print("🔇 Сервер остановлен")
    
    async def _handle_response(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Обрабатывает входящее соединение"""
        peer_name = writer.get_extra_info('peername')
        client_ip = peer_name[0] if peer_name else 'unknown'
        
        # Проверяем, что ответ пришел от нужного шаттла
        if client_ip != self.shuttle_ip:
            print(f"🚫 Игнорируем соединение от {client_ip} (ожидаем {self.shuttle_ip})")
            writer.close()
            await writer.wait_closed()
            return
        
        print(f"📥 Получено соединение от шаттла {client_ip}")
        
        try:
            # Читаем данные
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            if data:
                message = data.decode('utf-8').strip()
                print(f"📨 Ответ от шаттла {self.shuttle_id}: '{message}'")
                self.response_data = message
                self.response_received = True
            
        except asyncio.TimeoutError:
            print(f"⏰ Таймаут чтения данных от шаттла {client_ip}")
        except Exception as e:
            print(f"❌ Ошибка при чтении данных: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _wait_for_response(self):
        """Ждет получения ответа"""
        while not self.response_received:
            await asyncio.sleep(0.1)

async def main():
    parser = argparse.ArgumentParser(description='Прямое подключение к шаттлу и отправка команд')
    parser.add_argument('shuttle_id', help='ID шаттла (например: shuttle_140)')
    parser.add_argument('command', help='Команда для отправки (например: STATUS)')
    parser.add_argument('--timeout', '-t', type=float, default=10.0, help='Таймаут ожидания ответа в секундах')
    parser.add_argument('--no-listen', action='store_true', help='Не слушать ответ, только отправить команду')
    
    args = parser.parse_args()
    
    print(f"🚀 Запуск клиента для шаттла {args.shuttle_id}")
    print("=" * 60)
    
    client = ShuttleDirectClient(args.shuttle_id)
    
    # Загружаем конфигурацию шаттла
    if not client.load_shuttle_config():
        return 1
    
    # Отправляем команду
    if not await client.send_command(args.command):
        return 1
    
    # Слушаем ответ если нужно
    if not args.no_listen:
        await client.listen_for_response(args.timeout)
    
    print("=" * 60)
    print("✅ Завершено")
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
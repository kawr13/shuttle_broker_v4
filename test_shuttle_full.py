#!/usr/bin/env python3
"""
Полный тестовый скрипт для взаимодействия с шаттлом
"""
import asyncio
import argparse
import sys
import logging
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_shuttle")

class ShuttleTester:
    """Тестер для взаимодействия с шаттлом"""
    
    def __init__(self, shuttle_ip, command_port=2000, listen_port=8181):
        self.shuttle_ip = shuttle_ip
        self.command_port = command_port
        self.listen_port = listen_port
        self.server = None
        self.running = False
        self.response_received = asyncio.Event()
        self.responses = []
    
    async def start_listener(self):
        """Запускает слушатель для приема ответов от шаттла"""
        self.server = await asyncio.start_server(
            self.handle_response,
            '0.0.0.0',
            self.listen_port
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"Слушатель запущен на {addr[0]}:{addr[1]}")
        
        self.running = True
        asyncio.create_task(self._serve_forever())
    
    async def _serve_forever(self):
        """Запускает сервер в бесконечном цикле"""
        async with self.server:
            await self.server.serve_forever()
    
    async def handle_response(self, reader, writer):
        """Обрабатывает ответ от шаттла"""
        peer_name = writer.get_extra_info('peername')
        shuttle_ip, shuttle_port = peer_name
        logger.info(f"Получено соединение от {shuttle_ip}:{shuttle_port}")
        
        try:
            while self.running:
                data = await reader.read(1024)
                if not data:
                    logger.info(f"Соединение с {shuttle_ip}:{shuttle_port} закрыто")
                    break
                
                try:
                    message = data.decode('utf-8').strip()
                    logger.info(f"Получено сообщение: '{message}'")
                    self.responses.append(message)
                    
                    # Отправляем MRCD в ответ
                    if message != "MRCD":
                        response = "MRCD\n"
                        writer.write(response.encode())
                        await writer.drain()
                        logger.info(f"Отправлен MRCD в ответ")
                    
                    # Устанавливаем событие, что ответ получен
                    self.response_received.set()
                    
                except UnicodeDecodeError:
                    logger.info(f"Получено бинарное сообщение: {data.hex()}")
                    self.responses.append(f"BINARY: {data.hex()}")
                    self.response_received.set()
                
        except Exception as e:
            logger.error(f"Ошибка при обработке соединения: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Соединение с {shuttle_ip}:{shuttle_port} закрыто")
    
    async def send_command(self, command, params=None):
        """Отправляет команду шаттлу"""
        try:
            # Формируем команду
            if params:
                if command in ["FIFO", "FILO"]:
                    try:
                        param_value = int(params)
                        command_str = f"{command}-{param_value:03d}\n"
                    except ValueError:
                        command_str = f"{command}-{params}\n"
                else:
                    command_str = f"{command}-{params}\n"
            else:
                command_str = f"{command}\n"
            
            logger.info(f"Подключение к шаттлу {self.shuttle_ip}:{self.command_port}")
            reader, writer = await asyncio.open_connection(self.shuttle_ip, self.command_port)
            
            logger.info(f"Отправка команды: {command_str.strip()}")
            writer.write(command_str.encode())
            await writer.drain()
            
            # Ждем ответ на порту 2000
            logger.info("Ожидание прямого ответа...")
            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
                if data:
                    try:
                        response = data.decode('utf-8').strip()
                        logger.info(f"Получен прямой ответ: {response}")
                        self.responses.append(f"DIRECT: {response}")
                    except UnicodeDecodeError:
                        logger.info(f"Получен бинарный ответ: {data.hex()}")
                        self.responses.append(f"DIRECT_BINARY: {data.hex()}")
            except asyncio.TimeoutError:
                logger.info("Нет прямого ответа")
            
            # Закрываем соединение
            writer.close()
            await writer.wait_closed()
            
            return True
        except ConnectionRefusedError:
            logger.error(f"Не удалось подключиться к {self.shuttle_ip}:{self.command_port}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при отправке команды: {e}")
            return False
    
    async def stop(self):
        """Останавливает тестер"""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Слушатель остановлен")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Полный тестер для взаимодействия с шаттлом")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("command", help="Команда (STATUS, BATTERY, HOME, и т.д.)")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла для команд (по умолчанию 2000)")
    parser.add_argument("--listen-port", type=int, default=8181, help="Порт для прослушивания ответов (по умолчанию 8181)")
    parser.add_argument("--params", help="Параметры команды")
    parser.add_argument("--timeout", type=int, default=15, help="Таймаут ожидания ответа (по умолчанию 15 секунд)")
    
    args = parser.parse_args()
    
    # Создаем тестер
    tester = ShuttleTester(args.ip, args.port, args.listen_port)
    
    try:
        # Запускаем слушатель
        await tester.start_listener()
        
        # Отправляем команду
        await tester.send_command(args.command, args.params)
        
        # Ждем ответ
        logger.info(f"Ожидание ответа на порту {args.listen_port} в течение {args.timeout} секунд...")
        try:
            await asyncio.wait_for(tester.response_received.wait(), timeout=args.timeout)
            logger.info("Ответ получен!")
            
            # Выводим все полученные ответы
            if tester.responses:
                logger.info("Полученные ответы:")
                for i, response in enumerate(tester.responses):
                    logger.info(f"  {i+1}. {response}")
            else:
                logger.warning("Ответы не получены")
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут ожидания ответа ({args.timeout} секунд)")
        
        # Ждем еще немного для возможных дополнительных ответов
        await asyncio.sleep(2)
        
    finally:
        # Останавливаем тестер
        await tester.stop()
    
    return 0 if tester.responses else 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
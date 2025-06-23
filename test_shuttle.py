#!/usr/bin/env python3
"""
Тестовый скрипт для эмуляции шаттла и проверки связи
"""
import asyncio
import socket
import argparse
import sys
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_shuttle")

class ShuttleEmulator:
    """Эмулятор шаттла для тестирования связи"""
    
    def __init__(self, gateway_ip, gateway_port=8181, listen_port=2000):
        self.gateway_ip = gateway_ip
        self.gateway_port = gateway_port
        self.listen_port = listen_port
        self.running = False
        self.server = None
        
    async def start(self):
        """Запускает эмулятор шаттла"""
        # Запускаем сервер для прослушивания команд
        self.server = await asyncio.start_server(
            self.handle_command, 
            '0.0.0.0', 
            self.listen_port
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"Шаттл слушает команды на {addr[0]}:{addr[1]}")
        
        self.running = True
        asyncio.create_task(self.send_status_periodically())
        
        async with self.server:
            await self.server.serve_forever()
    
    async def handle_command(self, reader, writer):
        """Обрабатывает входящие команды"""
        addr = writer.get_extra_info('peername')
        logger.info(f"Получено соединение от {addr[0]}:{addr[1]}")
        
        while True:
            try:
                data = await reader.readuntil(b'\n')
                message = data.decode().strip()
                logger.info(f"Получена команда: {message}")
                
                # Отправляем ответ на команду
                if message == "STATUS":
                    response = "STATUS=FREE\n"
                    writer.write(response.encode())
                    await writer.drain()
                    logger.info(f"Отправлен ответ: {response.strip()}")
                    
                    # Отправляем дополнительную информацию
                    location = "LOCATION=CELL:A1\n"
                    writer.write(location.encode())
                    await writer.drain()
                    logger.info(f"Отправлен ответ: {location.strip()}")
                    
                elif message == "BATTERY":
                    response = "BATTERY=85%\n"
                    writer.write(response.encode())
                    await writer.drain()
                    logger.info(f"Отправлен ответ: {response.strip()}")
                    
                elif message == "MRCD":
                    # На MRCD не отвечаем
                    pass
                    
                else:
                    # На другие команды отвечаем подтверждением
                    response = f"{message}_DONE\n"
                    writer.write(response.encode())
                    await writer.drain()
                    logger.info(f"Отправлен ответ: {response.strip()}")
                    
            except asyncio.IncompleteReadError:
                logger.info("Соединение закрыто")
                break
            except Exception as e:
                logger.error(f"Ошибка при обработке команды: {e}")
                break
    
    async def send_status_periodically(self):
        """Периодически отправляет статус на шлюз"""
        while self.running:
            try:
                # Подключаемся к шлюзу
                reader, writer = await asyncio.open_connection(
                    self.gateway_ip, 
                    self.gateway_port
                )
                
                # Отправляем статус
                status = "STATUS=FREE\n"
                writer.write(status.encode())
                await writer.drain()
                logger.info(f"Отправлен статус на шлюз {self.gateway_ip}:{self.gateway_port}: {status.strip()}")
                
                # Ждем ответ (MRCD)
                try:
                    data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5.0)
                    response = data.decode().strip()
                    logger.info(f"Получен ответ от шлюза: {response}")
                except asyncio.TimeoutError:
                    logger.warning("Таймаут ожидания ответа от шлюза")
                except Exception as e:
                    logger.error(f"Ошибка при чтении ответа от шлюза: {e}")
                
                # Закрываем соединение
                writer.close()
                await writer.wait_closed()
                
            except ConnectionRefusedError:
                logger.error(f"Не удалось подключиться к шлюзу {self.gateway_ip}:{self.gateway_port}")
            except Exception as e:
                logger.error(f"Ошибка при отправке статуса: {e}")
            
            # Ждем перед следующей отправкой
            await asyncio.sleep(10)
    
    async def stop(self):
        """Останавливает эмулятор шаттла"""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Эмулятор шаттла остановлен")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Эмулятор шаттла для тестирования связи")
    parser.add_argument("--gateway-ip", default="10.181.80.30", help="IP-адрес шлюза")
    parser.add_argument("--gateway-port", type=int, default=8181, help="Порт шлюза")
    parser.add_argument("--listen-port", type=int, default=2000, help="Порт для прослушивания команд")
    
    args = parser.parse_args()
    
    # Запускаем эмулятор шаттла
    emulator = ShuttleEmulator(
        args.gateway_ip, 
        args.gateway_port, 
        args.listen_port
    )
    
    try:
        await emulator.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения")
    finally:
        await emulator.stop()

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
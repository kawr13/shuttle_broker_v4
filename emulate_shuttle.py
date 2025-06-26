#!/usr/bin/env python3
"""
Скрипт для эмуляции подключения шаттла
"""
import asyncio
import argparse
import sys
import logging
import time
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("emulate_shuttle")

class ShuttleEmulator:
    """Эмулятор шаттла"""
    
    def __init__(self, shuttle_id, shuttle_ip, gateway_ip, gateway_port=8181):
        self.shuttle_id = shuttle_id
        self.shuttle_ip = shuttle_ip
        self.gateway_ip = gateway_ip
        self.gateway_port = gateway_port
        self.writer = None
        self.reader = None
        self.connected = False
        self.running = False
    
    async def connect(self):
        """Подключается к шлюзу"""
        try:
            logger.info(f"Подключение к шлюзу {self.gateway_ip}:{self.gateway_port}...")
            self.reader, self.writer = await asyncio.open_connection(
                self.gateway_ip, self.gateway_port
            )
            self.connected = True
            logger.info(f"Подключение к шлюзу установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка при подключении к шлюзу: {e}")
            return False
    
    async def disconnect(self):
        """Отключается от шлюза"""
        if self.connected and self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
                logger.info(f"Отключение от шлюза выполнено")
            except Exception as e:
                logger.error(f"Ошибка при отключении от шлюза: {e}")
            finally:
                self.connected = False
                self.writer = None
                self.reader = None
    
    async def send_message(self, message):
        """Отправляет сообщение шлюзу"""
        if not self.connected:
            logger.error("Нет подключения к шлюзу")
            return False
        
        try:
            # Добавляем терминатор CRLF (\r\n), если его нет
            if not message.endswith('\r\n'):
                message = message.rstrip('\n')  # Удаляем существующий LF, если есть
                message += '\r\n'
            
            # Отправляем сообщение
            self.writer.write(message.encode('ascii'))
            await self.writer.drain()
            logger.info(f"Отправлено сообщение: '{message.strip()}'")
            return True
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")
            return False
    
    async def receive_message(self):
        """Получает сообщение от шлюза"""
        if not self.connected:
            logger.error("Нет подключения к шлюзу")
            return None
        
        try:
            # Читаем данные
            data = await self.reader.read(1024)
            if not data:
                logger.warning("Получены пустые данные")
                return None
            
            # Декодируем сообщение
            message = data.decode('ascii').strip()
            logger.info(f"Получено сообщение: '{message}'")
            return message
        except Exception as e:
            logger.error(f"Ошибка при получении сообщения: {e}")
            return None
    
    async def run(self):
        """Запускает эмулятор шаттла"""
        if not await self.connect():
            return False
        
        self.running = True
        
        # Отправляем статус
        await self.send_message(f"STATUS=FREE")
        
        # Основной цикл
        try:
            while self.running:
                # Получаем сообщение от шлюза
                message = await self.receive_message()
                
                if message:
                    # Обрабатываем сообщение
                    if message == "STATUS":
                        # Отправляем статус
                        await self.send_message(f"STATUS=FREE")
                    elif message == "LOC":
                        # Отправляем местоположение
                        await self.send_message(f"LOC=NONE_RFID")
                    elif message == "MRCD":
                        # Подтверждаем получение MRCD
                        logger.info("Получен MRCD")
                    else:
                        # Отправляем подтверждение получения команды
                        await self.send_message(f"{message}_STARTED")
                        
                        # Эмулируем выполнение команды
                        await asyncio.sleep(2)
                        
                        # Отправляем завершение команды
                        await self.send_message(f"{message}_DONE")
                
                # Периодически отправляем статус
                if random.random() < 0.1:  # 10% шанс отправить статус
                    await self.send_message(f"STATUS=FREE")
                    await asyncio.sleep(1)
                
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Эмулятор шаттла остановлен")
        except Exception as e:
            logger.error(f"Ошибка в эмуляторе шаттла: {e}")
        finally:
            await self.disconnect()
            self.running = False
        
        return True
    
    def stop(self):
        """Останавливает эмулятор шаттла"""
        self.running = False

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Эмулятор шаттла")
    parser.add_argument("--id", default="test_shuttle", help="ID шаттла")
    parser.add_argument("--ip", default="10.181.80.200", help="IP-адрес шаттла")
    parser.add_argument("--gateway", default="127.0.0.1", help="IP-адрес шлюза")
    parser.add_argument("--port", type=int, default=8181, help="Порт шлюза")
    parser.add_argument("--duration", type=int, default=60, help="Продолжительность работы в секундах")
    
    args = parser.parse_args()
    
    # Создаем эмулятор шаттла
    emulator = ShuttleEmulator(args.id, args.ip, args.gateway, args.port)
    
    # Запускаем эмулятор в отдельной задаче
    task = asyncio.create_task(emulator.run())
    
    try:
        # Ждем указанное время
        await asyncio.sleep(args.duration)
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    finally:
        # Останавливаем эмулятор
        emulator.stop()
        await task
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
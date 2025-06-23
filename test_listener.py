#!/usr/bin/env python3
"""
Тестовый слушатель для приема сообщений от шаттлов
"""
import asyncio
import argparse
import sys
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_listener")

class ShuttleListener:
    """Тестовый слушатель для приема сообщений от шаттлов"""
    
    def __init__(self, port=8181):
        self.port = port
        self.server = None
        self.running = False
    
    async def start(self):
        """Запускает слушатель"""
        self.server = await asyncio.start_server(
            self.handle_connection,
            '0.0.0.0',
            self.port
        )
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"Слушатель запущен на {addr[0]}:{addr[1]}")
        
        self.running = True
        async with self.server:
            await self.server.serve_forever()
    
    async def handle_connection(self, reader, writer):
        """Обрабатывает входящее соединение"""
        peer_name = writer.get_extra_info('peername')
        shuttle_ip, shuttle_port = peer_name
        logger.info(f"Новое соединение от {shuttle_ip}:{shuttle_port}")
        
        try:
            while self.running:
                try:
                    # Читаем данные от шаттла
                    data = await asyncio.wait_for(
                        reader.readuntil(b'\n'),
                        timeout=60.0
                    )
                    message = data.decode('utf-8').strip()
                    logger.info(f"Получено сообщение от {shuttle_ip}: '{message}'")
                    
                    # Отправляем MRCD в ответ на любое сообщение
                    if message != "MRCD":
                        response = "MRCD\n"
                        writer.write(response.encode('utf-8'))
                        await writer.drain()
                        logger.info(f"Отправлен MRCD в ответ на {shuttle_ip}")
                        
                except asyncio.TimeoutError:
                    # Проверяем, что соединение все еще активно
                    try:
                        writer.write(b"PING\n")
                        await writer.drain()
                    except Exception:
                        logger.warning(f"Соединение с {shuttle_ip} потеряно")
                        break
                except asyncio.IncompleteReadError:
                    logger.warning(f"Соединение с {shuttle_ip} закрыто")
                    break
                except Exception as e:
                    logger.error(f"Ошибка при чтении данных от {shuttle_ip}: {e}")
                    break
        finally:
            # Закрываем соединение
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            
            logger.info(f"Соединение с {shuttle_ip} закрыто")
    
    async def stop(self):
        """Останавливает слушатель"""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("Слушатель остановлен")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестовый слушатель для приема сообщений от шаттлов")
    parser.add_argument("--port", type=int, default=8181, help="Порт для прослушивания (по умолчанию 8181)")
    
    args = parser.parse_args()
    
    listener = ShuttleListener(args.port)
    
    try:
        await listener.start()
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения")
    finally:
        await listener.stop()

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
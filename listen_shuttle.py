#!/usr/bin/env python3
"""
Скрипт для прослушивания и анализа сообщений от шаттла
"""
import asyncio
import argparse
import sys
import logging
import binascii
import time
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("listen_shuttle")

class ShuttleListener:
    def __init__(self, host, port, log_file=None):
        self.host = host
        self.port = port
        self.server = None
        self.connections = {}
        self.log_file = log_file
        self.log_handle = None
        
        if self.log_file:
            try:
                self.log_handle = open(self.log_file, 'a', encoding='utf-8')
                self.log_handle.write(f"\n\n--- Начало сессии {datetime.now().isoformat()} ---\n\n")
                self.log_handle.flush()
            except Exception as e:
                logger.error(f"Не удалось открыть файл лога: {e}")
                self.log_handle = None
    
    def log_to_file(self, message):
        """Записывает сообщение в файл лога"""
        if self.log_handle:
            try:
                self.log_handle.write(f"{datetime.now().isoformat()} - {message}\n")
                self.log_handle.flush()
            except Exception as e:
                logger.error(f"Ошибка при записи в файл лога: {e}")
    
    async def start(self):
        """Запускает сервер для прослушивания сообщений от шаттлов"""
        try:
            self.server = await asyncio.start_server(
                self._handle_connection,
                self.host,
                self.port,
            )
            
            addr = self.server.sockets[0].getsockname()
            logger.info(f"Слушаем на {addr[0]}:{addr[1]}")
            self.log_to_file(f"Слушаем на {addr[0]}:{addr[1]}")
            
            async with self.server:
                await self.server.serve_forever()
        except Exception as e:
            logger.error(f"Ошибка при запуске сервера: {e}")
            self.log_to_file(f"Ошибка при запуске сервера: {e}")
    
    async def _handle_connection(self, reader, writer):
        """Обрабатывает новое соединение"""
        peer_name = writer.get_extra_info('peername')
        client_ip, client_port = peer_name
        connection_id = f"{client_ip}:{client_port}"
        
        logger.info(f"Новое соединение от {connection_id}")
        self.log_to_file(f"Новое соединение от {connection_id}")
        
        self.connections[connection_id] = writer
        
        try:
            while True:
                # Читаем данные
                data = await reader.read(1024)
                if not data:
                    logger.info(f"Соединение с {connection_id} закрыто клиентом")
                    self.log_to_file(f"Соединение с {connection_id} закрыто клиентом")
                    break
                
                # Анализируем полученные данные
                data_hex = data.hex()
                logger.info(f"Получены данные от {connection_id} (hex): {data_hex}")
                self.log_to_file(f"Получены данные от {connection_id} (hex): {data_hex}")
                
                # Пытаемся декодировать как текст в разных кодировках
                encodings = ["utf-8", "ascii", "latin1", "cp1251", "koi8-r", "cp866"]
                for enc in encodings:
                    try:
                        text = data.decode(enc)
                        logger.info(f"Декодировано как {enc}: '{text}'")
                        self.log_to_file(f"Декодировано как {enc}: '{text}'")
                    except UnicodeDecodeError:
                        pass
                
                # Анализируем терминаторы
                if data.endswith(b'\r\n'):
                    logger.info("Сообщение заканчивается на CRLF (\\r\\n)")
                    self.log_to_file("Сообщение заканчивается на CRLF (\\r\\n)")
                elif data.endswith(b'\n'):
                    logger.info("Сообщение заканчивается на LF (\\n)")
                    self.log_to_file("Сообщение заканчивается на LF (\\n)")
                elif data.endswith(b'\r'):
                    logger.info("Сообщение заканчивается на CR (\\r)")
                    self.log_to_file("Сообщение заканчивается на CR (\\r)")
                elif data.endswith(b'\0'):
                    logger.info("Сообщение заканчивается на NULL (\\0)")
                    self.log_to_file("Сообщение заканчивается на NULL (\\0)")
                
                # Отправляем MRCD в ответ
                response_options = [
                    b"MRCD\r\n",
                    b"MRCD\n",
                    b"MRCD\r",
                    b"MRCD",
                    b"MRCD\0",
                    b"MRCD;\r\n",
                    b"MRCD;\n",
                ]
                
                # Выбираем ответ в зависимости от аргументов
                response = response_options[0]  # По умолчанию MRCD\r\n
                
                writer.write(response)
                await writer.drain()
                
                logger.info(f"Отправлен ответ (hex): {response.hex()}")
                self.log_to_file(f"Отправлен ответ (hex): {response.hex()}")
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Ошибка при обработке соединения с {connection_id}: {e}")
            self.log_to_file(f"Ошибка при обработке соединения с {connection_id}: {e}")
        finally:
            # Закрываем соединение
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
            
            if connection_id in self.connections:
                del self.connections[connection_id]
            
            logger.info(f"Соединение с {connection_id} закрыто")
            self.log_to_file(f"Соединение с {connection_id} закрыто")
    
    def close(self):
        """Закрывает сервер и все соединения"""
        if self.log_handle:
            try:
                self.log_handle.write(f"\n\n--- Конец сессии {datetime.now().isoformat()} ---\n\n")
                self.log_handle.close()
            except:
                pass
            self.log_handle = None

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Прослушивание и анализ сообщений от шаттла")
    parser.add_argument("--host", default="0.0.0.0", help="Хост для прослушивания (по умолчанию 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="Порт для прослушивания (по умолчанию 5000)")
    parser.add_argument("--log-file", help="Файл для записи лога")
    
    args = parser.parse_args()
    
    listener = ShuttleListener(args.host, args.port, args.log_file)
    
    try:
        await listener.start()
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    finally:
        listener.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
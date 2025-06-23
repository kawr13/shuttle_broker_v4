#!/usr/bin/env python3
"""
Скрипт для отправки сырых команд шаттлу в шестнадцатеричном формате
"""
import asyncio
import argparse
import sys
import logging
import binascii
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("send_raw_command")

async def send_raw_command(ip, port, hex_command, wait_time=5.0):
    """Отправляет сырую команду в шестнадцатеричном формате"""
    try:
        # Преобразуем hex-строку в байты
        command_bytes = binascii.unhexlify(hex_command)
        
        logger.info(f"Отправка команды (hex): {hex_command}")
        logger.info(f"Размер команды: {len(command_bytes)} байт")
        
        # Пытаемся декодировать как текст
        try:
            text = command_bytes.decode('utf-8', errors='replace')
            logger.info(f"Команда как текст: '{text}'")
        except:
            logger.info("Не удалось декодировать команду как текст")
        
        # Подключаемся к шаттлу
        reader, writer = await asyncio.open_connection(ip, port)
        
        # Отправляем команду
        writer.write(command_bytes)
        await writer.drain()
        
        logger.info(f"Команда отправлена, ожидание ответа {wait_time} секунд...")
        
        # Ждем ответа
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=wait_time)
            if data:
                response_hex = data.hex()
                logger.info(f"Получен ответ (hex): {response_hex}")
                logger.info(f"Размер ответа: {len(data)} байт")
                
                # Пытаемся декодировать как текст
                try:
                    text = data.decode('utf-8', errors='replace')
                    logger.info(f"Ответ как текст: '{text}'")
                except:
                    logger.info("Не удалось декодировать ответ как текст")
                
                # Анализируем терминаторы
                if data.endswith(b'\r\n'):
                    logger.info("Ответ заканчивается на CRLF (\\r\\n)")
                elif data.endswith(b'\n'):
                    logger.info("Ответ заканчивается на LF (\\n)")
                elif data.endswith(b'\r'):
                    logger.info("Ответ заканчивается на CR (\\r)")
                elif data.endswith(b'\0'):
                    logger.info("Ответ заканчивается на NULL (\\0)")
            else:
                logger.warning("Получен пустой ответ")
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут ожидания ответа ({wait_time} секунд)")
        
        # Закрываем соединение
        writer.close()
        await writer.wait_closed()
        
    except binascii.Error:
        logger.error("Неверный формат шестнадцатеричной строки")
    except ConnectionRefusedError:
        logger.error(f"Соединение отклонено {ip}:{port}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")

def hex_string(command, terminator="0d0a"):
    """Преобразует текстовую команду в шестнадцатеричную строку с указанным терминатором"""
    command_bytes = command.encode('utf-8')
    terminator_bytes = binascii.unhexlify(terminator)
    full_command = command_bytes + terminator_bytes
    return full_command.hex()

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Отправка сырых команд шаттлу")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--hex", help="Команда в шестнадцатеричном формате")
    parser.add_argument("--command", help="Текстовая команда")
    parser.add_argument("--terminator", default="0d0a", help="Терминатор в формате hex (по умолчанию 0d0a для CRLF)")
    parser.add_argument("--wait", type=float, default=5.0, help="Время ожидания ответа в секундах (по умолчанию 5.0)")
    
    args = parser.parse_args()
    
    if args.hex:
        # Используем команду в шестнадцатеричном формате
        hex_command = args.hex
    elif args.command:
        # Преобразуем текстовую команду в шестнадцатеричную строку
        hex_command = hex_string(args.command, args.terminator)
    else:
        parser.error("Необходимо указать либо --hex, либо --command")
        return 1
    
    await send_raw_command(args.ip, args.port, hex_command, args.wait)
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
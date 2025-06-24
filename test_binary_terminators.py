#!/usr/bin/env python3
"""
Тестирование команд для шаттла с фиксированной длиной и прослушиванием на порту 8181
"""
import asyncio
import argparse
import sys
import logging
import binascii
from collections import deque
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_binary_terminators")

# Очередь для хранения ответов с 8181
responses = deque(maxlen=100)

async def handle_listener(reader, writer):
    """Обработчик входящих сообщений на порту 8181"""
    try:
        peer = writer.get_extra_info('peername')
        logger.debug(f"Новое подключение на порту 8181 от {peer}")
        while True:
            data = await reader.read(1024)
            if not data:
                break
            response_hex = data.hex()
            try:
                response_text = data.decode('ascii', errors='ignore')
                logger.info(f"Получен ответ на 8181 (text): '{response_text}'")
            except:
                response_text = "Не удалось декодировать как текст"
            logger.info(f"Получен ответ на 8181 (hex): {response_hex}")
            responses.append((datetime.now(), response_text, response_hex))
    except Exception as e:
        logger.error(f"Ошибка при обработке соединения на 8181: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def start_listener():
    """Запускает TCP-сервер на порту 8181"""
    try:
        server = await asyncio.start_server(handle_listener, '0.0.0.0', 8181)
        logger.info("Слушатель запущен на порту 8181")
        async with server:
            await server.serve_forever()
    except Exception as e:
        logger.error(f"Ошибка запуска слушателя на 8181: {e}")

async def test_binary_command(ip, port, command, terminator_hex, fixed_length=None):
    """Тестирует команду с бинарным терминатором или фиксированной длиной"""
    try:
        if terminator_hex == "binary":
            full_command = b'\x02' + command.encode('ascii') + b'\x03'
        elif terminator_hex == "fixed":
            full_command = command.encode('ascii').ljust(fixed_length, b' ')
        elif terminator_hex == "":
            full_command = command.encode('ascii')
        else:
            terminator = binascii.unhexlify(terminator_hex)
            full_command = command.encode('ascii') + terminator
        
        logger.debug(f"Тестирование команды '{command}' с терминатором '{terminator_hex}'")
        logger.debug(f"Полная команда (hex): {full_command.hex()}")
        
        start_time = datetime.now()
        
        reader, writer = await asyncio.open_connection(ip, port)
        
        writer.write(full_command)
        await writer.drain()
        
        # Проверяем ответ на порту отправки
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=10.0)
            if data:
                response_hex = data.hex()
                try:
                    response_text = data.decode('ascii', errors='ignore')
                    logger.info(f"Получен ответ на порту {port} (text): '{response_text}'")
                except:
                    response_text = "Не удалось декодировать как текст"
                logger.info(f"Получен ответ на порту {port} (hex): {response_hex}")
                success = True
            else:
                logger.warning(f"Получен пустой ответ на порту {port}")
                success = False
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут ожидания ответа на порту {port}")
            success = False
        
        writer.close()
        await writer.wait_closed()
        
        # Проверяем ответы на 8181
        await asyncio.sleep(3)  # Увеличиваем ожидание до 3 секунд
        related_responses = [
            (t, text, hex_data) for t, text, hex_data in responses
            if (t - start_time).total_seconds() <= 3 and "STATUS=" in text
        ]
        if related_responses:
            logger.info(f"Найдены связанные ответы на 8181 для команды '{command}' с '{terminator_hex}':")
            for t, text, hex_data in related_responses:
                logger.info(f"  Время: {t}, Текст: '{text}', Hex: {hex_data}")
            success = True
        else:
            logger.info(f"Не найдены связанные ответы на 8181 для команды '{command}' с '{terminator_hex}'")
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании команды: {e}")
        return False

async def test_all_binary_terminators(ip, port, commands=["STATUS"]):
    """Тестирует все возможные бинарные терминаторы и фиксированную длину для списка команд"""
    terminators = [
        "",             # Без терминатора
        "0a",           # LF
        "0d0a",         # CRLF
        "0d",           # CR
        "fixed"         # Фиксированная длина (20 байт)
    ]
    
    results = []
    for cmd in commands:
        logger.info(f"Тестируем команду: {cmd}")
        for term_hex in terminators:
            success = await test_binary_command(ip, port, cmd, term_hex, fixed_length=20)
            results.append((cmd, term_hex, success))
            await asyncio.sleep(1)
    
    logger.info("Результаты тестирования:")
    successful_terminators = []
    for cmd, term_hex, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        logger.info(f"Команда '{cmd}' с '{term_hex}' - {status}")
        if success:
            successful_terminators.append((cmd, term_hex))
    
    if successful_terminators:
        logger.info("Успешные комбинации:")
        for cmd, term_hex in successful_terminators:
            logger.info(f"  Команда: {cmd}, Терминатор: {term_hex}")
    else:
        logger.info("Ни одна комбинация не сработала")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование команд для шаттла")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт для отправки команд (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Команда для тестирования (по умолчанию STATUS)")
    parser.add_argument("--terminator", help="Конкретный терминатор для тестирования (в формате hex, например 0d0a)")
    
    args = parser.parse_args()
    
    listener_task = asyncio.create_task(start_listener())
    
    if args.terminator:
        await test_binary_command(args.ip, args.port, args.command, args.terminator, fixed_length=20)
    else:
        commands = [args.command]  # Тестируем только указанную команду
        await test_all_binary_terminators(args.ip, args.port, commands)
    
    await asyncio.sleep(2)
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
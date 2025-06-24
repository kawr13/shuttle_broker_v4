#!/usr/bin/env python3
"""
Тестирование бинарных терминаторов и фиксированной длины команд для шаттла с прослушиванием на порту 8181
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
        logger.info(f"Новое соединение на порту 8181 от {peer}")
        while True:
            data = await reader.read(1024)
            if not data:
                break
            response_hex = data.hex()
            try:
                response_text = data.decode('ascii', errors='replace')
                logger.info(f"Получен ответ на 8181 (text): '{response_text}'")
            except:
                response_text = "Не удалось декодировать как текст"
            logger.info(f"Получен ответ на 8181 (hex): {response_hex}")
            # Сохраняем ответ с временной меткой
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
            full_command = command.encode('ascii').ljust(fixed_length, b' ')  # Дополняем пробелами
        else:
            terminator = binascii.unhexlify(terminator_hex)
            full_command = command.encode('ascii') + terminator
        
        logger.debug(f"Тестирование команды '{command}' с терминатором '{terminator_hex}'")
        logger.debug(f"Полная команда (hex): {full_command.hex()}")
        
        # Запоминаем время отправки
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
                    response_text = data.decode('ascii', errors='replace')
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
        await asyncio.sleep(1)  # Даём время для получения ответа
        related_responses = [
            (t, text, hex_data) for t, text, hex_data in responses
            if (t - start_time).total_seconds() <= 2  # Ответы в пределах 2 секунд
        ]
        if related_responses:
            logger.info(f"Найдены связанные ответы на 8181 для команды '{command}' с '{terminator_hex}':")
            for t, text, hex_data in related_responses:
                logger.info(f"  Время: {t}, Текст: '{text}', Hex: {hex_data}")
            success = True  # Считаем успешным, если есть ответ на 8181
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании команды: {e}")
        return False

async def test_all_binary_terminators(ip, port, command="STATUS"):
    """Тестирует все возможные бинарные терминаторы и фиксированную длину"""
    terminators = [
        "",             # Без терминатора
        "0a",           # LF
        "0d0a",         # CRLF
        "0d",           # CR
        "00",           # NULL
        "000a", "000d0a", "000d", "0a00", "0d0a00", "0d00",
        "03", "04", "1a", "1c", "1d", "1e", "1f",
        "030a", "030d0a", "040a", "040d0a",
        "0a0a", "0d0d", "0d0a0d0a",
        "3b", "3b0a", "3b0d0a", "2c", "2c0a", "2c0d0a",
        "binary",       # STX + команда + ETX
        "fixed"         # Фиксированная длина (20 байт)
    ]
    
    results = []
    for term_hex in terminators:
        success = await test_binary_command(ip, port, command, term_hex, fixed_length=20)
        results.append((term_hex, success))
        await asyncio.sleep(1)
    
    logger.info("Результаты тестирования бинарных терминаторов:")
    successful_terminators = []
    for term_hex, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        logger.info(f"'{term_hex}' - {status}")
        if success:
            successful_terminators.append(term_hex)
    
    if successful_terminators:
        logger.info("Успешные терминаторы:")
        for term_hex in successful_terminators:
            logger.info(f"  {term_hex}")
    else:
        logger.info("Ни один терминатор не сработал")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование бинарных терминаторов команд для шаттла")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт для отправки команд (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Команда для тестирования (по умолчанию STATUS)")
    parser.add_argument("--terminator", help="Конкретный терминатор для тестирования (в формате hex, например 0d0a)")
    
    args = parser.parse_args()
    
    # Запускаем слушатель на 8181
    listener_task = asyncio.create_task(start_listener())
    
    # Тестируем терминаторы
    if args.terminator:
        await test_binary_command(args.ip, args.port, args.command, args.terminator, fixed_length=20)
    else:
        await test_all_binary_terminators(args.ip, args.port, args.command)
    
    # Завершаем слушатель
    await asyncio.sleep(2)
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
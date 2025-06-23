#!/usr/bin/env python3
"""
Тестирование различных кодировок для команд шаттла
"""
import asyncio
import argparse
import sys
import logging
import binascii

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_encodings")

async def test_encoding(ip, port, command, encoding, terminator_hex="0d0a"):
    """Тестирует команду с указанной кодировкой и терминатором"""
    try:
        # Преобразуем hex-строку в байты
        terminator = binascii.unhexlify(terminator_hex)
        
        # Кодируем команду в указанной кодировке
        try:
            command_bytes = command.encode(encoding)
        except LookupError:
            logger.error(f"Неизвестная кодировка: {encoding}")
            return False
        
        # Формируем полную команду
        full_command = command_bytes + terminator
        
        logger.info(f"Тестирование команды '{command}' с кодировкой '{encoding}' и терминатором '{terminator_hex}'")
        logger.info(f"Полная команда (hex): {full_command.hex()}")
        
        reader, writer = await asyncio.open_connection(ip, port)
        
        writer.write(full_command)
        await writer.drain()
        
        try:
            # Ждем ответа с таймаутом
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            if data:
                response_hex = data.hex()
                try:
                    response_text = data.decode('utf-8', errors='replace')
                    logger.info(f"Получен ответ (text): '{response_text}'")
                except:
                    response_text = "Не удалось декодировать как текст"
                
                logger.info(f"Получен ответ (hex): {response_hex}")
                success = True
            else:
                logger.warning("Получен пустой ответ")
                success = False
        except asyncio.TimeoutError:
            logger.warning("Таймаут ожидания ответа")
            success = False
        
        writer.close()
        await writer.wait_closed()
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании кодировки: {e}")
        return False

async def test_all_encodings(ip, port, command="STATUS", terminator_hex="0d0a"):
    """Тестирует команду с различными кодировками"""
    encodings = [
        "utf-8",
        "ascii",
        "latin1",
        "cp1251",  # Windows-1251 (кириллица)
        "koi8-r",  # KOI8-R (кириллица)
        "cp866",   # DOS (кириллица)
        "iso8859-5", # ISO 8859-5 (кириллица)
        "utf-16",
        "utf-16-le",
        "utf-16-be",
        "utf-32",
        "utf-32-le",
        "utf-32-be",
    ]
    
    results = []
    for enc in encodings:
        success = await test_encoding(ip, port, command, enc, terminator_hex)
        results.append((enc, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
    logger.info(f"Результаты тестирования кодировок для команды '{command}' с терминатором '{terminator_hex}':")
    successful_encodings = []
    for enc, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        logger.info(f"'{enc}' - {status}")
        if success:
            successful_encodings.append(enc)
    
    if successful_encodings:
        logger.info("Успешные кодировки:")
        for enc in successful_encodings:
            logger.info(f"  '{enc}'")
    else:
        logger.info("Ни одна кодировка не сработала")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование кодировок для команд шаттла")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Команда для тестирования (по умолчанию STATUS)")
    parser.add_argument("--terminator", default="0d0a", help="Терминатор в формате hex (по умолчанию 0d0a для CRLF)")
    parser.add_argument("--encoding", help="Конкретная кодировка для тестирования")
    
    args = parser.parse_args()
    
    if args.encoding:
        await test_encoding(args.ip, args.port, args.command, args.encoding, args.terminator)
    else:
        await test_all_encodings(args.ip, args.port, args.command, args.terminator)
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
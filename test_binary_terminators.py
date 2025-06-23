#!/usr/bin/env python3
"""
Тестирование бинарных терминаторов команд для шаттла
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
logger = logging.getLogger("test_binary_terminators")

async def test_binary_command(ip, port, command, terminator_hex):
    """Тестирует команду с бинарным терминатором"""
    try:
        # Преобразуем hex-строку в байты
        terminator = binascii.unhexlify(terminator_hex)
        
        # Формируем полную команду
        full_command = command.encode() + terminator
        
        logger.info(f"Тестирование команды '{command}' с терминатором '{terminator_hex}'")
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
        logger.error(f"Ошибка при тестировании команды: {e}")
        return False

async def test_all_binary_terminators(ip, port, command="STATUS"):
    """Тестирует все возможные бинарные терминаторы команд"""
    terminators = [
        # Стандартные терминаторы
        "0a",           # LF (Unix)
        "0d0a",         # CRLF (Windows)
        "0d",           # CR (старые Mac)
        
        # Нулевые байты и комбинации
        "00",           # NULL
        "000a",         # NULL + LF
        "000d0a",       # NULL + CRLF
        "000d",         # NULL + CR
        "0a00",         # LF + NULL
        "0d0a00",       # CRLF + NULL
        "0d00",         # CR + NULL
        
        # Другие специальные символы
        "03",           # ETX (End of Text)
        "04",           # EOT (End of Transmission)
        "1a",           # SUB (Substitute, используется как EOF в некоторых системах)
        "1c",           # FS (File Separator)
        "1d",           # GS (Group Separator)
        "1e",           # RS (Record Separator)
        "1f",           # US (Unit Separator)
        
        # Комбинации с ETX/EOT
        "030a",         # ETX + LF
        "030d0a",       # ETX + CRLF
        "040a",         # EOT + LF
        "040d0a",       # EOT + CRLF
        
        # Двойные терминаторы
        "0a0a",         # LF + LF
        "0d0d",         # CR + CR
        "0d0a0d0a",     # CRLF + CRLF
        
        # Другие возможные комбинации
        "3b",           # ; (точка с запятой)
        "3b0a",         # ; + LF
        "3b0d0a",       # ; + CRLF
        "2c",           # , (запятая)
        "2c0a",         # , + LF
        "2c0d0a",       # , + CRLF
    ]
    
    results = []
    for term_hex in terminators:
        success = await test_binary_command(ip, port, command, term_hex)
        results.append((term_hex, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
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
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Команда для тестирования (по умолчанию STATUS)")
    parser.add_argument("--terminator", help="Конкретный терминатор для тестирования (в формате hex, например 0d0a)")
    
    args = parser.parse_args()
    
    if args.terminator:
        await test_binary_command(args.ip, args.port, args.command, args.terminator)
    else:
        await test_all_binary_terminators(args.ip, args.port, args.command)
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
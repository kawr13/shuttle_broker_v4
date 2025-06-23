#!/usr/bin/env python3
"""
Тестирование различных форматов команд для шаттла
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
logger = logging.getLogger("test_command_formats")

async def test_command_format(ip, port, command_format, terminator_hex="0d0a"):
    """Тестирует указанный формат команды с заданным терминатором"""
    try:
        # Преобразуем hex-строку в байты
        terminator = binascii.unhexlify(terminator_hex)
        
        # Формируем полную команду
        full_command = command_format.encode() + terminator
        
        logger.info(f"Тестирование формата: '{command_format}' с терминатором '{terminator_hex}'")
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
        logger.error(f"Ошибка при тестировании формата: {e}")
        return False

async def test_all_command_formats(ip, port, base_command="STATUS", terminator_hex="0d0a"):
    """Тестирует все возможные форматы команд с заданным терминатором"""
    formats = [
        # Базовая команда
        f"{base_command}",
        
        # С префиксами
        f"CMD:{base_command}",
        f"CMD {base_command}",
        f">{base_command}",
        f"#{base_command}",
        f"*{base_command}",
        f"@{base_command}",
        f"${base_command}",
        f"!{base_command}",
        f"/{base_command}",
        f"\\{base_command}",
        
        # С суффиксами
        f"{base_command}:",
        f"{base_command};",
        f"{base_command},",
        f"{base_command}.",
        f"{base_command}?",
        f"{base_command}!",
        f"{base_command}=",
        
        # С префиксами и суффиксами
        f"CMD:{base_command}:",
        f"CMD:{base_command};",
        f"CMD {base_command}:",
        f"CMD {base_command};",
        
        # С параметрами
        f"{base_command}:1",
        f"{base_command}=1",
        f"{base_command}-1",
        f"{base_command}_1",
        f"{base_command}/1",
        f"{base_command} 1",
        f"{base_command},1",
        
        # С префиксами и параметрами
        f"CMD:{base_command}:1",
        f"CMD:{base_command}=1",
        f"CMD {base_command}:1",
        f"CMD {base_command}=1",
        
        # Другие варианты
        f"GET {base_command}",
        f"SET {base_command}",
        f"REQ {base_command}",
        f"SEND {base_command}",
        f"EXEC {base_command}",
        f"RUN {base_command}",
        
        # Нижний регистр
        f"{base_command.lower()}",
        f"cmd:{base_command.lower()}",
        f"get {base_command.lower()}",
        
        # Смешанный регистр
        f"{base_command.title()}",
        f"Cmd:{base_command.title()}",
        f"Get {base_command.title()}",
    ]
    
    results = []
    for fmt in formats:
        success = await test_command_format(ip, port, fmt, terminator_hex)
        results.append((fmt, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
    logger.info(f"Результаты тестирования форматов команд с терминатором '{terminator_hex}':")
    successful_formats = []
    for fmt, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        logger.info(f"'{fmt}' - {status}")
        if success:
            successful_formats.append(fmt)
    
    if successful_formats:
        logger.info("Успешные форматы:")
        for fmt in successful_formats:
            logger.info(f"  '{fmt}'")
    else:
        logger.info("Ни один формат не сработал")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование форматов команд для шаттла")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Базовая команда для тестирования (по умолчанию STATUS)")
    parser.add_argument("--terminator", default="0d0a", help="Терминатор в формате hex (по умолчанию 0d0a для CRLF)")
    parser.add_argument("--format", help="Конкретный формат команды для тестирования")
    
    args = parser.parse_args()
    
    if args.format:
        await test_command_format(args.ip, args.port, args.format, args.terminator)
    else:
        await test_all_command_formats(args.ip, args.port, args.command, args.terminator)
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
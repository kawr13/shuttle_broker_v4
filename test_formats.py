#!/usr/bin/env python3
"""
Тестирование различных форматов команд для шаттла
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
logger = logging.getLogger("test_formats")

async def test_format(ip, port, command_format):
    """Тестирует указанный формат команды"""
    try:
        logger.info(f"Тестирование формата: {command_format}")
        reader, writer = await asyncio.open_connection(ip, port)
        
        writer.write(command_format.encode())
        await writer.drain()
        
        try:
            data = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=5.0)
            response = data.decode().strip()
            logger.info(f"Получен ответ: {response}")
            success = True
        except asyncio.TimeoutError:
            logger.warning("Таймаут ожидания ответа")
            success = False
        
        writer.close()
        await writer.wait_closed()
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании формата: {e}")
        return False

async def test_all_formats(ip, port):
    """Тестирует все возможные форматы команд"""
    formats = [
        "STATUS\n",                # Простая команда
        "STATUS\r\n",              # С CR+LF
        "STATUS;",                 # С точкой с запятой
        "STATUS;\n",               # С точкой с запятой и переводом строки
        "STATUS=\n",               # С равно
        "STATUS=?\n",              # С запросом
        "STATUS?\n",               # Только запрос
        "GET STATUS\n",            # С GET
        "CMD STATUS\n",            # С CMD
        "COMMAND STATUS\n",        # С COMMAND
        "REQ STATUS\n",            # С REQ
        "@STATUS\n",               # С @
        "#STATUS\n",               # С #
        "$STATUS\n",               # С $
        "*STATUS\n",               # С *
        "STATUS:?\n",              # С двоеточием и запросом
        "STATUS:REQ\n",            # С двоеточием и REQ
        "STATUS:GET\n",            # С двоеточием и GET
        "STATUS:CMD\n",            # С двоеточием и CMD
        "STATUS:COMMAND\n",        # С двоеточием и COMMAND
        "STATUS:1\n",              # С двоеточием и числом
        "STATUS 1\n",              # С пробелом и числом
        "STATUS,1\n",              # С запятой и числом
        "STATUS-1\n",              # С дефисом и числом
        "STATUS_1\n",              # С подчеркиванием и числом
        "STATUS/1\n",              # С слешем и числом
        "STATUS\\1\n",             # С обратным слешем и числом
        "STATUS.1\n",              # С точкой и числом
        "STATUS+1\n",              # С плюсом и числом
        "STATUS=1\n",              # С равно и числом
        "STATUS:?\r\n",            # С CR+LF
        "STATUS:REQ\r\n",          # С CR+LF
        "STATUS:GET\r\n",          # С CR+LF
        "STATUS:CMD\r\n",          # С CR+LF
        "STATUS:COMMAND\r\n",      # С CR+LF
        "STATUS:1\r\n",            # С CR+LF
        "STATUS 1\r\n",            # С CR+LF
        "STATUS,1\r\n",            # С CR+LF
        "STATUS-1\r\n",            # С CR+LF
        "STATUS_1\r\n",            # С CR+LF
        "STATUS/1\r\n",            # С CR+LF
        "STATUS\\1\r\n",           # С CR+LF
        "STATUS.1\r\n",            # С CR+LF
        "STATUS+1\r\n",            # С CR+LF
        "STATUS=1\r\n",            # С CR+LF
    ]
    
    results = []
    for fmt in formats:
        success = await test_format(ip, port, fmt)
        results.append((fmt, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
    logger.info("Результаты тестирования форматов:")
    successful_formats = []
    for fmt, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        logger.info(f"{fmt.strip()} - {status}")
        if success:
            successful_formats.append(fmt)
    
    if successful_formats:
        logger.info("Успешные форматы:")
        for fmt in successful_formats:
            logger.info(f"  {fmt.strip()}")
    else:
        logger.info("Ни один формат не сработал")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование форматов команд для шаттла")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    
    args = parser.parse_args()
    
    await test_all_formats(args.ip, args.port)
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
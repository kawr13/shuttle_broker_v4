#!/usr/bin/env python3
"""
Тестирование различных терминаторов команд для шаттла
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
logger = logging.getLogger("test_terminators")

async def test_terminator(ip, port, command, terminator):
    """Тестирует указанный терминатор команды"""
    try:
        full_command = f"{command}{terminator}"
        logger.info(f"Тестирование команды '{command}' с терминатором '{terminator.encode().hex()}'")
        reader, writer = await asyncio.open_connection(ip, port)
        
        writer.write(full_command.encode())
        await writer.drain()
        
        try:
            # Ждем ответа с таймаутом
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            response = data.decode().strip()
            logger.info(f"Получен ответ: '{response}'")
            success = True
        except asyncio.TimeoutError:
            logger.warning("Таймаут ожидания ответа")
            success = False
        
        writer.close()
        await writer.wait_closed()
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании терминатора: {e}")
        return False

async def test_all_terminators(ip, port, command="STATUS"):
    """Тестирует все возможные терминаторы команд"""
    terminators = [
        "\n",           # LF (Unix)
        "\r\n",         # CRLF (Windows)
        "\r",           # CR (старые Mac)
        ";",            # Точка с запятой
        ";\n",          # Точка с запятой + LF
        ";\r\n",        # Точка с запятой + CRLF
        ";\r",          # Точка с запятой + CR
        "",             # Без терминатора
        " ",            # Пробел
        "\t",           # Табуляция
        "\t\n",         # Табуляция + LF
        "\t\r\n",       # Табуляция + CRLF
        " \n",          # Пробел + LF
        " \r\n",        # Пробел + CRLF
        "\n\n",         # Двойной LF
        "\r\n\r\n",     # Двойной CRLF
    ]
    
    results = []
    for term in terminators:
        success = await test_terminator(ip, port, command, term)
        results.append((term, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
    logger.info("Результаты тестирования терминаторов:")
    successful_terminators = []
    for term, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        term_hex = term.encode().hex()
        logger.info(f"'{term_hex}' - {status}")
        if success:
            successful_terminators.append(term)
    
    if successful_terminators:
        logger.info("Успешные терминаторы:")
        for term in successful_terminators:
            term_hex = term.encode().hex()
            term_desc = {
                "\n": "LF (\\n)",
                "\r\n": "CRLF (\\r\\n)",
                "\r": "CR (\\r)",
                ";": "Точка с запятой (;)",
                ";\n": "Точка с запятой + LF (;\\n)",
                ";\r\n": "Точка с запятой + CRLF (;\\r\\n)",
                ";\r": "Точка с запятой + CR (;\\r)",
                "": "Без терминатора",
                " ": "Пробел",
                "\t": "Табуляция (\\t)",
                "\t\n": "Табуляция + LF (\\t\\n)",
                "\t\r\n": "Табуляция + CRLF (\\t\\r\\n)",
                " \n": "Пробел + LF ( \\n)",
                " \r\n": "Пробел + CRLF ( \\r\\n)",
                "\n\n": "Двойной LF (\\n\\n)",
                "\r\n\r\n": "Двойной CRLF (\\r\\n\\r\\n)",
            }.get(term, f"Неизвестный ({term_hex})")
            logger.info(f"  {term_desc}")
    else:
        logger.info("Ни один терминатор не сработал")

async def test_commands_with_params(ip, port, terminator):
    """Тестирует команды с параметрами, используя указанный терминатор"""
    commands = [
        "STATUS",
        "BATTERY",
        "PALLET_IN-A1",
        "PALLET_OUT-A1",
        "FIFO-001",
        "FILO-001",
        "HOME",
        "COUNT",
        "MRCD"
    ]
    
    results = []
    for cmd in commands:
        success = await test_terminator(ip, port, cmd, terminator)
        results.append((cmd, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
    logger.info(f"Результаты тестирования команд с терминатором '{terminator.encode().hex()}':")
    for cmd, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        logger.info(f"{cmd} - {status}")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование терминаторов команд для шаттла")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Команда для тестирования (по умолчанию STATUS)")
    parser.add_argument("--test-commands", action="store_true", help="Тестировать различные команды с найденным терминатором")
    
    args = parser.parse_args()
    
    await test_all_terminators(args.ip, args.port, args.command)
    
    if args.test_commands:
        # Спрашиваем пользователя, какой терминатор использовать для тестирования команд
        print("\nВведите терминатор для тестирования команд (в формате hex, например 0d0a для \\r\\n):")
        terminator_hex = input("> ")
        try:
            terminator = bytes.fromhex(terminator_hex).decode()
            await test_commands_with_params(args.ip, args.port, terminator)
        except Exception as e:
            logger.error(f"Ошибка при декодировании терминатора: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
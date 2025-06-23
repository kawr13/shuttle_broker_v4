#!/usr/bin/env python3
"""
Тестирование гибкого класса ShuttleCommand
"""
import asyncio
import argparse
import sys
import logging
import binascii
from shuttle_module.commands_flexible import ShuttleCommand, ShuttleCommandEnum

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_flexible_command")

async def send_command(ip, port, command, wait_time=5.0):
    """Отправляет команду шаттлу и ждет ответа"""
    try:
        # Получаем байты команды
        command_bytes = command.to_bytes()
        
        logger.info(f"Отправка команды (hex): {command_bytes.hex()}")
        logger.info(f"Отправка команды (str): {command.to_string()}")
        
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
                
                return True
            else:
                logger.warning("Получен пустой ответ")
                return False
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут ожидания ответа ({wait_time} секунд)")
            return False
        
    except ConnectionRefusedError:
        logger.error(f"Соединение отклонено {ip}:{port}")
        return False
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return False
    finally:
        # Закрываем соединение
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass

async def test_all_formats(ip, port, command_type=ShuttleCommandEnum.STATUS, params=None):
    """Тестирует все комбинации форматов команд"""
    # Список терминаторов для тестирования
    terminators = ["LF", "CRLF", "CR", "NULL", "NONE", "ETX", "EOT"]
    
    # Список префиксов для тестирования
    prefixes = ["", "CMD:", "CMD ", ">", "#", "*", "@", "$", "!", "/", "\\"]
    
    # Список разделителей для тестирования
    separators = ["-", ":", "=", " ", ",", ".", "/", "_"]
    
    # Список суффиксов для тестирования
    suffixes = ["", ":", ";", ",", ".", "?", "!", "="]
    
    # Список кодировок для тестирования
    encodings = ["utf-8", "ascii", "latin1"]
    
    # Тестируем только терминаторы
    logger.info("=== Тестирование терминаторов ===")
    for term in terminators:
        command = ShuttleCommand(
            command_type=command_type,
            shuttle_id="test",
            params=params,
            terminator=term
        )
        
        logger.info(f"Тестирование терминатора: {term}")
        success = await send_command(ip, port, command)
        
        if success:
            logger.info(f"Терминатор {term} УСПЕШНО")
        else:
            logger.info(f"Терминатор {term} НЕУДАЧНО")
        
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Тестируем только префиксы с успешным терминатором
    logger.info("=== Тестирование префиксов ===")
    for prefix in prefixes:
        command = ShuttleCommand(
            command_type=command_type,
            shuttle_id="test",
            params=params,
            prefix=prefix,
            terminator="CRLF"  # Используем CRLF как наиболее вероятный успешный терминатор
        )
        
        logger.info(f"Тестирование префикса: '{prefix}'")
        success = await send_command(ip, port, command)
        
        if success:
            logger.info(f"Префикс '{prefix}' УСПЕШНО")
        else:
            logger.info(f"Префикс '{prefix}' НЕУДАЧНО")
        
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Если есть параметры, тестируем разделители
    if params:
        logger.info("=== Тестирование разделителей ===")
        for sep in separators:
            command = ShuttleCommand(
                command_type=command_type,
                shuttle_id="test",
                params=params,
                separator=sep,
                terminator="CRLF"  # Используем CRLF как наиболее вероятный успешный терминатор
            )
            
            logger.info(f"Тестирование разделителя: '{sep}'")
            success = await send_command(ip, port, command)
            
            if success:
                logger.info(f"Разделитель '{sep}' УСПЕШНО")
            else:
                logger.info(f"Разделитель '{sep}' НЕУДАЧНО")
            
            # Пауза между запросами
            await asyncio.sleep(1)
    
    # Тестируем только суффиксы
    logger.info("=== Тестирование суффиксов ===")
    for suffix in suffixes:
        command = ShuttleCommand(
            command_type=command_type,
            shuttle_id="test",
            params=params,
            suffix=suffix,
            terminator="CRLF"  # Используем CRLF как наиболее вероятный успешный терминатор
        )
        
        logger.info(f"Тестирование суффикса: '{suffix}'")
        success = await send_command(ip, port, command)
        
        if success:
            logger.info(f"Суффикс '{suffix}' УСПЕШНО")
        else:
            logger.info(f"Суффикс '{suffix}' НЕУДАЧНО")
        
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Тестируем только кодировки
    logger.info("=== Тестирование кодировок ===")
    for enc in encodings:
        command = ShuttleCommand(
            command_type=command_type,
            shuttle_id="test",
            params=params,
            encoding=enc,
            terminator="CRLF"  # Используем CRLF как наиболее вероятный успешный терминатор
        )
        
        logger.info(f"Тестирование кодировки: {enc}")
        success = await send_command(ip, port, command)
        
        if success:
            logger.info(f"Кодировка {enc} УСПЕШНО")
        else:
            logger.info(f"Кодировка {enc} НЕУДАЧНО")
        
        # Пауза между запросами
        await asyncio.sleep(1)

async def test_raw_command(ip, port, hex_command):
    """Тестирует отправку сырой команды"""
    try:
        # Преобразуем hex-строку в байты
        raw_bytes = binascii.unhexlify(hex_command)
        
        # Создаем команду из сырых данных
        command = ShuttleCommand.from_raw(raw_bytes, "test")
        
        logger.info(f"Тестирование сырой команды (hex): {hex_command}")
        success = await send_command(ip, port, command)
        
        if success:
            logger.info("Сырая команда УСПЕШНО")
        else:
            logger.info("Сырая команда НЕУДАЧНО")
        
    except binascii.Error:
        logger.error("Неверный формат шестнадцатеричной строки")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование гибкого класса ShuttleCommand")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--command", default="STATUS", help="Тип команды (по умолчанию STATUS)")
    parser.add_argument("--params", help="Параметры команды")
    parser.add_argument("--raw", help="Сырая команда в шестнадцатеричном формате")
    parser.add_argument("--wait", type=float, default=5.0, help="Время ожидания ответа в секундах (по умолчанию 5.0)")
    
    args = parser.parse_args()
    
    if args.raw:
        await test_raw_command(args.ip, args.port, args.raw)
    else:
        try:
            command_type = ShuttleCommandEnum[args.command]
        except KeyError:
            logger.error(f"Неизвестный тип команды: {args.command}")
            logger.info(f"Доступные типы команд: {', '.join([cmd.name for cmd in ShuttleCommandEnum])}")
            return 1
        
        await test_all_formats(args.ip, args.port, command_type, args.params)
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
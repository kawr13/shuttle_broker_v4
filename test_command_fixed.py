#!/usr/bin/env python3
"""
Простой тестовый клиент для отправки команд шаттлу напрямую
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
logger = logging.getLogger("test_command")

async def send_command(ip, port, command, params=None):
    """Отправляет команду шаттлу и ждет ответа"""
    try:
        # Формируем команду
        if params:
            if command in ["FIFO", "FILO"]:
                try:
                    param_value = int(params)
                    command_str = f"{command}-{param_value:03d}\n"
                except ValueError:
                    command_str = f"{command}-{params}\n"
            else:
                command_str = f"{command}-{params}\n"
        else:
            command_str = f"{command}\n"
        
        logger.info(f"Подключение к шаттлу {ip}:{port}")
        reader, writer = await asyncio.open_connection(ip, port)
        
        logger.info(f"Отправка команды: {command_str.strip()}")
        writer.write(command_str.encode())
        await writer.drain()
        
        # Ждем ответ
        logger.info("Ожидание ответа...")
        try:
            # Используем read вместо readuntil
            data = await asyncio.wait_for(reader.read(1024), timeout=10.0)
            if data:
                try:
                    response = data.decode('utf-8').strip()
                    logger.info(f"Получен ответ: {response}")
                except UnicodeDecodeError:
                    logger.info(f"Получен бинарный ответ: {data.hex()}")
                
                # Отправляем MRCD в ответ
                if response and response != "MRCD":
                    mrcd = "MRCD\n"
                    logger.info(f"Отправка MRCD в ответ")
                    writer.write(mrcd.encode())
                    await writer.drain()
                    
                    # Ждем еще данные после отправки MRCD
                    try:
                        more_data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
                        if more_data:
                            try:
                                more_response = more_data.decode('utf-8').strip()
                                logger.info(f"Получен дополнительный ответ: {more_response}")
                            except UnicodeDecodeError:
                                logger.info(f"Получен дополнительный бинарный ответ: {more_data.hex()}")
                    except asyncio.TimeoutError:
                        pass
            else:
                logger.warning("Получен пустой ответ")
                
        except asyncio.TimeoutError:
            logger.warning("Таймаут ожидания ответа")
        
        # Закрываем соединение
        writer.close()
        await writer.wait_closed()
        logger.info("Соединение закрыто")
        
        return True
    except ConnectionRefusedError:
        logger.error(f"Не удалось подключиться к {ip}:{port}")
        return False
    except Exception as e:
        logger.error(f"Ошибка при отправке команды: {e}")
        return False

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестовый клиент для отправки команд шаттлу")
    parser.add_argument("ip", help="IP-адрес шаттла")
    parser.add_argument("command", help="Команда (STATUS, BATTERY, HOME, и т.д.)")
    parser.add_argument("--port", type=int, default=2000, help="Порт шаттла (по умолчанию 2000)")
    parser.add_argument("--params", help="Параметры команды")
    
    args = parser.parse_args()
    
    success = await send_command(args.ip, args.port, args.command, args.params)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
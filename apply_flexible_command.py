#!/usr/bin/env python3
"""
Скрипт для применения гибкого класса ShuttleCommand
"""
import os
import shutil
import logging
import argparse
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("apply_flexible_command")

def apply_flexible_command(terminator="CRLF", prefix="", separator="-", suffix="", encoding="utf-8"):
    """Применяет гибкий класс ShuttleCommand с указанными параметрами"""
    try:
        # Путь к файлам
        commands_path = os.path.join("shuttle_module", "commands.py")
        commands_flexible_path = os.path.join("shuttle_module", "commands_flexible.py")
        commands_backup_path = os.path.join("shuttle_module", "commands.py.bak")
        
        # Проверяем, существуют ли файлы
        if not os.path.exists(commands_path):
            logger.error(f"Файл {commands_path} не найден")
            return False
        
        if not os.path.exists(commands_flexible_path):
            logger.error(f"Файл {commands_flexible_path} не найден")
            return False
        
        # Создаем резервную копию
        logger.info(f"Создание резервной копии {commands_path} -> {commands_backup_path}")
        shutil.copy2(commands_path, commands_backup_path)
        
        # Читаем содержимое гибкого класса
        with open(commands_flexible_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Устанавливаем параметры по умолчанию
        content = content.replace('DEFAULT_TERMINATOR = "CRLF"', f'DEFAULT_TERMINATOR = "{terminator}"')
        content = content.replace('DEFAULT_PREFIX = ""', f'DEFAULT_PREFIX = "{prefix}"')
        content = content.replace('DEFAULT_SEPARATOR = "-"', f'DEFAULT_SEPARATOR = "{separator}"')
        content = content.replace('DEFAULT_SUFFIX = ""', f'DEFAULT_SUFFIX = "{suffix}"')
        content = content.replace('DEFAULT_ENCODING = "utf-8"', f'DEFAULT_ENCODING = "{encoding}"')
        
        # Записываем модифицированный класс в основной файл
        with open(commands_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Гибкий класс ShuttleCommand успешно применен с параметрами:")
        logger.info(f"  Терминатор: {terminator}")
        logger.info(f"  Префикс: '{prefix}'")
        logger.info(f"  Разделитель: '{separator}'")
        logger.info(f"  Суффикс: '{suffix}'")
        logger.info(f"  Кодировка: {encoding}")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при применении гибкого класса ShuttleCommand: {e}")
        return False

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Применение гибкого класса ShuttleCommand")
    parser.add_argument("--terminator", default="CRLF", choices=["LF", "CRLF", "CR", "NULL", "NONE", "ETX", "EOT"],
                        help="Терминатор команды (по умолчанию CRLF)")
    parser.add_argument("--prefix", default="", help="Префикс команды (по умолчанию пусто)")
    parser.add_argument("--separator", default="-", help="Разделитель команды и параметров (по умолчанию -)")
    parser.add_argument("--suffix", default="", help="Суффикс команды (по умолчанию пусто)")
    parser.add_argument("--encoding", default="utf-8", help="Кодировка команды (по умолчанию utf-8)")
    
    args = parser.parse_args()
    
    success = apply_flexible_command(
        terminator=args.terminator,
        prefix=args.prefix,
        separator=args.separator,
        suffix=args.suffix,
        encoding=args.encoding
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
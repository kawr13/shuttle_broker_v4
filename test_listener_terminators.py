#!/usr/bin/env python3
"""
Тестирование отправки команд с разными терминаторами через ShuttleListener
"""
import asyncio
import argparse
import sys
import logging
from shuttle_module.shuttle_listener import get_shuttle_listener

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_listener_terminators")

async def test_send_message(shuttle_id, message, terminator):
    """Тестирует отправку сообщения через ShuttleListener с указанным терминатором"""
    try:
        # Получаем экземпляр ShuttleListener
        shuttle_listener = get_shuttle_listener()
        
        # Формируем сообщение с указанным терминатором
        full_message = message
        if terminator:
            # Удаляем существующие терминаторы
            full_message = full_message.rstrip('\r\n')
            full_message += terminator
        
        logger.info(f"Отправка сообщения '{full_message.encode().hex()}' шаттлу {shuttle_id}")
        
        # Отправляем сообщение
        success = await shuttle_listener.send_message(shuttle_id, full_message)
        
        if success:
            logger.info(f"Сообщение успешно отправлено шаттлу {shuttle_id}")
        else:
            logger.error(f"Ошибка при отправке сообщения шаттлу {shuttle_id}")
        
        return success
    except Exception as e:
        logger.error(f"Ошибка при тестировании отправки сообщения: {e}")
        return False

async def test_all_terminators(shuttle_id, message="STATUS"):
    """Тестирует все возможные терминаторы команд"""
    terminators = [
        "\n",           # LF (Unix)
        "\r\n",         # CRLF (Windows)
        "\r",           # CR (старые Mac)
        "",             # Без терминатора (ShuttleListener должен добавить)
    ]
    
    results = []
    for term in terminators:
        success = await test_send_message(shuttle_id, message, term)
        results.append((term, success))
        # Пауза между запросами
        await asyncio.sleep(1)
    
    # Выводим результаты
    logger.info("Результаты тестирования терминаторов:")
    successful_terminators = []
    for term, success in results:
        status = "УСПЕШНО" if success else "НЕУДАЧНО"
        term_hex = term.encode().hex()
        term_desc = {
            "\n": "LF (\\n)",
            "\r\n": "CRLF (\\r\\n)",
            "\r": "CR (\\r)",
            "": "Без терминатора",
        }.get(term, f"Неизвестный ({term_hex})")
        logger.info(f"{term_desc} - {status}")
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
                "": "Без терминатора",
            }.get(term, f"Неизвестный ({term_hex})")
            logger.info(f"  {term_desc}")
    else:
        logger.info("Ни один терминатор не сработал")

async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Тестирование отправки команд с разными терминаторами через ShuttleListener")
    parser.add_argument("shuttle_id", help="ID шаттла")
    parser.add_argument("--message", default="STATUS", help="Сообщение для отправки (по умолчанию STATUS)")
    parser.add_argument("--terminator", help="Конкретный терминатор для тестирования (в формате hex, например 0d0a для \\r\\n)")
    
    args = parser.parse_args()
    
    # Запускаем ShuttleListener, если он еще не запущен
    shuttle_listener = get_shuttle_listener()
    await shuttle_listener.start()
    
    if args.terminator:
        try:
            terminator = bytes.fromhex(args.terminator).decode()
            await test_send_message(args.shuttle_id, args.message, terminator)
        except Exception as e:
            logger.error(f"Ошибка при декодировании терминатора: {e}")
    else:
        await test_all_terminators(args.shuttle_id, args.message)
    
    # Останавливаем ShuttleListener
    await shuttle_listener.stop()
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
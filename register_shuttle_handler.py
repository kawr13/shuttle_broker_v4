#!/usr/bin/env python3
"""
Скрипт для регистрации обработчиков сообщений для шаттлов
"""
import asyncio
import sys
import yaml
import argparse

async def register_shuttle_handler(shuttle_id):
    """Регистрирует обработчик сообщений для шаттла"""
    print(f"🔄 Регистрация обработчика для шаттла {shuttle_id}...")
    
    # Импортируем необходимые модули
    try:
        # Импортируем менеджер шаттлов
        from shuttle_module.shuttle_manager import get_shuttle_manager
        shuttle_manager = get_shuttle_manager()
        
        # Импортируем слушатель шаттлов
        from shuttle_module.shuttle_listener import get_shuttle_listener
        shuttle_listener = get_shuttle_listener()
        
        # Импортируем клиент шаттла
        from shuttle_module.shuttle_client import ShuttleClient
        from core.config import get_config, ShuttleConfig
        
        # Получаем конфигурацию
        config = get_config()
        
        # Проверяем, существует ли шаттл в конфигурации
        if shuttle_id not in config.shuttles:
            print(f"❌ Шаттл {shuttle_id} не найден в конфигурации")
            return False
        
        # Получаем конфигурацию шаттла
        shuttle_config = config.shuttles[shuttle_id]
        
        # Создаем клиент шаттла
        shuttle_client = ShuttleClient(shuttle_id, shuttle_config)
        
        # Добавляем шаттл в менеджер, если его там нет
        if shuttle_id not in shuttle_manager.shuttles:
            await shuttle_manager.add_shuttle(shuttle_id, shuttle_config)
            print(f"✅ Шаттл {shuttle_id} добавлен в менеджер шаттлов")
        
        # Регистрируем обработчик сообщений
        shuttle_listener.register_message_handler(
            shuttle_id, 
            shuttle_manager.shuttles[shuttle_id]._process_message_from_listener
        )
        print(f"✅ Обработчик сообщений для шаттла {shuttle_id} зарегистрирован")
        
        # Запрашиваем статус шаттла
        try:
            await shuttle_listener.send_message(shuttle_id, "STATUS\r\n")
            print(f"✅ Запрошен статус шаттла {shuttle_id}")
        except Exception as e:
            print(f"❌ Ошибка при запросе статуса шаттла {shuttle_id}: {e}")
        
        return True
    except Exception as e:
        print(f"❌ Ошибка при регистрации обработчика: {e}")
        return False

async def main():
    parser = argparse.ArgumentParser(description='Регистрация обработчиков сообщений для шаттлов')
    parser.add_argument('shuttle_id', help='ID шаттла (например: shuttle_135)')
    
    args = parser.parse_args()
    
    # Инициализируем необходимые компоненты
    from core.config import load_config
    load_config()
    
    # Запускаем слушатель шаттлов, если он еще не запущен
    from shuttle_module.shuttle_listener import get_shuttle_listener
    shuttle_listener = get_shuttle_listener()
    await shuttle_listener.start()
    
    # Запускаем менеджер шаттлов, если он еще не запущен
    from shuttle_module.shuttle_manager import get_shuttle_manager
    shuttle_manager = get_shuttle_manager()
    await shuttle_manager.start()
    
    # Регистрируем обработчик
    success = await register_shuttle_handler(args.shuttle_id)
    
    if success:
        print(f"✅ Обработчик для шаттла {args.shuttle_id} успешно зарегистрирован")
        
        # Ждем некоторое время для получения сообщений
        print("⏳ Ожидание сообщений от шаттла (10 секунд)...")
        await asyncio.sleep(10)
    else:
        print(f"❌ Не удалось зарегистрировать обработчик для шаттла {args.shuttle_id}")
    
    return 0 if success else 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
#!/usr/bin/env python3
"""
Простой скрипт для отправки команды шаттлу
Использование: python send_command.py shuttle_140 STATUS
"""
import asyncio
import sys
import yaml

async def send_command_to_shuttle(shuttle_id: str, command: str):
    """Отправляет команду шаттлу и слушает ответ"""
    
    # Загружаем конфигурацию
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        if shuttle_id not in config['shuttles']:
            print(f"❌ Шаттл {shuttle_id} не найден в конфигурации")
            return
        
        shuttle_config = config['shuttles'][shuttle_id]
        shuttle_ip = shuttle_config['host']
        command_port = shuttle_config.get('command_port', 2000)
        
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        return
    
    print(f"📤 Отправляем команду '{command}' шаттлу {shuttle_id} ({shuttle_ip}:{command_port})")
    
    # Отправляем команду
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(shuttle_ip, command_port),
            timeout=5.0
        )
        
        # Добавляем терминатор
        if not command.endswith('\r\n'):
            command = command.rstrip('\n') + '\r\n'
        
        writer.write(command.encode('utf-8'))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        
        print(f"✅ Команда отправлена")
        
    except Exception as e:
        print(f"❌ Ошибка отправки команды: {e}")
        return
    
    # Слушаем ответ
    print(f"👂 Слушаем ответ на порту 8181...")
    
    response_received = False
    
    async def handle_connection(reader, writer):
        nonlocal response_received
        peer_name = writer.get_extra_info('peername')
        client_ip = peer_name[0] if peer_name else 'unknown'
        
        # Проверяем IP шаттла
        if client_ip != shuttle_ip:
            writer.close()
            await writer.wait_closed()
            return
        
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            if data:
                message = data.decode('utf-8').strip()
                print(f"📨 Ответ от {shuttle_id}: '{message}'")
                response_received = True
        except Exception as e:
            print(f"❌ Ошибка чтения ответа: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    # Запускаем сервер для прослушивания
    server = await asyncio.start_server(handle_connection, '0.0.0.0', 8181)
    
    try:
        # Ждем ответ 10 секунд
        for i in range(100):
            if response_received:
                break
            await asyncio.sleep(0.1)
        
        if not response_received:
            print("⏰ Таймаут ожидания ответа")
            
    finally:
        server.close()
        await server.wait_closed()

async def main():
    if len(sys.argv) != 3:
        print("Использование: python send_command.py <shuttle_id> <command>")
        print("Пример: python send_command.py shuttle_140 STATUS")
        return 1
    
    shuttle_id = sys.argv[1]
    command = sys.argv[2]
    
    await send_command_to_shuttle(shuttle_id, command)
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Прервано")
        sys.exit(1)
#!/usr/bin/env python3
"""
Скрипт для тестирования шаттлов
"""
import asyncio
import sys
from shuttle.shuttle_client import ShuttleClient

async def test_shuttle_command(ip: str, command: str):
    """Тестировать команду шаттла"""
    client = ShuttleClient("shuttles.json")
    
    print(f"Отправка команды '{command}' шаттлу {ip}")
    success = await client.send_command(ip, command)
    
    if success:
        print("✅ Команда отправлена успешно")
    else:
        print("❌ Ошибка отправки команды")

async def main():
    if len(sys.argv) < 3:
        print("Использование: python test_shuttle.py <IP> <КОМАНДА>")
        print("Пример: python test_shuttle.py 10.181.80.135 STATUS")
        return
    
    ip = sys.argv[1]
    command = " ".join(sys.argv[2:])
    
    await test_shuttle_command(ip, command)

if __name__ == "__main__":
    asyncio.run(main())
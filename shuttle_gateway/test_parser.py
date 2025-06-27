#!/usr/bin/env python3
"""Тест парсера сообщений шаттлов"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from shuttle.shuttle_client import ShuttleClient

def test_parser():
    client = ShuttleClient("shuttles.json")
    
    test_messages = [
        "PALLET_IN_DONE",
        "FILO-040_DONE", 
        "STATUS=NOT_READY",
        "LOC=NONE_RFID",
        "COMMAND STATUS TASK123",
        "SIMPLE_MESSAGE"
    ]
    
    print("🧪 Тестирование парсера сообщений шаттлов:")
    print("-" * 50)
    
    for msg in test_messages:
        result = client.parse_shuttle_response(msg, "test_ip")
        if result:
            command, status, task_id = result
            print(f"✅ '{msg}' → command: {command}, status: {status}, task_id: {task_id}")
        else:
            print(f"❌ '{msg}' → Не удалось распарсить")

if __name__ == "__main__":
    test_parser()
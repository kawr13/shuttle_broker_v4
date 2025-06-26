#!/usr/bin/env python3
"""
Скрипт для проверки URL WMS API
"""
import asyncio
import aiohttp
import base64
from wms_module.wms_client import WmsClient
from core.config import get_config

async def test_wms_url():
    """Тестирует URL WMS API"""
    print("🔍 Проверка URL WMS API")
    print("=" * 60)
    
    # Получаем конфигурацию
    config = get_config()
    if not config.wms:
        print("❌ WMS не настроена в конфигурации")
        return
    
    # Создаем клиент WMS
    client = WmsClient()
    
    print(f"📋 Конфигурация:")
    print(f"  URL в config.yaml: {config.wms.api_url}")
    print(f"  URL в WmsClient: {client.api_url}")
    print()
    
    # Формируем заголовок авторизации
    auth_header = client._get_auth_header()
    
    # Тестируем URL
    url = f"{client.api_url}?action=IncomeApi.getObjectsShipment"
    print(f"🧪 Тестирование URL: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=auth_header, timeout=10) as response:
                print(f"  📊 Статус: {response.status}")
                print(f"  📋 Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                
                # Проверяем тип контента
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type.lower():
                    try:
                        data = await response.json()
                        print(f"  ✅ JSON ответ получен")
                        print(f"  📄 Тип данных: {type(data)}")
                        if isinstance(data, dict):
                            print(f"  🔑 Ключи: {list(data.keys())}")
                    except Exception as e:
                        print(f"  ❌ Ошибка парсинга JSON: {e}")
                else:
                    text = await response.text()
                    print(f"  ⚠️ Получен не JSON ответ: {content_type}")
                    print(f"  📄 Первые 200 символов: {text[:200]}")
    except Exception as e:
        print(f"  ❌ Ошибка запроса: {e}")
    
    print()
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_wms_url())
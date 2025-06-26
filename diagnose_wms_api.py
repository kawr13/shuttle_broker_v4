#!/usr/bin/env python3
"""
Скрипт для диагностики WMS API
"""
import asyncio
import aiohttp
import base64
import json
import sys
from core.config import get_config

async def diagnose_wms_api():
    """Диагностирует проблемы с WMS API"""
    print("🔍 Диагностика WMS API")
    print("=" * 60)
    
    # Получаем конфигурацию
    config = get_config()
    if not config.wms:
        print("❌ WMS не настроена в конфигурации")
        return
    
    wms_config = config.wms
    auth_string = f"{wms_config.username}:{wms_config.password}"
    auth_header = f"Basic {base64.b64encode(auth_string.encode()).decode()}"
    
    print(f"🌐 URL: {wms_config.api_url}")
    print(f"👤 Пользователь: {wms_config.username}")
    print(f"🔑 Пароль: {'*' * len(wms_config.password)}")
    print(f"⏱️ Интервал опроса: {wms_config.poll_interval} сек")
    print()
    
    # Тестируем различные варианты URL
    test_urls = [
        wms_config.api_url,
        wms_config.api_url.rstrip('/exec'),
        "http://10.181.80.28:8080/exec",
        "http://10.181.80.28:8080/"
    ]
    
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(test_urls, 1):
            print(f"🧪 Тест {i}: {url}")
            
            try:
                # Простой GET запрос
                async with session.get(url, headers={"Authorization": auth_header}) as response:
                    print(f"   📊 Статус: {response.status}")
                    print(f"   📋 Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                    
                    # Читаем первые 200 символов ответа
                    text = await response.text()
                    preview = text[:200].replace('\n', '\\n')
                    print(f"   📄 Превью ответа: {preview}...")
                    
                # Запрос с действием
                action_url = f"{url}{'?' if '?' not in url else '&'}action=IncomeApi.getObjectsShipment"
                print(f"   🔍 Тест с действием: {action_url}")
                
                async with session.get(action_url, headers={"Authorization": auth_header}) as response:
                    print(f"   📊 Статус: {response.status}")
                    print(f"   📋 Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                    
                    # Проверяем тип контента
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' in content_type.lower():
                        try:
                            data = await response.json()
                            print(f"   ✅ JSON валиден, тип: {type(data)}")
                            if isinstance(data, dict):
                                print(f"   ✅ Ключи: {list(data.keys())}")
                        except json.JSONDecodeError as e:
                            print(f"   ❌ Ошибка парсинга JSON: {e}")
                    else:
                        text = await response.text()
                        preview = text[:200].replace('\n', '\\n')
                        print(f"   ⚠️ Не JSON ответ: {preview}...")
                
            except Exception as e:
                print(f"   ❌ Ошибка запроса: {e}")
            
            print()
    
    print("=" * 60)
    print("🔍 Диагностика завершена")
    print()
    print("📋 Рекомендации:")
    print("1. Убедитесь, что URL в config.yaml содержит '/exec'")
    print("2. Проверьте правильность логина и пароля")
    print("3. Убедитесь, что WMS сервер доступен и работает")
    print("4. Проверьте, что WMS API возвращает JSON, а не HTML")

if __name__ == "__main__":
    asyncio.run(diagnose_wms_api())
#!/usr/bin/env python3
"""
Диагностика проблем с WMS API
"""
import asyncio
import aiohttp
import base64
from core.config import get_config

async def diagnose_wms():
    """Диагностирует проблемы с WMS API"""
    print("🔍 Диагностика WMS API")
    print("=" * 50)
    
    # Получаем конфигурацию
    config = get_config()
    if not config.wms:
        print("❌ WMS не настроена в конфигурации")
        return
    
    wms_config = config.wms
    auth_header = f"Basic {base64.b64encode(f'{wms_config.username}:{wms_config.password}'.encode()).decode()}"
    
    print(f"🌐 URL: {wms_config.api_url}")
    print(f"👤 Пользователь: {wms_config.username}")
    print(f"🔑 Пароль: {'*' * len(wms_config.password)}")
    print()
    
    # Тестируем различные варианты URL
    test_urls = [
        f"{wms_config.api_url}?action=IncomeApi.getObjectsShipment",
        f"{wms_config.api_url.rstrip('/')}/exec?action=IncomeApi.getObjectsShipment",
        f"http://10.181.80.28:8080/exec?action=IncomeApi.getObjectsShipment",
        f"http://10.181.80.28:8080/?action=IncomeApi.getObjectsShipment",
        f"http://10.181.80.28:8080/",
    ]
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        for i, url in enumerate(test_urls, 1):
            print(f"🧪 Тест {i}: {url}")
            
            try:
                async with session.get(url, headers={"Authorization": auth_header}) as response:
                    print(f"   📊 Статус: {response.status}")
                    print(f"   📋 Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                    
                    # Читаем первые 200 символов ответа
                    text = await response.text()
                    preview = text[:200].replace('\n', '\\n').replace('\r', '\\r')
                    print(f"   📄 Превью ответа: {preview}...")
                    
                    # Пытаемся парсить как JSON
                    if 'application/json' in response.headers.get('Content-Type', ''):
                        try:
                            data = await response.json()
                            print(f"   ✅ JSON валиден, ключи: {list(data.keys()) if isinstance(data, dict) else 'не dict'}")
                        except Exception as e:
                            print(f"   ❌ Ошибка парсинга JSON: {e}")
                    else:
                        print(f"   ⚠️  Не JSON ответ")
                    
            except Exception as e:
                print(f"   ❌ Ошибка запроса: {e}")
            
            print()
    
    # Тестируем без аутентификации
    print("🔓 Тест без аутентификации:")
    try:
        async with session.get("http://10.181.80.28:8080/") as response:
            print(f"   📊 Статус: {response.status}")
            text = await response.text()
            preview = text[:200].replace('\n', '\\n')
            print(f"   📄 Превью: {preview}...")
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")

async def test_correct_url():
    """Тестирует правильный URL"""
    print("\n" + "=" * 50)
    print("🎯 Тест с правильным URL")
    
    config = get_config()
    wms_config = config.wms
    auth_header = f"Basic {base64.b64encode(f'{wms_config.username}:{wms_config.password}'.encode()).decode()}"
    
    # Правильный URL должен быть с /exec
    correct_url = "http://10.181.80.28:8080/exec?action=IncomeApi.getObjectsShipment"
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        try:
            async with session.get(correct_url, headers={"Authorization": auth_header}) as response:
                print(f"✅ Статус: {response.status}")
                print(f"✅ Content-Type: {response.headers.get('Content-Type')}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Получены данные: {type(data)}")
                    
                    if isinstance(data, dict):
                        print(f"✅ Ключи ответа: {list(data.keys())}")
                        
                        # Ищем документы
                        documents = data.get('getObjectsShipment', [])
                        print(f"✅ Найдено документов: {len(documents)}")
                        
                        if documents:
                            print("✅ Первый документ:")
                            first_doc = documents[0]
                            for key, value in list(first_doc.items())[:5]:
                                print(f"     {key}: {value}")
                    else:
                        print(f"⚠️  Данные не dict: {data}")
                else:
                    text = await response.text()
                    print(f"❌ Ошибка: {text[:200]}")
                    
        except Exception as e:
            print(f"❌ Ошибка запроса: {e}")

if __name__ == "__main__":
    asyncio.run(diagnose_wms())
    asyncio.run(test_correct_url())
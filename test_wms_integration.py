#!/usr/bin/env python3
"""
Тестовый скрипт для проверки интеграции с WMS
"""
import asyncio
import json
from wms_integration_improved import get_wms_integration

async def test_wms_connection():
    """Тестирует подключение к WMS"""
    print("🔍 Тестирование подключения к WMS...")
    
    try:
        integration = get_wms_integration()
        print(f"✅ WMS URL: {integration.wms_config.api_url}")
        print(f"✅ Пользователь: {integration.wms_config.username}")
        print(f"✅ Интервал опроса: {integration.wms_config.poll_interval} сек")
        
        # Тестируем получение документов
        import aiohttp
        from aiohttp import ClientTimeout
        
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
            documents = await integration._get_documents_from_wms(session)
            print(f"📄 Найдено документов: {len(documents)}")
            
            if documents:
                print("\n📋 Первые 3 документа:")
                for i, doc in enumerate(documents[:3]):
                    print(f"  {i+1}. ID: {doc.get('externalId', 'N/A')}")
                    print(f"     Статус: {doc.get('idStatus', 'N/A')}")
                    print(f"     Тип: {doc.get('type', 'N/A')}")
                    print()
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения к WMS: {e}")
        return False

async def test_document_processing():
    """Тестирует обработку документов"""
    print("🔄 Тестирование обработки документов...")
    
    try:
        integration = get_wms_integration()
        
        # Запускаем один цикл обработки
        import aiohttp
        from aiohttp import ClientTimeout
        
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
            documents = await integration._get_documents_from_wms(session)
            
            if documents:
                print(f"📄 Обрабатываем {len(documents)} документов...")
                await integration._process_documents(session, documents)
                
                # Показываем статус документов
                status = integration.get_documents_status()
                print(f"📊 Статус документов в системе: {len(status)}")
                
                for doc_id, doc_status in list(status.items())[:5]:
                    print(f"  📄 {doc_id}: {doc_status['status']} ({doc_status['tasks_created']} задач)")
            else:
                print("📄 Документы для обработки не найдены")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка обработки документов: {e}")
        return False

def show_documents_state():
    """Показывает текущее состояние документов"""
    print("📊 Текущее состояние документов:")
    
    try:
        integration = get_wms_integration()
        status = integration.get_documents_status()
        
        if not status:
            print("  📭 Нет отслеживаемых документов")
            return
        
        print(f"  📄 Всего документов: {len(status)}")
        print("  " + "="*50)
        
        for doc_id, doc_status in status.items():
            print(f"  📄 {doc_id}")
            print(f"     Статус: {doc_status['status']}")
            print(f"     Последняя проверка: {doc_status['last_check']}")
            print(f"     Создано задач: {doc_status['tasks_created']}")
            print()
            
    except Exception as e:
        print(f"❌ Ошибка получения состояния: {e}")

async def monitor_wms(duration: int = 60):
    """Мониторит WMS в течение указанного времени"""
    print(f"👁️ Мониторинг WMS в течение {duration} секунд...")
    
    try:
        integration = get_wms_integration()
        
        # Запускаем мониторинг в отдельной задаче
        monitor_task = asyncio.create_task(integration.start())
        
        # Ждем указанное время
        await asyncio.sleep(duration)
        
        # Останавливаем мониторинг
        await integration.stop()
        monitor_task.cancel()
        
        print("✅ Мониторинг завершен")
        show_documents_state()
        
    except Exception as e:
        print(f"❌ Ошибка мониторинга: {e}")

async def main():
    """Основная функция тестирования"""
    print("🧪 Тестирование интеграции с WMS")
    print("=" * 50)
    
    # Тест 1: Подключение к WMS
    if not await test_wms_connection():
        print("❌ Тест подключения не пройден, завершаем")
        return
    
    print("\n" + "="*50)
    
    # Тест 2: Обработка документов
    if not await test_document_processing():
        print("❌ Тест обработки не пройден")
    
    print("\n" + "="*50)
    
    # Показываем текущее состояние
    show_documents_state()
    
    print("\n" + "="*50)
    
    # Предлагаем запустить мониторинг
    try:
        response = input("Запустить мониторинг на 60 секунд? (y/N): ")
        if response.lower() == 'y':
            await monitor_wms(60)
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
    
    print("\n✅ Тестирование завершено")

if __name__ == "__main__":
    asyncio.run(main())
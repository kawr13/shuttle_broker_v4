#!/usr/bin/env python3
"""
Монитор для отслеживания статуса WMS интеграции
"""
import json
import sys
import argparse
from datetime import datetime
from wms_integration_improved import get_wms_integration

def show_documents_status():
    """Показывает статус всех документов"""
    try:
        integration = get_wms_integration()
        status = integration.get_documents_status()
        
        print("📊 Статус документов WMS")
        print("=" * 60)
        
        if not status:
            print("📭 Нет отслеживаемых документов")
            return
        
        print(f"📄 Всего документов: {len(status)}")
        print()
        
        # Группируем по статусам
        status_groups = {}
        for doc_id, doc_status in status.items():
            doc_status_key = doc_status['status']
            if doc_status_key not in status_groups:
                status_groups[doc_status_key] = []
            status_groups[doc_status_key].append((doc_id, doc_status))
        
        # Показываем по группам
        for status_key, docs in status_groups.items():
            print(f"📋 Статус '{status_key}': {len(docs)} документов")
            for doc_id, doc_status in docs[:5]:  # Показываем первые 5
                last_check = datetime.fromisoformat(doc_status['last_check'])
                time_ago = datetime.now() - last_check
                print(f"  📄 {doc_id}")
                print(f"     Задач создано: {doc_status['tasks_created']}")
                print(f"     Последняя проверка: {time_ago.seconds // 60} мин назад")
            
            if len(docs) > 5:
                print(f"     ... и еще {len(docs) - 5} документов")
            print()
        
    except Exception as e:
        print(f"❌ Ошибка получения статуса: {e}")

def show_state_file():
    """Показывает содержимое файла состояния"""
    try:
        with open('wms_documents_state.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print("📁 Содержимое файла состояния")
        print("=" * 60)
        print(f"📄 Документов в файле: {len(data)}")
        
        if data:
            print("\n🔍 Детали:")
            for doc_id, doc_data in list(data.items())[:10]:  # Первые 10
                print(f"  📄 {doc_id}")
                print(f"     Статус: {doc_data['status']}")
                print(f"     Задач создано: {len(doc_data['tasks_created'])}")
                print(f"     Последняя проверка: {doc_data['last_check']}")
                print()
            
            if len(data) > 10:
                print(f"     ... и еще {len(data) - 10} документов")
        
    except FileNotFoundError:
        print("📁 Файл состояния не найден")
    except Exception as e:
        print(f"❌ Ошибка чтения файла состояния: {e}")

def clear_state():
    """Очищает файл состояния"""
    try:
        import os
        if os.path.exists('wms_documents_state.json'):
            os.remove('wms_documents_state.json')
            print("✅ Файл состояния очищен")
        else:
            print("📁 Файл состояния не существует")
    except Exception as e:
        print(f"❌ Ошибка очистки файла состояния: {e}")

def show_config():
    """Показывает конфигурацию WMS"""
    try:
        from core.config import get_config
        config = get_config()
        
        print("⚙️ Конфигурация WMS")
        print("=" * 60)
        
        if config.wms:
            print(f"🌐 URL: {config.wms.api_url}")
            print(f"👤 Пользователь: {config.wms.username}")
            print(f"🔄 Интервал опроса: {config.wms.poll_interval} сек")
            print(f"🔗 Webhook URL: {config.wms.webhook_url or 'Не настроен'}")
        else:
            print("❌ WMS не настроена в конфигурации")
        
    except Exception as e:
        print(f"❌ Ошибка получения конфигурации: {e}")

def main():
    parser = argparse.ArgumentParser(description='Монитор WMS интеграции')
    parser.add_argument('action', choices=['status', 'file', 'config', 'clear'], 
                       help='Действие для выполнения')
    
    args = parser.parse_args()
    
    print(f"🔍 WMS Монитор - {args.action}")
    print()
    
    if args.action == 'status':
        show_documents_status()
    elif args.action == 'file':
        show_state_file()
    elif args.action == 'config':
        show_config()
    elif args.action == 'clear':
        response = input("Вы уверены, что хотите очистить файл состояния? (y/N): ")
        if response.lower() == 'y':
            clear_state()
        else:
            print("Операция отменена")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Скрипт для перезапуска шлюза с правильной конфигурацией
"""
import os
import sys
import signal
import subprocess
import time
import argparse
import yaml

def load_config() -> dict:
    """Загружает конфигурацию из файла"""
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)

def find_gateway_process():
    """Находит процесс шлюза"""
    try:
        result = subprocess.run(
            ["ps", "-ef"], 
            capture_output=True, 
            text=True, 
            check=True
        )
        
        for line in result.stdout.splitlines():
            if "python" in line and "main.py" in line and "shuttle_gateway" in line:
                parts = line.split()
                if len(parts) > 1:
                    return int(parts[1])
        
        return None
    except Exception as e:
        print(f"❌ Ошибка при поиске процесса шлюза: {e}")
        return None

def stop_gateway():
    """Останавливает шлюз"""
    pid = find_gateway_process()
    if pid:
        print(f"🛑 Останавливаем шлюз (PID: {pid})...")
        try:
            os.kill(pid, signal.SIGTERM)
            # Ждем завершения процесса
            for _ in range(10):
                try:
                    os.kill(pid, 0)  # Проверяем, существует ли процесс
                    time.sleep(1)
                except OSError:
                    break
            else:
                # Если процесс не завершился, убиваем его
                print("⚠️ Шлюз не завершился корректно, принудительно завершаем...")
                os.kill(pid, signal.SIGKILL)
            
            print("✅ Шлюз остановлен")
            return True
        except Exception as e:
            print(f"❌ Ошибка при остановке шлюза: {e}")
            return False
    else:
        print("ℹ️ Шлюз не запущен")
        return True

def start_gateway():
    """Запускает шлюз"""
    print("🚀 Запускаем шлюз...")
    try:
        # Запускаем шлюз в фоновом режиме
        subprocess.Popen(
            ["python", "main.py"],
            stdout=open("gateway_stdout.log", "a"),
            stderr=open("gateway_stderr.log", "a"),
            start_new_session=True
        )
        
        print("✅ Шлюз запущен")
        return True
    except Exception as e:
        print(f"❌ Ошибка при запуске шлюза: {e}")
        return False

def register_handlers():
    """Регистрирует обработчики для всех шаттлов"""
    print("🔄 Регистрация обработчиков для шаттлов...")
    
    # Загружаем конфигурацию
    config = load_config()
    
    # Получаем список шаттлов
    shuttles = config.get('shuttles', {})
    
    if not shuttles:
        print("❌ Шаттлы не найдены в конфигурации")
        return False
    
    # Регистрируем обработчики для каждого шаттла
    for shuttle_id in shuttles:
        print(f"🔄 Регистрация обработчика для шаттла {shuttle_id}...")
        try:
            subprocess.run(
                ["python", "register_shuttle_handler.py", shuttle_id],
                check=True
            )
            print(f"✅ Обработчик для шаттла {shuttle_id} зарегистрирован")
        except subprocess.CalledProcessError as e:
            print(f"❌ Ошибка при регистрации обработчика для шаттла {shuttle_id}: {e}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Перезапуск шлюза с правильной конфигурацией')
    parser.add_argument('--no-stop', action='store_true', help='Не останавливать шлюз перед запуском')
    parser.add_argument('--no-start', action='store_true', help='Не запускать шлюз после остановки')
    parser.add_argument('--register', action='store_true', help='Только зарегистрировать обработчики')
    
    args = parser.parse_args()
    
    if args.register:
        register_handlers()
        return 0
    
    if not args.no_stop:
        if not stop_gateway():
            return 1
    
    if not args.no_start:
        if not start_gateway():
            return 1
        
        # Даем шлюзу время на запуск
        print("⏳ Ожидание запуска шлюза (5 секунд)...")
        time.sleep(5)
        
        # Регистрируем обработчики
        register_handlers()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
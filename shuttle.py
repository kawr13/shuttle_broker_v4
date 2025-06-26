#!/usr/bin/env python3
"""
Универсальный скрипт для работы с шаттлами
"""
import sys
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(
        description='Универсальный инструмент для работы с шаттлами',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  shuttle.py list                          # Показать все шаттлы
  shuttle.py send shuttle_140 STATUS       # Отправить команду STATUS
  shuttle.py cmd shuttle_140 HOME -t 15    # Отправить HOME с таймаутом 15 сек
  shuttle.py cmd shuttle_140 BATTERY --no-listen  # Только отправить, не слушать
        """
    )
    
    subparsers = parser.add_subparsers(dest='action', help='Доступные действия')
    
    # Команда list
    list_parser = subparsers.add_parser('list', help='Показать все доступные шаттлы')
    
    # Команда send (упрощенная)
    send_parser = subparsers.add_parser('send', help='Отправить команду шаттлу (упрощенно)')
    send_parser.add_argument('shuttle_id', help='ID шаттла')
    send_parser.add_argument('command', help='Команда для отправки')
    
    # Команда cmd (расширенная)
    cmd_parser = subparsers.add_parser('cmd', help='Отправить команду шаттлу (расширенно)')
    cmd_parser.add_argument('shuttle_id', help='ID шаттла')
    cmd_parser.add_argument('command', help='Команда для отправки')
    cmd_parser.add_argument('--timeout', '-t', type=float, default=10.0, help='Таймаут ожидания ответа')
    cmd_parser.add_argument('--no-listen', action='store_true', help='Не слушать ответ')
    
    args = parser.parse_args()
    
    if not args.action:
        parser.print_help()
        return 1
    
    try:
        if args.action == 'list':
            return subprocess.call(['python', 'list_shuttles.py'])
        
        elif args.action == 'send':
            return subprocess.call(['python', 'send_command.py', args.shuttle_id, args.command])
        
        elif args.action == 'cmd':
            cmd = ['python', 'shuttle_direct_client.py', args.shuttle_id, args.command]
            if args.timeout != 10.0:
                cmd.extend(['--timeout', str(args.timeout)])
            if args.no_listen:
                cmd.append('--no-listen')
            return subprocess.call(cmd)
        
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        return 1
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
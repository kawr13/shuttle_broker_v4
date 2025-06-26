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
  shuttle.py direct shuttle_140 STATUS --ip 10.181.80.132  # Прямое подключение с указанием IP
  shuttle.py check                         # Проверить доступность шаттлов
  shuttle.py fix shuttle_140 10.181.80.132 # Исправить IP-адрес шаттла
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
    
    # Команда direct (прямое подключение)
    direct_parser = subparsers.add_parser('direct', help='Прямое подключение к шаттлу')
    direct_parser.add_argument('shuttle_id', help='ID шаттла')
    direct_parser.add_argument('command', help='Команда для отправки')
    direct_parser.add_argument('--ip', help='IP-адрес шаттла')
    direct_parser.add_argument('--port', type=int, default=2000, help='Порт для отправки команд')
    direct_parser.add_argument('--listen-port', type=int, default=8181, help='Порт для прослушивания ответов')
    direct_parser.add_argument('--timeout', '-t', type=float, default=10.0, help='Таймаут ожидания ответа')
    direct_parser.add_argument('--no-listen', action='store_true', help='Не слушать ответ')
    
    # Команда check (проверка доступности)
    check_parser = subparsers.add_parser('check', help='Проверить доступность шаттлов')
    check_parser.add_argument('--discover', action='store_true', help='Обнаружить шаттлы в сети')
    check_parser.add_argument('--subnet', default='10.181.80.0', help='Подсеть для поиска')
    
    # Команда fix (исправление конфигурации)
    fix_parser = subparsers.add_parser('fix', help='Исправить конфигурацию шаттла')
    fix_parser.add_argument('shuttle_id', help='ID шаттла')
    fix_parser.add_argument('new_ip', help='Новый IP-адрес')
    
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
        
        elif args.action == 'direct':
            cmd = ['python', 'shuttle_direct_connect.py', args.shuttle_id, args.command]
            if args.ip:
                cmd.extend(['--ip', args.ip])
            if args.port != 2000:
                cmd.extend(['--port', str(args.port)])
            if args.listen_port != 8181:
                cmd.extend(['--listen-port', str(args.listen_port)])
            if args.timeout != 10.0:
                cmd.extend(['--timeout', str(args.timeout)])
            if args.no_listen:
                cmd.append('--no-listen')
            return subprocess.call(cmd)
        
        elif args.action == 'check':
            cmd = ['python', 'check_shuttle_connectivity.py']
            if args.discover:
                cmd.append('--discover')
                if args.subnet != '10.181.80.0':
                    cmd.extend(['--subnet', args.subnet])
            return subprocess.call(cmd)
        
        elif args.action == 'fix':
            return subprocess.call(['python', 'fix_shuttle_config.py', 'fix', args.shuttle_id, args.new_ip])
        
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        return 1
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
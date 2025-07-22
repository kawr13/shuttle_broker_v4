"""Утилиты для работы с шаттлами"""

def get_shuttle_name(ip, custom_name=None):
    """
    Генерирует имя шаттла на основе IP-адреса или возвращает пользовательское имя
    
    Args:
        ip (str): IP-адрес шаттла
        custom_name (str, optional): Пользовательское имя
        
    Returns:
        str: Имя шаттла
    """
    if custom_name:
        return custom_name
        
    try:
        last_octet = int(ip.split('.')[-1])
        if 131 <= last_octet <= 139:
            return f"Шаттл_{last_octet - 130}"
        elif 140 <= last_octet <= 149:
            return f"Шаттл_{last_octet - 130}"
        else:
            return f"Shuttle-{last_octet}"
    except (ValueError, IndexError):
        return f"Shuttle-{ip}"
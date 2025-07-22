# Финальные улучшения шлюза шаттлов

## 1. Улучшенное добавление шаттлов

Исправлена проблема с добавлением шаттлов в конфигурацию:

- Автоматическое создание пустого файла конфигурации, если он не существует
- Двойная проверка при добавлении новых шаттлов
- Резервный механизм добавления шаттлов даже при ошибках
- Улучшенное логирование процесса добавления шаттлов

```python
# Создаем пустой файл конфигурации, если он не существует
if not os.path.exists(self.shuttles_config_path):
    with open(self.shuttles_config_path, 'w') as f:
        json.dump({"shuttles": []}, f, indent=2)
    logger.info(f"Создан пустой файл конфигурации: {self.shuttles_config_path}")
```

## 2. Надежное сохранение конфигурации

Улучшен механизм сохранения конфигурации:

- Создание директории для конфигурации, если она не существует
- Атомарное сохранение через временный файл
- Резервный механизм прямого сохранения при ошибках
- Подробное логирование процесса сохранения

```python
# Пытаемся сохранить напрямую при ошибке атомарного сохранения
try:
    with open(self.shuttles_config_path, 'w') as f:
        json.dump({"shuttles": list(self.shuttles.values())}, f, indent=2)
    logger.info(f"Конфигурация сохранена напрямую: {len(self.shuttles)} шаттлов")
except Exception as e2:
    logger.error(f"Критическая ошибка сохранения конфигурации: {e2}")
```

## 3. Автоматическое именование шаттлов

Добавлена система автоматического именования шаттлов по диапазонам IP:

- IP 131-139: Шаттл_1 - Шаттл_9
- IP 140-149: Шаттл_10 - Шаттл_19
- Другие IP: Shuttle-XXX

```python
def get_shuttle_name(ip, custom_name=None):
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
```

## 4. Улучшенное отображение логов

- Отображение времени в московской временной зоне
- Новые сообщения отображаются сверху
- Автоматическая прокрутка к новым сообщениям

```javascript
// Опции для московской временной зоны
const timeOptions = { 
    timeZone: 'Europe/Moscow',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
};

// Добавляем в начало лога (новые сверху)
if (log.firstChild) {
    log.insertBefore(entry, log.firstChild);
} else {
    log.appendChild(entry);
}
```

## 5. Периодическое сохранение конфигурации

Добавлено автоматическое сохранение конфигурации каждую минуту:

```python
async def periodic_config_save(self):
    """Периодически сохранять конфигурацию шаттлов"""
    while True:
        try:
            await asyncio.sleep(60)  # Сохраняем каждую минуту
            if self.shuttles:
                logger.debug("Периодическое сохранение конфигурации шаттлов")
                self.save_shuttles_config()
        except Exception as e:
            logger.error(f"Ошибка периодического сохранения: {e}")
```
#!/usr/bin/env python3
"""
Тест для проверки отправки MRCD
"""
import asyncio
import sys

async def test_mrcd_response():
    """Тестирует отправку MRCD в ответ на сообщение от шаттла"""
    
    print("🧪 Тест отправки MRCD")
    print("=" * 50)
    
    # Эмулируем шаттл, который отправляет сообщение
    async def emulate_shuttle():
        """Эмулирует шаттл, отправляющий сообщение на порт 8181"""
        try:
            # Подключаемся к шлюзу
            reader, writer = await asyncio.open_connection('127.0.0.1', 8181)
            print("📡 Подключились к шлюзу как шаттл")
            
            # Отправляем сообщение STATUS=FREE
            message = "STATUS=FREE\r\n"
            writer.write(message.encode('utf-8'))
            await writer.drain()
            print(f"📤 Отправили сообщение: {message.strip()}")
            
            # Ждем ответ MRCD
            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
                if data:
                    response = data.decode('utf-8').strip()
                    print(f"📨 Получили ответ: {response}")
                    if response == "MRCD":
                        print("✅ MRCD получен корректно!")
                        return True
                    else:
                        print(f"❌ Ожидали MRCD, получили: {response}")
                        return False
                else:
                    print("❌ Не получили ответ")
                    return False
            except asyncio.TimeoutError:
                print("⏰ Таймаут ожидания ответа MRCD")
                return False
            finally:
                writer.close()
                await writer.wait_closed()
                
        except Exception as e:
            print(f"❌ Ошибка эмуляции шаттла: {e}")
            return False
    
    # Проверяем, запущен ли шлюз
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection('127.0.0.1', 8181), 
            timeout=2.0
        )
        writer.close()
        await writer.wait_closed()
        print("✅ Шлюз запущен на порту 8181")
    except Exception:
        print("❌ Шлюз не запущен на порту 8181")
        print("💡 Запустите шлюз командой: python main.py")
        return False
    
    # Запускаем тест
    result = await emulate_shuttle()
    
    print("=" * 50)
    if result:
        print("🎉 Тест пройден: MRCD отправляется корректно!")
    else:
        print("💥 Тест провален: MRCD не отправляется!")
    
    return result

if __name__ == "__main__":
    try:
        result = asyncio.run(test_mrcd_response())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n🛑 Тест прерван")
        sys.exit(1)
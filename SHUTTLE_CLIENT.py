import asyncio
import yaml
from typing import Optional


class ShuttleClient:
    def __init__(self, config_path: str = "config.yaml", response_port: int = 8181):
        self.response_port = response_port
        self.config = self._load_config(config_path)

    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f).get("shuttles", {})

    async def send_command(self, shuttle_id: str, command: str, timeout: float = 10.0) -> Optional[str]:
        """Отправляет команду шаттлу и ожидает ответ"""
        if shuttle_id not in self.config:
            raise ValueError(f"Шаттл {shuttle_id} не найден в конфигурации")

        shuttle_info = self.config[shuttle_id]
        host = shuttle_info["host"]
        port = shuttle_info.get("command_port", 2000)

        # Отправка команды
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=5.0
            )
            if not command.endswith('\r\n'):
                command += '\r\n'

            writer.write(command.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            raise ConnectionError(f"Ошибка при отправке команды шаттлу: {e}")

        # Прослушка ответа
        return await self._listen_for_response(host, timeout)

    async def _listen_for_response(self, expected_ip: str, timeout: float = 10.0) -> Optional[str]:
        response_data = None

        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            nonlocal response_data
            peername = writer.get_extra_info("peername")
            if not peername or peername[0] != expected_ip:
                writer.close()
                await writer.wait_closed()
                return

            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
                response_data = data.decode('utf-8').strip()
            except Exception:
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(handler, host="0.0.0.0", port=self.response_port)
        try:
            await asyncio.wait_for(self._wait_for_response(lambda: response_data), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            server.close()
            await server.wait_closed()

        return response_data

    async def _wait_for_response(self, condition_func, check_interval=0.1):
        while not condition_func():
            await asyncio.sleep(check_interval)


'''

import asyncio
from shuttle_client import ShuttleClient

async def main():
    client = ShuttleClient("config.yaml")

    response = await client.send_command("shuttle_140", "STATUS")
    print("🔁 Ответ STATUS:", response)

    if "READY" in response:
        mrcd_response = await client.send_command("shuttle_140", "MRCD")
        print("🔁 Ответ MRCD:", mrcd_response)

    battery = await client.send_command("shuttle_140", "BATTERY")
    print("🔋 Батарея:", battery)

asyncio.run(main())
🔩 Что ты можешь теперь делать:
Вызывать любые команды по очереди: STATUS, MRCD, BATTERY, STACK_OUT, PALLET_IN, и т.д.

Интегрировать в свой основной шлюз:

from shuttle_client import ShuttleClient

shuttle = ShuttleClient("config.yaml")

await shuttle.send_command("shuttle_123", "STACK_OUT 00123")


'''
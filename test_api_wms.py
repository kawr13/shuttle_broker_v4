import asyncio
import aiohttp
import base64
import logging
from aiohttp import ClientSession, ClientTimeout
import json

# Configuration
WMS_API_URL = "http://10.181.80.28:8080/exec"
USERNAME = "1000"
PASSWORD = "1000"
AUTH_HEADER = f"Basic {base64.b64encode(f'{USERNAME}:{PASSWORD}'.encode()).decode()}"
SHUTTLE_COMMAND_PORT = 2000
SHUTTLE_RESPONSE_PORT = 8181
POLL_INTERVAL = 300  # 5 minutes
SHUTTLE_READ_TIMEOUT = 5  # Timeout for reading shuttle response
RETRY_INTERVAL = 5  # Retry interval for server startup

# Shuttle commands
SHUTTLE_COMMANDS = [
    "PALLET_IN", "PALLET_OUT", "FIFO-nnn", "FILO-nnn", "STACK_IN", "STACK_OUT",
    "HOME", "COUNT", "STATUS", "BATTERY", "WDH", "WLH", "MRCD"
]

# Logging setup
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AsyncShuttleMonitor")

async def get_documents_from_wms(session: ClientSession, action="IncomeApi.getObjectsShipment"):
    """Fetch documents from WMS asynchronously."""
    logger.debug(f"Отправка запроса к WMS: {action}")
    try:
        async with session.get(f"{WMS_API_URL}?action={action}", headers={"Authorization": AUTH_HEADER}) as response:
            response.raise_for_status()
            data = await response.json()
            logger.debug(f"Получен ответ от WMS: {data}")
            return data.get(action.split('.')[-1], [])
    except aiohttp.ClientConnectionError as e:
        logger.error(f"Ошибка соединения с WMS: {e}")
        return []
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка ответа WMS (статус {e.status}): {e.message}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка при запросе к WMS: {e}")
        return []

async def get_tasks_for_doc(session: ClientSession, external_id: str):
    """Fetch tasks for a specific document asynchronously."""
    logger.debug(f"Запрос задач для документа: {external_id}")
    try:
        async with session.get(
            f"{WMS_API_URL}?action=IncomeApi.taskExecutesDoc&p={external_id}",
            headers={"Authorization": AUTH_HEADER}
        ) as response:
            response.raise_for_status()
            data = await response.json()
            logger.debug(f"Получены задачи для документа {external_id}: {data}")
            return data
    except aiohttp.ClientConnectionError as e:
        logger.error(f"Ошибка соединения при запросе задач для {external_id}: {e}")
        return []
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка ответа WMS для задач (статус {e.status}): {e.message}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка при запросе задач для {external_id}: {e}")
        return []

def determine_shuttle_command(task: dict) -> str:
    """Determine the shuttle command based on task details."""
    task_type = task.get("type", "").upper()
    if "IN" in task_type:
        return "PALLET_IN"
    elif "OUT" in task_type:
        return "PALLET_OUT"
    return "STATUS"  # Default command if unclear

async def send_command_to_shuttle(command: str, task_id: str):
    """Send command to shuttle system asynchronously."""
    full_command = f"{command} {task_id}\r\n"
    try:
        reader, writer = await asyncio.open_connection("localhost", SHUTTLE_COMMAND_PORT)
        writer.write(full_command.encode())
        await writer.drain()
        logger.info(f"Отправлена команда: {full_command.strip()}")
        writer.close()
        await writer.wait_closed()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"Ошибка при отправке команды шатлу: {e}")

async def update_wms_task_status(session: ClientSession, task_id: str, status: str):
    """Update task status in WMS asynchronously."""
    payload = {
        "task": [
            {
                "externalId": task_id,
                "status": status
            }
        ]
    }
    logger.debug(f"Обновление статуса задачи {task_id} в WMS: {status}")
    try:
        async with session.post(
            f"{WMS_API_URL}?action=IncomeApi.insertUpdate",
            headers={"Authorization": AUTH_HEADER, "Content-Type": "application/json"},
            json=payload
        ) as response:
            response.raise_for_status()
            logger.info(f"Статус задачи {task_id} обновлен на {status}")
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при обновлении статуса задачи: {e}")

async def parse_shuttle_response(data: str) -> tuple[str, str]:
    """Parse shuttle response to get task ID and status."""
    logger.debug(f"Получено сообщение от шаттла: {data!r}")
    parts = data.strip().split()
    if len(parts) >= 2:
        command_status = parts[0]
        task_id = parts[1]
        status = "completed" if "DONE" in command_status else "error"
        return task_id, status
    logger.warning(f"Некорректный формат сообщения шаттла: {data!r}")
    return None, None

async def handle_shuttle_response(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handle individual shuttle response with timeout."""
    try:
        peer_name = writer.get_extra_info('peername')
        client_ip = peer_name[0] if peer_name else 'unknown'
        
        # Read up to 1024 bytes with a timeout
        data = await asyncio.wait_for(
            reader.read(1024),
            timeout=SHUTTLE_READ_TIMEOUT
        )
        decoded_data = data.decode('utf-8', errors='ignore')
        logger.debug(f"Сырые данные от шаттла ({client_ip}): {decoded_data!r}")
        
        task_id, status = await parse_shuttle_response(decoded_data)
        if task_id and status:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                await update_wms_task_status(session, task_id, status)
        else:
            logger.warning(f"Не удалось разобрать сообщение шаттла: {decoded_data!r}")
    except asyncio.TimeoutError:
        logger.error("Тайм-аут при ожидании ответа шаттла")
    except Exception as e:
        logger.error(f"Ошибка при обработке ответа шаттла: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def shuttle_listener():
    """Listen for shuttle responses asynchronously with retry on failure."""
    while True:
        try:
            server = await asyncio.start_server(handle_shuttle_response, '0.0.0.0', SHUTTLE_RESPONSE_PORT)
            logger.info(f"Прослушивание ответов шаттла на порту {SHUTTLE_RESPONSE_PORT}")
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Ошибка сервера шаттлов: {e}. Повторная попытка через {RETRY_INTERVAL} секунд.")
            await asyncio.sleep(RETRY_INTERVAL)

async def wms_polling():
    """Poll WMS for documents and tasks asynchronously."""
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
        while True:
            logger.debug("Начало цикла опроса WMS")
            try:
                documents = await get_documents_from_wms(session)
                if not documents:
                    logger.info("Документы не найдены")
                tasks = []
                for doc in documents:
                    doc_id = doc.get("externalId", "unknown")
                    logger.info(f"Обработка документа: {doc_id} (статус: {doc.get('idStatus', 'unknown')})")
                    if doc.get("idStatus") in ["selectionCreated", "selectionWork"]:
                        doc_tasks = await get_tasks_for_doc(session, doc_id)
                        for task in doc_tasks:
                            command = determine_shuttle_command(task)
                            tasks.append(send_command_to_shuttle(command, task["externalId"]))
                if tasks:
                    await asyncio.gather(*tasks)
                else:
                    logger.debug("Задачи для шаттлов не найдены")
                await asyncio.sleep(POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Ошибка в цикле опроса WMS: {e}")
                await asyncio.sleep(60)  # Wait before retrying

async def main():
    """Main function to run the async tasks."""
    await asyncio.gather(shuttle_listener(), wms_polling())

if __name__ == "__main__":
    asyncio.run(main())
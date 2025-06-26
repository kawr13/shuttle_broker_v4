import asyncio
import aiohttp
import base64
import logging
import sys
import json
from aiohttp import ClientSession, ClientTimeout

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
    url = f"{WMS_API_URL}?action={action}"
    logger.debug(f"Отправка запроса к WMS: {url}")
    try:
        async with session.get(url, headers={"Authorization": AUTH_HEADER}) as response:
            logger.debug(f"Получен ответ от WMS, статус: {response.status}, тип контента: {response.headers.get('Content-Type')}")
            response.raise_for_status()
            
            # Проверяем тип контента
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type.lower():
                text = await response.text()
                logger.error(f"WMS вернул не JSON ответ: {content_type}, первые 200 символов: {text[:200]}")
                return []
                
            data = await response.json()
            logger.debug(f"Получен JSON ответ от WMS: {data}")
            return data.get(action.split('.')[-1], [])
    except aiohttp.ClientConnectionError as e:
        logger.error(f"Ошибка соединения с WMS: {e}")
        return []
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка ответа WMS (статус {e.status}): {e.message}, URL: {url}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON от WMS: {e}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка при запросе к WMS: {e}, URL: {url}")
        return []

async def get_tasks_for_doc(session: ClientSession, external_id: str):
    """Fetch tasks for a specific document asynchronously."""
    url = f"{WMS_API_URL}?action=IncomeApi.taskExecutesDoc&p={external_id}"
    logger.debug(f"Запрос задач для документа: {external_id}, URL: {url}")
    try:
        async with session.get(url, headers={"Authorization": AUTH_HEADER}) as response:
            logger.debug(f"Получен ответ для задач документа {external_id}, статус: {response.status}")
            response.raise_for_status()
            
            # Проверяем тип контента
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type.lower():
                text = await response.text()
                logger.error(f"WMS вернул не JSON ответ для задач: {content_type}, первые 200 символов: {text[:200]}")
                return []
                
            data = await response.json()
            logger.debug(f"Получены задачи для документа {external_id}: {data}")
            return data if isinstance(data, list) else []
    except aiohttp.ClientConnectionError as e:
        logger.error(f"Ошибка соединения при запросе задач для {external_id}: {e}")
        return []
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка ответа WMS для задач (статус {e.status}): {e.message}, URL: {url}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON для задач документа {external_id}: {e}")
        return []
    except Exception as e:
        logger.error(f"Неизвестная ошибка при запросе задач для {external_id}: {e}, URL: {url}")
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
    url = f"{WMS_API_URL}?action=IncomeApi.insertUpdate"
    logger.debug(f"Обновление статуса задачи {task_id} в WMS: {status}, URL: {url}")
    try:
        async with session.post(
            url,
            headers={"Authorization": AUTH_HEADER, "Content-Type": "application/json"},
            json=payload
        ) as response:
            logger.debug(f"Получен ответ на обновление статуса {task_id}, статус: {response.status}")
            response.raise_for_status()
            
            # Проверяем тип контента
            content_type = response.headers.get('Content-Type', '')
            if response.status == 200:
                try:
                    result = await response.json() if 'application/json' in content_type.lower() else None
                    logger.info(f"Статус задачи {task_id} обновлен на {status}, ответ: {result}")
                except:
                    text = await response.text()
                    logger.info(f"Статус задачи {task_id} обновлен на {status}, ответ не JSON")
            else:
                logger.warning(f"Неожиданный статус при обновлении задачи {task_id}: {response.status}")
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка ответа при обновлении статуса задачи {task_id} (статус {e.status}): {e.message}")
    except aiohttp.ClientConnectionError as e:
        logger.error(f"Ошибка соединения при обновлении статуса задачи {task_id}: {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при обновлении статуса задачи {task_id}: {e}")

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

async def diagnose_wms_api():
    """Diagnostic function to check WMS API connectivity"""
    logger.info("=== Диагностика WMS API ===")
    logger.info(f"WMS API URL: {WMS_API_URL}")
    
    # Проверяем разные варианты URL
    urls_to_test = [
        WMS_API_URL,
        WMS_API_URL.replace('/exec', ''),
        "http://10.181.80.28:8080/exec",
        "http://10.181.80.28:8080/"
    ]
    
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
        for url in urls_to_test:
            logger.info(f"\nПроверка URL: {url}")
            try:
                # Пробуем простой GET запрос
                async with session.get(url, headers={"Authorization": AUTH_HEADER}) as response:
                    logger.info(f"  Статус: {response.status}")
                    logger.info(f"  Content-Type: {response.headers.get('Content-Type')}")
                    
                    # Пробуем получить текст
                    text = await response.text()
                    logger.info(f"  Первые 100 символов: {text[:100]}")
                    
                # Пробуем запрос с действием
                action_url = f"{url}?action=IncomeApi.getObjectsShipment"
                logger.info(f"  Проверка с действием: {action_url}")
                async with session.get(action_url, headers={"Authorization": AUTH_HEADER}) as response:
                    logger.info(f"  Статус: {response.status}")
                    logger.info(f"  Content-Type: {response.headers.get('Content-Type')}")
                    
                    if 'application/json' in response.headers.get('Content-Type', '').lower():
                        try:
                            data = await response.json()
                            logger.info(f"  JSON ответ: {type(data)}")
                            if isinstance(data, dict):
                                logger.info(f"  Ключи: {list(data.keys())}")
                        except Exception as e:
                            logger.error(f"  Ошибка парсинга JSON: {e}")
                    else:
                        text = await response.text()
                        logger.info(f"  Первые 100 символов: {text[:100]}")
            except Exception as e:
                logger.error(f"  Ошибка при проверке URL {url}: {e}")
    
    logger.info("=== Диагностика завершена ===")

async def main():
    """Main function to run the async tasks."""
    # Запускаем диагностику перед запуском основных задач
    if '--diagnose' in sys.argv:
        await diagnose_wms_api()
        return
    
    # Запуск основных задач
    await asyncio.gather(shuttle_listener(), wms_polling())

if __name__ == "__main__":
    asyncio.run(main())
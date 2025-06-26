import asyncio
import httpx
import json
import base64
import logging
from datetime import datetime
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wms_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("WMS_API")

# Параметры WMS
WMS_URL = "http://10.181.80.28:8080/exec"
LOGIN = "1000"
PASSWORD = "1000"
AUTH_HEADER = f"Basic {base64.b64encode(f'{LOGIN}:{PASSWORD}'.encode()).decode()}"

# Возможные методы GET для получения заданий
GET_METHODS = [
    {"action": "IncomeApi.getObjectsTransfer", "params": {}, "description": "Все перемещения"},
    {"action": "IncomeApi.getObjectsShipment", "params": {}, "description": "Все отгрузки"},
    {"action": "IncomeApi.getObjectsUserReceipt", "params": {}, "description": "Все поступления"},
    {"action": "IncomeApi.getObjectsInventoryItem", "params": {}, "description": "Все инвентаризации"},
    {"action": "IncomeApi.getShipmentStatusesPeriod", 
     "params": {"p": "26.01.2024 7:10", "p2": "26.06.2025 7:10"}, 
     "description": "Статусы отгрузок за период"},
    {"action": "IncomeApi.getUserReceiptStatusesPeriod", 
     "params": {"p": "26.01.2024 7:10", "p2": "26.06.2025 7:10"}, 
     "description": "Статусы поступлений за период"},
    {"action": "IncomeApi.getInventoryItemStatusesPeriod", 
     "params": {"p": "26.01.2024 7:10", "p2": "26.06.2025 7:10"}, 
     "description": "Статусы инвентаризаций за период"},
    {"action": "IncomeApi.taskExecutesDoc", 
     "params": {"p": "1f1ef602-cbf0-4381-8730-0c26ec4e6c0b"}, 
     "description": "Задачи по перемещению (тестовый externalId)"}
]

async def make_get_request(client, action, params, description):
    """Отправка GET-запроса к WMS API."""
    try:
        url = f"{WMS_URL}?action={action}"
        if params:
            url += "&" + "&".join(f"{k}={v}" for k, v in params.items())
        
        logger.debug(f"Отправка запроса: {url}")
        response = await client.get(
            url,
            headers={
                "Authorization": AUTH_HEADER,
                "Host": "10.181.80.28:8080",
                "Content-Type": "application/json"
            },
            timeout=10.0
        )
        
        result = {
            "action": action,
            "description": description,
            "status_code": response.status_code,
            "response": response.json() if response.is_success and response.text else response.text,
            "error": None
        }
        
        if response.status_code == 200:
            logger.info(f"Успешный запрос: {action}, Ответ: {response.text[:200]}...")
        elif response.status_code == 401:
            logger.error(f"Ошибка авторизации для {action}")
            result["error"] = "Unauthorized: Проверьте логин/пароль"
        elif response.status_code == 500:
            logger.error(f"Ошибка сервера для {action}: {response.text}")
            result["error"] = "Server Error: Проверьте синтаксис запроса"
        else:
            logger.error(f"Неизвестная ошибка для {action}: {response.status_code} {response.text}")
            result["error"] = f"Status {response.status_code}: {response.text}"
            
        return result
    
    except httpx.RequestError as e:
        logger.error(f"Ошибка сети для {action}: {e}")
        return {
            "action": action,
            "description": description,
            "status_code": None,
            "response": None,
            "error": f"Network Error: {str(e)}"
        }

async def main():
    """Основная функция для проверки всех методов GET."""
    results = []
    async with httpx.AsyncClient() as client:
        tasks = [
            make_get_request(client, method["action"], method["params"], method["description"])
            for method in GET_METHODS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Сохранение результатов в файл
    output_dir = Path("wms_responses")
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"wms_tasks_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Результаты сохранены в {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
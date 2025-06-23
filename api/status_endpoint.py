import json
from aiohttp import web
from shuttle_module.shuttle_manager import get_shuttle_manager
from core.logging import get_logger

logger = get_logger()

async def status_handler(request):
    """Обработчик запросов на эндпоинт /status"""
    shuttle_manager = get_shuttle_manager()
    states = await shuttle_manager.get_all_shuttle_states()
    
    result = {}
    for shuttle_id, state in states.items():
        result[shuttle_id] = {
            "status": str(state.status),
            "current_command": state.current_command,
            "last_seen": state.last_seen,
            "battery_level": state.battery_level,
            "location_data": state.location_data,
            "current_cell": state.current_cell,
            "error_code": state.error_code
        }
    
    logger.info(f"Запрос статуса шаттлов: возвращено {len(states)} шаттлов")
    return web.json_response(result)

async def discovered_shuttles_handler(request):
    """Обработчик запросов на эндпоинт /discovered"""
    from shuttle_module.shuttle_discovery import get_shuttle_discovery
    
    shuttle_discovery = get_shuttle_discovery()
    discovered = shuttle_discovery.get_discovered_shuttles()
    
    result = {}
    for shuttle_name, shuttle_info in discovered.items():
        result[shuttle_name] = {
            "ip": shuttle_info.ip,
            "name": shuttle_info.name,
            "location": shuttle_info.location,
            "status": shuttle_info.status,
            "battery_level": shuttle_info.battery_level,
            "discovered_at": shuttle_info.discovered_at
        }
    
    logger.info(f"Запрос обнаруженных шаттлов: возвращено {len(discovered)} шаттлов")
    return web.json_response(result)

def setup_routes(app):
    """Настраивает маршруты для API"""
    app.router.add_get('/status', status_handler)
    app.router.add_get('/discovered', discovered_shuttles_handler)
    logger.info("API маршруты настроены")

async def start_api_server(host='0.0.0.0', port=8000):
    """Запускает API-сервер"""
    app = web.Application()
    setup_routes(app)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"API-сервер запущен на http://{host}:{port}")
    return runner

async def stop_api_server(runner):
    """Останавливает API-сервер"""
    await runner.cleanup()
    logger.info("API-сервер остановлен")
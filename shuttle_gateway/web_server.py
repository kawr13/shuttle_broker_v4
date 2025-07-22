import asyncio
import json
import logging
import os
from aiohttp import web
from typing import Dict, List

# Импортируем модули шлюза
from shuttle.shuttle_client import ShuttleClient

logger = logging.getLogger(__name__)

class WebServer:
    def __init__(self, shuttle_client: ShuttleClient, host: str = "0.0.0.0", port: int = 8000):
        self.app = web.Application()
        self.shuttle_client = shuttle_client
        self.host = host
        self.port = port
        self.shuttle_states = {}  # Хранение состояний шаттлов
        self.setup_routes()
        
    def setup_routes(self):
        """Настройка маршрутов"""
        # Статические файлы
        self.app.router.add_static('/static', os.path.join(os.path.dirname(__file__), 'static'))
        
        # HTML интерфейс
        self.app.router.add_get('/', self.serve_index)
        
        # API для шаттлов
        self.app.router.add_get('/api/shuttles', self.get_shuttles)
        self.app.router.add_get('/api/shuttle/{ip}', self.get_shuttle_info)
        self.app.router.add_get('/api/shuttle/{ip}/responses', self.get_shuttle_responses)
        self.app.router.add_post('/api/command', self.send_command)
        
        # API для телеграм-бота
        self.app.router.add_get('/api/telegram/shuttles', self.get_shuttles_for_telegram)
        self.app.router.add_get('/api/telegram/shuttle/{ip}', self.get_shuttle_for_telegram)
        
    async def serve_index(self, request):
        """Отдать HTML интерфейс"""
        with open(os.path.join(os.path.dirname(__file__), 'index.html'), 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    
    async def get_shuttles(self, request):
        """Получить список всех шаттлов"""
        # Перезагружаем конфигурацию для получения актуального списка
        self.shuttle_client.load_shuttles_config()
        
        shuttles = []
        for ip, shuttle in self.shuttle_client.shuttles.items():
            # Получаем состояние шаттла из кэша или устанавливаем по умолчанию
            state = self.shuttle_states.get(ip, {
                "battery": None,
                "status": "offline",
                "errors": None,
                "last_response": None
            })
            
            shuttles.append({
                "id": ip.replace(".", "_"),
                "name": f"Shuttle-{ip.split('.')[-1]}",
                "ip": ip,
                "cell": shuttle.get("cell", "Unknown"),
                "warehouse": shuttle.get("warehouse", "Unknown"),
                "status": state["status"],
                "battery": state["battery"],
                "errors": state["errors"],
                "last_response": state["last_response"]
            })
        
        return web.json_response({"shuttles": shuttles})
    
    async def get_shuttle_info(self, request):
        """Получить информацию о конкретном шаттле"""
        ip = request.match_info['ip']
        
        if ip not in self.shuttle_client.shuttles:
            return web.json_response({"error": "Shuttle not found"}, status=404)
        
        shuttle = self.shuttle_client.shuttles[ip]
        state = self.shuttle_states.get(ip, {
            "battery": None,
            "status": "offline",
            "errors": None,
            "last_response": None
        })
        
        return web.json_response({
            "id": ip.replace(".", "_"),
            "name": f"Shuttle-{ip.split('.')[-1]}",
            "ip": ip,
            "cell": shuttle.get("cell", "Unknown"),
            "warehouse": shuttle.get("warehouse", "Unknown"),
            "status": state["status"],
            "battery": state["battery"],
            "errors": state["errors"],
            "last_response": state["last_response"]
        })
    
    async def get_shuttle_responses(self, request):
        """Получить ответы от шаттла"""
        ip = request.match_info['ip']
        
        if ip not in self.shuttle_client.shuttles:
            return web.json_response({"responses": []}, status=404)
        
        responses = self.shuttle_states.get(ip, {}).get("responses", [])
        return web.json_response({"responses": responses})
    
    async def send_command(self, request):
        """Отправить команду шаттлу"""
        try:
            data = await request.json()
            ip = data.get('ip')
            command = data.get('command')
            
            if not ip or not command:
                return web.json_response({"error": "Missing ip or command"}, status=400)
            
            if ip not in self.shuttle_client.shuttles:
                return web.json_response({"error": "Shuttle not found"}, status=404)
            
            # Отправляем команду шаттлу
            success = await self.shuttle_client.send_command(ip, command)
            
            if success:
                return web.json_response({"status": "success", "message": f"Command {command} sent to {ip}"})
            else:
                return web.json_response({"status": "error", "message": "Failed to send command"}, status=500)
                
        except Exception as e:
            logger.error(f"Error sending command: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)
    
    async def get_shuttles_for_telegram(self, request):
        """API для телеграм-бота - список шаттлов"""
        self.shuttle_client.load_shuttles_config()
        
        shuttles = []
        for ip, shuttle in self.shuttle_client.shuttles.items():
            state = self.shuttle_states.get(ip, {
                "battery": None,
                "status": "offline",
                "errors": None
            })
            
            shuttles.append({
                "ip": ip,
                "name": f"Shuttle-{ip.split('.')[-1]}",
                "cell": shuttle.get("cell", "Unknown"),
                "warehouse": shuttle.get("warehouse", "Unknown"),
                "status": state["status"],
                "battery": state["battery"]
            })
        
        return web.json_response({"shuttles": shuttles})
    
    async def get_shuttle_for_telegram(self, request):
        """API для телеграм-бота - информация о шаттле"""
        ip = request.match_info['ip']
        
        # Поддержка поиска по имени шаттла
        if not ip.replace(".", "").isdigit():
            # Ищем по имени
            for shuttle_ip, shuttle in self.shuttle_client.shuttles.items():
                if f"Shuttle-{shuttle_ip.split('.')[-1]}" == ip:
                    ip = shuttle_ip
                    break
        
        if ip not in self.shuttle_client.shuttles:
            return web.json_response({"error": "Shuttle not found"}, status=404)
        
        # Отправляем команды для получения актуальной информации
        await self.shuttle_client.send_command(ip, "STATUS")
        await asyncio.sleep(0.5)
        await self.shuttle_client.send_command(ip, "BATTERY")
        await asyncio.sleep(0.5)
        await self.shuttle_client.send_command(ip, "WDH")
        await asyncio.sleep(0.5)
        await self.shuttle_client.send_command(ip, "WLH")
        
        # Ждем немного для получения ответов
        await asyncio.sleep(1)
        
        shuttle = self.shuttle_client.shuttles[ip]
        state = self.shuttle_states.get(ip, {
            "battery": None,
            "status": "offline",
            "errors": None,
            "wdh": None,
            "wlh": None
        })
        
        return web.json_response({
            "ip": ip,
            "name": f"Shuttle-{ip.split('.')[-1]}",
            "cell": shuttle.get("cell", "Unknown"),
            "warehouse": shuttle.get("warehouse", "Unknown"),
            "status": state["status"],
            "battery": state["battery"],
            "wdh": state.get("wdh", "Unknown"),
            "wlh": state.get("wlh", "Unknown"),
            "errors": state["errors"]
        })
    
    async def update_shuttle_state(self, ip: str, key: str, value: str):
        """Обновить состояние шаттла"""
        if ip not in self.shuttle_states:
            self.shuttle_states[ip] = {
                "battery": None,
                "status": "online",  # Если получаем ответ, значит шаттл онлайн
                "errors": None,
                "last_response": None,
                "responses": []  # Список последних ответов
            }
        
        # Обновляем состояние
        self.shuttle_states[ip][key] = value
        self.shuttle_states[ip]["last_response"] = asyncio.get_event_loop().time()
        
        # Если получили ошибку, обновляем статус
        if key == "errors" and value:
            self.shuttle_states[ip]["status"] = "error"
        
        logger.debug(f"Updated shuttle {ip} state: {key}={value}")
    
    async def add_shuttle_response(self, ip: str, response: str):
        """Добавить ответ от шаттла в историю"""
        if ip not in self.shuttle_states:
            await self.update_shuttle_state(ip, "status", "online")
        
        # Добавляем ответ в конец списка (старые вначале, новые в конце)
        if "responses" not in self.shuttle_states[ip]:
            self.shuttle_states[ip]["responses"] = []
            
        self.shuttle_states[ip]["responses"].append({
            "text": response,
            "time": asyncio.get_event_loop().time()
        })
        
        # Ограничиваем список последними 20 ответами
        if len(self.shuttle_states[ip]["responses"]) > 20:
            self.shuttle_states[ip]["responses"] = self.shuttle_states[ip]["responses"][-20:]
        
        logger.debug(f"Added response for shuttle {ip}: {response}")
    
    async def periodic_status_check(self):
        """Периодическая проверка статуса шаттлов"""
        while True:
            try:
                # Перезагружаем конфигурацию для получения актуального списка
                self.shuttle_client.load_shuttles_config()
                
                for ip in self.shuttle_client.shuttles:
                    # Проверяем, когда был последний ответ
                    last_response = self.shuttle_states.get(ip, {}).get("last_response")
                    current_time = asyncio.get_event_loop().time()
                    
                    # Если нет ответа более 30 секунд, считаем шаттл оффлайн
                    if last_response is None or current_time - last_response > 30:
                        if ip in self.shuttle_states:
                            self.shuttle_states[ip]["status"] = "offline"
                    
                    # Отправляем команды для обновления состояния
                    await self.shuttle_client.send_command(ip, "STATUS")
                    await asyncio.sleep(0.5)
                    await self.shuttle_client.send_command(ip, "BATTERY")
                
                # Ждем 10 секунд перед следующей проверкой
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in periodic status check: {e}")
                await asyncio.sleep(10)
    
    async def start(self):
        """Запустить веб-сервер"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        
        logger.info(f"Starting web server on http://{self.host}:{self.port}")
        await site.start()
        
        # Запускаем периодическую проверку статуса
        asyncio.create_task(self.periodic_status_check())
        
        return runner
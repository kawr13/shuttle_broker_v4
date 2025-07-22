# import asyncio
# import logging
# import os
# from aiogram import Bot, Dispatcher, types
# from aiogram.contrib.fsm_storage.memory import MemoryStorage
# from aiogram.dispatcher import FSMContext
# from aiogram.dispatcher.filters.state import State, StatesGroup
# from aiogram.types import ParseMode
# import aiohttp

# logger = logging.getLogger(__name__)

# class ShuttleBot:
#     def __init__(self, token: str, api_url: str = "http://localhost:8080"):
#         self.token = token
#         self.api_url = api_url
#         self.bot = Bot(token=token)
#         self.storage = MemoryStorage()
#         self.dp = Dispatcher(self.bot, storage=self.storage)
#         self.setup_handlers()
        
#     def setup_handlers(self):
#         """Настройка обработчиков сообщений"""
#         self.dp.register_message_handler(self.cmd_start, commands=["start", "help"])
#         self.dp.register_message_handler(self.cmd_list, commands=["list"])
#         self.dp.register_message_handler(self.cmd_status, commands=["status"])
#         self.dp.register_message_handler(self.cmd_shuttle, lambda msg: msg.text.startswith("/shuttle"))
        
#     async def cmd_start(self, message: types.Message):
#         """Обработчик команды /start и /help"""
#         await message.answer(
#             "👋 Привет! Я бот для мониторинга шаттлов.\n\n"
#             "Доступные команды:\n"
#             "/list - Список всех шаттлов\n"
#             "/status - Общий статус системы\n"
#             "/shuttle <ip или имя> - Информация о конкретном шаттле"
#         )
        
#     async def cmd_list(self, message: types.Message):
#         """Обработчик команды /list"""
#         try:
#             async with aiohttp.ClientSession() as session:
#                 async with session.get(f"{self.api_url}/api/telegram/shuttles") as response:
#                     if response.status == 200:
#                         data = await response.json()
#                         shuttles = data.get("shuttles", [])
                        
#                         if not shuttles:
#                             await message.answer("⚠️ Шаттлы не найдены")
#                             return
                        
#                         text = "📋 Список шаттлов:\n\n"
#                         for shuttle in shuttles:
#                             status_emoji = "🟢" if shuttle["status"] == "online" else "🔴"
#                             battery = f" 🔋 {shuttle['battery']}" if shuttle.get("battery") else ""
#                             text += f"{status_emoji} {shuttle['name']} ({shuttle['ip']}){battery}\n"
#                             text += f"   Ячейка: {shuttle['cell']}, Склад: {shuttle['warehouse']}\n\n"
                        
#                         await message.answer(text)
#                     else:
#                         await message.answer(f"⚠️ Ошибка получения списка шаттлов: {response.status}")
#         except Exception as e:
#             logger.error(f"Error in cmd_list: {e}")
#             await message.answer(f"⚠️ Ошибка: {e}")
            
#     async def cmd_status(self, message: types.Message):
#         """Обработчик команды /status"""
#         try:
#             async with aiohttp.ClientSession() as session:
#                 async with session.get(f"{self.api_url}/api/telegram/shuttles") as response:
#                     if response.status == 200:
#                         data = await response.json()
#                         shuttles = data.get("shuttles", [])
                        
#                         if not shuttles:
#                             await message.answer("⚠️ Шаттлы не найдены")
#                             return
                        
#                         online_count = sum(1 for s in shuttles if s["status"] == "online")
#                         error_count = sum(1 for s in shuttles if s["status"] == "error")
#                         offline_count = sum(1 for s in shuttles if s["status"] == "offline")
                        
#                         text = "📊 Статус системы:\n\n"
#                         text += f"Всего шаттлов: {len(shuttles)}\n"
#                         text += f"🟢 Онлайн: {online_count}\n"
#                         text += f"🟡 С ошибками: {error_count}\n"
#                         text += f"🔴 Оффлайн: {offline_count}\n\n"
                        
#                         if error_count > 0:
#                             text += "⚠️ Шаттлы с ошибками:\n"
#                             for shuttle in shuttles:
#                                 if shuttle["status"] == "error":
#                                     text += f"- {shuttle['name']} ({shuttle['ip']})\n"
                        
#                         await message.answer(text)
#                     else:
#                         await message.answer(f"⚠️ Ошибка получения статуса: {response.status}")
#         except Exception as e:
#             logger.error(f"Error in cmd_status: {e}")
#             await message.answer(f"⚠️ Ошибка: {e}")
            
#     async def cmd_shuttle(self, message: types.Message):
#         """Обработчик команды /shuttle <ip или имя>"""
#         try:
#             # Извлекаем IP или имя шаттла из сообщения
#             parts = message.text.split(maxsplit=1)
#             if len(parts) < 2:
#                 await message.answer("⚠️ Укажите IP или имя шаттла: /shuttle <ip или имя>")
#                 return
                
#             shuttle_id = parts[1].strip()
            
#             async with aiohttp.ClientSession() as session:
#                 async with session.get(f"{self.api_url}/api/telegram/shuttle/{shuttle_id}") as response:
#                     if response.status == 200:
#                         data = await response.json()
                        
#                         status_emoji = "🟢" if data["status"] == "online" else "🔴"
#                         battery = f"🔋 Батарея: {data['battery']}\n" if data.get("battery") else ""
#                         wdh = f"⏱️ Часы привода: {data['wdh']}\n" if data.get("wdh") else ""
#                         wlh = f"⚙️ Состояние подъема: {data['wlh']}\n" if data.get("wlh") else ""
#                         errors = f"⚠️ Ошибки: {data['errors']}\n" if data.get("errors") else ""
                        
#                         text = f"📌 Информация о шаттле {data['name']}:\n\n"
#                         text += f"{status_emoji} Статус: {data['status']}\n"
#                         text += f"🌐 IP: {data['ip']}\n"
#                         text += f"📍 Ячейка: {data['cell']}\n"
#                         text += f"🏭 Склад: {data['warehouse']}\n"
#                         text += battery
#                         text += wdh
#                         text += wlh
#                         text += errors
                        
#                         await message.answer(text)
#                     elif response.status == 404:
#                         await message.answer(f"⚠️ Шаттл {shuttle_id} не найден")
#                     else:
#                         await message.answer(f"⚠️ Ошибка получения информации о шаттле: {response.status}")
#         except Exception as e:
#             logger.error(f"Error in cmd_shuttle: {e}")
#             await message.answer(f"⚠️ Ошибка: {e}")
    
#     async def start(self):
#         """Запуск бота"""
#         logger.info("Starting Telegram bot")
#         await self.dp.start_polling()
        
#     async def stop(self):
#         """Остановка бота"""
#         logger.info("Stopping Telegram bot")
#         await self.dp.storage.close()
#         await self.dp.storage.wait_closed()
#         await self.bot.session.close()
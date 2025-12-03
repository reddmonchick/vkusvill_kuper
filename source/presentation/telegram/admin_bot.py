import asyncio
import uuid
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from source.core.dto import Task, ParseResult
from source.core.config import settings
from source.utils.csv_exporter import result_to_csv_bytes
import redis.asyncio as redis
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
bot = Bot(token=settings.TG_BOT_TOKEN)
dp = Dispatcher()

VKUSVILL_ALLOWED_CITIES = {"москва", "санкт-петербург", "спб", "питер", "новосибирск", "екатеринбург", "казань"}

async def results_listener():
    r = redis.from_url(settings.REDIS_URL)
    processed = set()
    while True:
        try:
            msgs = await r.xread({settings.OUTPUT_STREAM: "0"}, count=10, block=5000)
            for _, messages in msgs:
                for msg_id, data in messages:
                    if msg_id in processed:
                        await r.xdel(settings.OUTPUT_STREAM, msg_id)
                        continue
                    result = ParseResult.model_validate_json(data[b"data"].decode())
                    if not result.products:
                        processed.add(msg_id)
                        await r.xdel(settings.OUTPUT_STREAM, msg_id)
                        continue

                    document = BufferedInputFile(
                        result_to_csv_bytes(result),
                        filename=f"{result.service}_{result.mode}_{result.task_id}.csv"
                    )
                    await bot.send_document(
                        chat_id=result.chat_id,
                        document=document,
                        caption=f"Готово за {result.took_seconds:.1f}с | {result.service.upper()} • {result.mode} | {len(result.products)} товаров"
                    )
                    processed.add(msg_id)
                    await r.xdel(settings.OUTPUT_STREAM, msg_id)
        except Exception as e:
            logger.error(f"Listener error: {e}")
            await asyncio.sleep(3)

@dp.message(Command("parse"))
async def parse_command(message: Message):
    args = message.text.split()
    if len(args) < 4:
        return await message.answer(
            "Примеры:\n"
            "/parse vkusvill fast Москва\n"
            "/parse vkusvill fast 55.75,37.61\n"
            "/parse kuper fast Саратов Лента"
        )

    service = args[1].lower()
    mode = args[2].lower()

    if service not in ["vkusvill", "kuper"]:
        return await message.answer("Сервис: vkusvill или kuper")
    if mode not in ["fast", "heavy"]:
        return await message.answer("Режим: fast или heavy")

    store = None
    if service == "kuper" and len(args) >= 5:
        store = args[-1].lower()
        args = args[:-1]

    location = " ".join(args[3:]).strip()
    if not location:
        return await message.answer("Укажите город или координаты")

    coord_match = re.match(r"^([-\d.]+)[\s,]+([-\d.]+)$", location)
    if coord_match:
        city_input = f"{coord_match.group(1)},{coord_match.group(2)}"
    else:
        city_input = location

    task = Task(
        task_id=str(uuid.uuid4())[:8],
        service=service,
        mode=mode,
        city=city_input,     
        store=store,    
        limit=5000,
        user_id=message.from_user.id,
        chat_id=message.chat.id
    )

    r = redis.from_url(settings.REDIS_URL)
    await r.xadd(settings.INPUT_STREAM, {"data": task.model_dump_json()})

    store_text = f" | Магазин: {store.title()}" if store else ""
    location_text = city_input

    await message.answer(
        f"Задача запущена!\n"
        f"Сервис: {service.title()}\n"
        f"Режим: {mode}\n"
        f"Локация: {location_text}{store_text}\n"
        f"ID: <code>{task.task_id}</code>",
        parse_mode="HTML"
    )

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я парсер готовой еды\n\n"
        "Команда:\n"
        "/parse vkusvill fast Москва\n"
        "/parse vkusvill heavy Москва\n"
        "/parse kuper fast Саратов Ашан\n"
        "/parse kuper heavy Казань Перекрёсток\n\n"
        "Для Купера — любой город России!"
    )

async def main():
    logger.info("Bot started")
    await asyncio.gather(
        dp.start_polling(bot),
        results_listener()
    )

if __name__ == "__main__":
    asyncio.run(main())
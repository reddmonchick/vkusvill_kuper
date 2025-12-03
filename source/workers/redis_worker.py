import asyncio
import time
import logging
import redis.asyncio as redis
from aiogram import Bot

from source.core.config import settings
from source.infra.vkusvill import VkusvillParser
from source.infra.kuper import KuperParser
from source.core.dto import Task, ParseResult

logger = logging.getLogger("redis_worker")
logger.setLevel(logging.INFO)

bot = Bot(token=settings.TG_BOT_TOKEN)
parsers = {"vkusvill": VkusvillParser(), "kuper": KuperParser()}

async def initialize_proxies(r: redis.Redis):
    key = "vkusvill:proxies:free"
    await r.delete(key)
    proxies = settings.VKUSVILL_PROXY_LIST
    if proxies:
        await r.rpush(key, *proxies)
        logger.info(f"В Redis загружено {len(proxies)} прокси для ВкусВилл")
    else:
        logger.warning("Список прокси пуст! Парсер будет работать с локального IP.")

async def process_task(task: Task, r: redis.Redis) -> ParseResult:
    logger.info("Новая задача | id=%s | %s %s | user=%s",
                task.task_id, task.service, task.mode, task.user_id)

    start = time.time()
    try:
        parser = parsers[task.service]
        
        if task.service == "vkusvill":
            result = await parser.parse(task, redis_client=r)
        else:
            result = await parser.parse(task)

        result.took_seconds = round(time.time() - start, 1)
        logger.info("Задача завершена | id=%s | товаров=%d | время=%.1fс",
                    task.task_id, len(result.products), result.took_seconds)
        return result
    except Exception as e:
        logger.error("Ошибка обработки задачи %s: %s", task.task_id, e, exc_info=True)
        raise

async def main():
    logger.info("Redis Worker запущен")
    r = redis.from_url(settings.REDIS_URL, decode_responses=False)
    await r.ping()
    logger.info("Подключено к Redis")

    await initialize_proxies(r)

    while True:
        try:
            msgs = await r.xread({settings.INPUT_STREAM: "$"}, count=1, block=5000)
            if not msgs:
                continue

            for _, messages in msgs:
                for msg_id, fields in messages:
                    raw = fields.get(b"data")
                    if not raw:
                        await r.xack(settings.INPUT_STREAM, "food_group", msg_id)
                        continue

                    task = Task.model_validate_json(raw.decode())
                    
                    result = await process_task(task, r)

                    await r.xadd(settings.OUTPUT_STREAM, {"data": result.model_dump_json()})
                    await r.xack(settings.INPUT_STREAM, "food_group", msg_id)

        except Exception as e:
            logger.error("Критическая ошибка в worker: %s", e, exc_info=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
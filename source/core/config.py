import os
from pydantic_settings import BaseSettings
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()  

class Settings(BaseSettings):
    model_config = {
        "env_file": ".env",       
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }

    TG_BOT_TOKEN: str
    REDIS_URL: str = "redis://redis:6379/0"

    VKUSVILL_PROXIES: str = ""

    @property
    def VKUSVILL_PROXY_LIST(self) -> List[str]:
        if not self.VKUSVILL_PROXIES:
            return []
        return [p.strip() for p in self.VKUSVILL_PROXIES.split(",") if p.strip()]

    VKUSVILL_CITY_COORDS: Dict[str, tuple[float, float]] = {
        "москва": (55.7558, 37.6173),
        "санкт-петербург": (59.9343, 30.3351),
        "спб": (59.9343, 30.3351),
        "питер": (59.9343, 30.3351),
        "новосибирск": (55.0084, 82.9357),
        "екатеринбург": (56.8389, 60.6057),
        "казань": (55.8304, 49.0661),
    }

    DATA_DIR: str = "source/data"
    INPUT_STREAM: str = "food_parse_tasks"
    OUTPUT_STREAM: str = "food_parse_results"

settings = Settings()
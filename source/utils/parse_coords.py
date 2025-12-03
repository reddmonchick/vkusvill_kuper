import re
from typing import Optional

def parse_city_or_coords(city_str: str) -> tuple[str, Optional[float], Optional[float]]:
    if re.match(r"^[-.\d]+[,;\s]+[-.\d]+$", city_str.strip()):
        try:
            lat_str, lon_str = re.split(r"[,;\s]+", city_str.strip(), 1)
            return ("координаты", float(lat_str), float(lon_str))
        except:
            pass
    return (city_str.strip().lower(), None, None)
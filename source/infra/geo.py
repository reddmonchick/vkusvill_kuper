from async_tls_client.session.session import AsyncSession
import logging

logger = logging.getLogger(__name__)

async def get_coords_by_city(city_name: str) -> tuple[float, float]:
    session = AsyncSession(client_identifier="chrome_120", random_tls_extension_order=True)
    headers = {
        'client-id': 'KuperAndroid',
        'client-ver': '15.1.29',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'client-bundleid': 'ru.instamart',
        'api-version': '2.2',
        'cache-control': 'no-store',
        'content-type': 'application/json',
        'anonymousid': '03d2c216ac476531',
        'backenduseruuid': '',
        'user-uuid': '',
        'screenname': 'MapScreen',
    }
    try:
        resp = await session.get(
            "https://catalog.api.2gis.com/3.0/suggests",
            params={
                "key": "rutvqk5607",
                "q": city_name,
                "type": "building,street,adm_div.city",
                "suggest_type": "address",
                "fields": "items.full_address_name,items.address,items.adm_div,items.point",
                "location": "37.584212,55.645531"
            },
            headers=headers
        )
        data = resp.json()
        point = data.get("result", {}).get("items", [{}])[0].get("point", {})
        lat = float(point.get("lat", 55.7558))
        lon = float(point.get("lon", 37.6173))
        logger.error(f"2GIS: {city_name} → {lat}, {lon}")

        return lat, lon
    except Exception as e:
        logger.error(f"2GIS ошибка для {city_name}: {e}")
        return 55.7558, 37.6173
    finally:
        await session.close()
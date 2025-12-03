from async_tls_client.session.session import AsyncSession
import asyncio
import random

class TLSClient:
    def __init__(self, proxy: str = None):
        self.proxy = proxy
        self.client = None

    async def __aenter__(self):
        self.client = AsyncSession(
            client_identifier=random.choice([
                "chrome_120"
            ]),
            random_tls_extension_order=True
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client:
            await self.client.close()

    async def get(self, url: str, params: dict = None, headers: dict = None):
        kwargs = {"url": url}
        if params:
            kwargs["params"] = params
        if headers:
            kwargs["headers"] = headers
        if self.proxy:
            kwargs["proxy"] = self.proxy
        response = await self.client.get(**kwargs)
        return response

    async def post(self, url: str, data: dict = None, headers: dict = None):
        kwargs = {"url": url}
        if data:
            kwargs["json"] = data
        if headers:
            kwargs["headers"] = headers
        if self.proxy:
            kwargs["proxy"] = self.proxy
        response = await self.client.post(**kwargs)
        return response
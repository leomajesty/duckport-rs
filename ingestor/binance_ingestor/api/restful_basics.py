import aiohttp

from .exceptions import BinanceRequestException, BinanceAPIException
from binance_ingestor.config import proxy


class BinanceBaseApi:

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    @staticmethod
    async def _handle_response(response: aiohttp.ClientResponse):
        if not str(response.status).startswith('2'):
            raise BinanceAPIException(response, response.status, await response.text())
        try:
            return await response.json()
        except ValueError:
            txt = await response.text()
            raise BinanceRequestException(f'Invalid Response: {txt}')

    async def _aio_get(self, url, params):
        if params is None:
            params = {}
        async with self.session.get(url, params=params, proxy=proxy) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        async with self.session.post(url, data=params, proxy=proxy) as resp:
            return await self._handle_response(resp)

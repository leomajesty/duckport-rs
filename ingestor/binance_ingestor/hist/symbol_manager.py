"""
Symbol management — list and filter Binance trading pairs from the public S3 index.
"""

import asyncio
import aiohttp
from lxml import objectify

from binance_ingestor.utils.log_kit import logger
from binance_ingestor.config import root_center_url, proxy, blind


async def request_session(session, params):
    while True:
        try:
            async with session.get(root_center_url, params=params, proxy=proxy, timeout=20) as response:
                return await response.text()
        except aiohttp.ClientError as ae:
            logger.warning(f'请求失败，继续重试, 错误信息:{ae}')
        except Exception as e:
            logger.warning(f'请求失败，继续重试:{e}')


async def request_session_4_list(session, params):
    result = []
    while True:
        try:
            async with session.get(root_center_url, params=params, proxy=proxy, timeout=20) as response:
                data = await response.text()
                root = objectify.fromstring(data.encode('UTF-8'))
                result.append(root)

                if root.IsTruncated:
                    params = {
                        'delimiter': '/',
                        'prefix': root.Prefix.text,
                        'marker': root.NextMarker.text
                    }
                    continue
                else:
                    return result
        except aiohttp.ClientError as ae:
            if not blind:
                logger.error('请求失败，继续重试', ae)
        except Exception as e:
            if not blind:
                logger.error('请求失败，继续重试', e)


async def get_symbols(params):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        return await get_symbols_by_session(session, params)


async def get_symbols_by_session(session, params):
    data = await request_session(session, params)
    root = objectify.fromstring(data.encode('UTF-8'))
    result = []
    for item in root.CommonPrefixes:
        param = item.Prefix
        s = param.text.split('/')
        result.append(s[len(s) - 2])
    if root.IsTruncated:
        params['marker'] = root.NextMarker.text
        next_page = await get_symbols_by_session(session, params)
        result.extend(next_page)
    return result


def async_get_all_symbols(params):
    return asyncio.run(get_symbols(params))


def async_get_usdt_symbols(params):
    all_symbols = async_get_all_symbols(params)
    usdt = set()
    [usdt.add(i) for i in all_symbols if i.endswith('USDT')]
    return usdt


def spot_symbols_filter(symbols):
    others = []
    stable_symbol = ['BKRW', 'USDC', 'USDP', 'TUSD', 'BUSD', 'FDUSD', 'DAI', 'EUR', 'GBP', 'AEUR']
    stable_symbols = [s + 'USDT' for s in stable_symbol]
    special_symbols = ['JUPUSDT']
    pure_spot_symbols = []

    for symbol in symbols:
        if symbol in special_symbols:
            pure_spot_symbols.append(symbol)
            continue
        if symbol.endswith('UPUSDT') or symbol.endswith('DOWNUSDT') or symbol.endswith('BULLUSDT') or symbol.endswith('BEARUSDT'):
            others.append(symbol)
            continue
        if symbol in stable_symbols:
            others.append(symbol)
            continue
        pure_spot_symbols.append(symbol)

    logger.info(f'过滤掉的现货symbol {others}')
    return pure_spot_symbols

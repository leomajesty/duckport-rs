import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict, Any

import pandas as pd

from binance_ingestor.api.binance_market_restful import create_binance_market_api
from binance_ingestor.api.exceptions import BinanceAPIException
from binance_ingestor.utils import async_retry_getter
from binance_ingestor.utils.log_kit import logger, divider


def _check_from_permission_sets(permission_sets, permission_name):
    for permission_set in permission_sets:
        if permission_name in permission_set:
            return True
    return False

def _get_from_filters(filters, filter_type, field_name):
    for f in filters:
        if f['filterType'] == filter_type:
            return f[field_name]


def _parse_usdt_futures_syminfo(info):
    filters = info['filters']
    price_tick = _get_from_filters(filters, 'PRICE_FILTER', 'tickSize') or '0.01'
    lot_size = _get_from_filters(filters, 'LOT_SIZE', 'stepSize') or '0.001'
    min_notional = _get_from_filters(filters, 'MIN_NOTIONAL', 'notional') or '5'

    return {
        'market': 'usdt_perp',
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(price_tick),
        'lot_size': Decimal(lot_size),
        'min_notional_value': Decimal(min_notional),
        'pre_market': None
    }

def _parse_coin_futures_syminfo(info):
    filters = info['filters']
    return {
        'market': 'coin_perp',
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['contractStatus'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(info['contractSize']),
        'pre_market': None,
    }


def _parse_spot_syminfo(info):
    filters = info['filters']
    permission_sets = info.get('permissionSets', [])
    price_tick = _get_from_filters(filters, 'PRICE_FILTER', 'tickSize') or '0.01'
    lot_size = _get_from_filters(filters, 'LOT_SIZE', 'stepSize') or '0.001'
    min_notional = _get_from_filters(filters, 'NOTIONAL', 'minNotional') or '10'

    return {
        'market': 'usdt_spot',
        'symbol': info['symbol'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'price_tick': Decimal(price_tick),
        'lot_size': Decimal(lot_size),
        'min_notional_value': Decimal(min_notional),
        'pre_market': _check_from_permission_sets(permission_sets, 'PRE_MARKET'),
        'contract_type': None,
        'margin_asset': None,
    }

class BinanceFetcher:

    TYPE_MAP = {
        'usdt_perp': _parse_usdt_futures_syminfo,
        'coin_perp': _parse_coin_futures_syminfo,
        'usdt_spot': _parse_spot_syminfo,
    }

    def __init__(self, type_, session):
        self.trade_type = type_
        self.market_api = create_binance_market_api(type_, session)

        if type_ in self.TYPE_MAP:
            self.syminfo_parse_func = self.TYPE_MAP[type_]
        else:
            raise ValueError(f'Type {type_} not supported')

    def get_api_limits(self) -> tuple[int, int]:
        return self.market_api.MAX_MINUTE_WEIGHT, self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES

    async def get_time_and_weight(self) -> tuple[pd.Timestamp, int]:
        server_timestamp, weight = await self.market_api.aioreq_time_and_weight()
        server_timestamp = pd.to_datetime(server_timestamp, unit='ms', utc=True)
        return server_timestamp, weight

    async def get_exchange_info(self) -> dict[str, dict]:
        exg_info = await async_retry_getter(self.market_api.aioreq_exchange_info)
        results = dict()
        for info in exg_info['symbols']:
            results[info['symbol']] = self.syminfo_parse_func(info)
        return results

    async def get_candle(self, symbol, interval, limit=499, end_timestamp=None, **kwargs) -> pd.DataFrame:
        if end_timestamp:
            data = await async_retry_getter(self.market_api.aioreq_klines, symbol=symbol, interval=interval,
                                            limit=limit, endTime=end_timestamp, **kwargs)
        else:
            data = await async_retry_getter(self.market_api.aioreq_klines, symbol=symbol, interval=interval,
                                            limit=limit, **kwargs)
        columns = [
            'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ]
        df = pd.DataFrame(data, columns=columns)
        df.drop(columns=['ignore', 'close_time'], inplace=True)
        df['open_time'] = pd.to_datetime(df['open_time'].astype('int64'), unit='ms', utc=True)
        for col in [
                'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                'taker_buy_quote_asset_volume'
        ]:
            df[col] = df[col].astype(float)

        df['avg_price'] = df['quote_volume'] / df['volume']
        df['symbol'] = symbol
        df = df[['open_time', 'symbol', 'open', 'high', 'low', 'close', 'volume',
                 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                 'avg_price']]

        return df

    async def get_funding_rate(self) -> pd.DataFrame:
        if self.trade_type == 'spot':
            raise RuntimeError('Cannot request funding rate for spot')
        data = await self.market_api.aioreq_premium_index()
        data = [{
            'symbol': d['symbol'],
            'fundingRate': pd.to_numeric(d['lastFundingRate'], errors='coerce')
        } for d in data]
        df = pd.DataFrame.from_records(data)
        return df


@dataclass
class WeightManager:
    max_weight: int
    efficient_weight: int
    current_weight: int = 0
    last_reset_time: float = 0

    def can_make_request(self) -> bool:
        current_time = time.time()
        if current_time - self.last_reset_time >= 60:
            self.current_weight = 0
            self.last_reset_time = current_time
        return self.current_weight + self.efficient_weight <= self.max_weight

    def add_weight(self, weight: int = None):
        if weight is None:
            weight = self.efficient_weight
        self.current_weight += weight

    def get_wait_time(self) -> float:
        if self.can_make_request():
            return 0
        current_time = time.time()
        time_since_reset = current_time - self.last_reset_time
        wait_time = 60 - time_since_reset
        return max(0, int(wait_time) + 1)


class OptimizedKlineFetcher:

    def __init__(self, fetcher: BinanceFetcher, max_concurrent: int = 10):
        self.fetcher = fetcher
        self.max_concurrent = max_concurrent
        self.weight_manager = WeightManager(*fetcher.get_api_limits())
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def await_for_weight(self):
        wait_time = self.weight_manager.get_wait_time()
        if wait_time > 0:
            logger.warning(f'等待{wait_time}秒权重恢复')
            await asyncio.sleep(wait_time)

    async def get_kline(self, symbol: str, interval: str, limit: int, end_timestamp = None, **kwargs) -> pd.DataFrame:
        try:
            data = await self.fetcher.get_candle(symbol=symbol, interval=interval, limit=limit,
                                                 end_timestamp=end_timestamp)
        except BinanceAPIException as e:
            if e.code == -1003:
                self.weight_manager.add_weight(self.weight_manager.max_weight)
                await self.await_for_weight()
                return await self.get_kline(symbol=symbol, interval=interval, limit=limit, end_timestamp=end_timestamp)
            else:
                raise e
        return data

    async def get_klines_with_retry(self, symbol: str, interval: str = '1m', limit=499, end_timestamp = None, max_retries: int = 3) -> Dict[str, Any]:
        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    await self.await_for_weight()
                    self.weight_manager.add_weight()
                    data = await self.get_kline(symbol=symbol, interval=interval, limit=limit, end_timestamp=end_timestamp)

                    return {
                        'symbol': symbol,
                        'data': data,
                        'num': data.shape[0],
                        'begin_time': data['open_time'].min(),
                        'success': True,
                        'attempt': attempt + 1
                    }

            except Exception as e:
                if attempt == max_retries - 1:
                    return {
                        'symbol': symbol,
                        'error': str(e),
                        'success': False,
                        'attempt': attempt + 1
                    }
                else:
                    await asyncio.sleep(1 * (attempt + 1))

        return {'symbol': symbol, 'success': False, 'error': 'Max retries exceeded'}

    async def get_all_klines(self, symbols: List[str], interval: str = '1m', start_time = None, limit = 499) -> List[Dict[str, Any]]:
        logger.info(f"start fetching {len(symbols)} symbols...")

        interval_delta = pd.Timedelta(interval)
        fetch_symbols = symbols.copy()
        results = []
        last_begin_time = dict()
        num = 0
        while fetch_symbols:
            num += 1
            if start_time:
                divider(f"Fetch historical klines, round{num}")
            tasks = []
            for symbol in fetch_symbols:
                end_timestamp = None
                if symbol in last_begin_time:
                    end_timestamp = (last_begin_time[symbol] - interval_delta).value // 1000000
                t = self.get_klines_with_retry(symbol=symbol, interval=interval, limit=limit, end_timestamp=end_timestamp)
                tasks.append(t)

            current_batch_result = []
            for coro in asyncio.as_completed(tasks):
                result = await coro
                current_batch_result.append(result)

                count = sum(1 for _ in current_batch_result)
                if count % 100 == 0:
                    logger.debug(f'[Restful] {self.fetcher.trade_type} - process {count} / {len(fetch_symbols)}')

            results.extend(current_batch_result)
            logger.ok(f'[Restful] {self.fetcher.trade_type} - process done. total: {len(fetch_symbols)}')
            failed = [r for r in results if not r.get('success', False)]

            if failed:
                logger.warning("\nFailed Symbols:")
                for f in failed[:10]:
                    logger.warning(f"  {f['symbol']}: {f.get('error', 'Unknown error')}")
                if len(failed) > 10:
                    logger.critical(f"  ... and {len(failed) - 10} more failures")
            else:
                succeed = [r for r in results if r.get('success', False)]
                if start_time:
                    for s in succeed:
                        con1 = s.get('begin_time') > start_time
                        con2 = s.get('num') == limit
                        if con1 and con2:
                            last_begin_time[s.get('symbol')] = s.get('begin_time')
                        elif s.get('symbol') in fetch_symbols:
                            fetch_symbols.remove(s.get('symbol'))
                else:
                    [fetch_symbols.remove(s.get('symbol')) for s in succeed]

        return results

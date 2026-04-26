import asyncio
from datetime import datetime
from typing import Dict, Any

import pandas as pd

from binance_ingestor.api import (get_coin_futures_multi_candlesticks_socket, get_spot_multi_candlesticks_socket,
                                   get_usdt_futures_multi_candlesticks_socket)
from binance_ingestor.bus import TRADE_TYPE_MAP
from binance_ingestor.component.candle_fetcher import BinanceFetcher
from binance_ingestor.utils import convert_interval_to_timedelta, get_logger, create_aiohttp_session
from binance_ingestor.config import KLINE_INTERVAL, FETCH_CONCURRENCY
from binance_ingestor.utils.log_kit import logger, divider
from binance_ingestor.utils.time import now_time, async_sleep_until_run_time, next_run_time


def convert_to_dataframe(x, interval_delta):
    columns = [
        'open_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
    ]
    candle_data = [
        pd.to_datetime(int(x['t']), unit='ms', utc=True),
        float(x['o']),
        float(x['h']),
        float(x['l']),
        float(x['c']),
        float(x['v']),
        float(x['q']),
        int(x['n']),
        float(x['V']),
        float(x['Q'])
    ]
    return pd.DataFrame(data=[candle_data], columns=columns, index=[candle_data[0] + interval_delta])


class CandleListener:

    TRADE_TYPE_MAP = {
        'usdt_perp': get_usdt_futures_multi_candlesticks_socket,
        'coin_perp': get_coin_futures_multi_candlesticks_socket,
        'usdt_spot': get_spot_multi_candlesticks_socket
    }

    def __init__(self, type_, symbols, time_interval, que):
        self.trade_type = type_
        self.symbols = set(symbols)
        self.time_interval = time_interval
        self.interval_delta = convert_interval_to_timedelta(time_interval)
        self.que: asyncio.Queue = que
        self.req_reconnect = False

    async def start_listen(self):
        if not self.symbols:
            return

        socket_func = self.TRADE_TYPE_MAP[self.trade_type]
        while True:
            socket = socket_func(self.symbols, self.time_interval)
            async with socket as socket_conn:
                while True:
                    if self.req_reconnect:
                        self.req_reconnect = False
                        break
                    try:
                        res = await socket_conn.recv()
                        self.handle_candle_data(res)
                    except asyncio.TimeoutError:
                        get_logger().error('Recv candle ws timeout, reconnecting')
                        break

    def handle_candle_data(self, res):
        if 'data' not in res:
            return
        data = res['data']
        if data.get('e', None) != 'kline' or 'k' not in data:
            return
        candle = data['k']
        is_closed = candle.get('x', False)
        if not is_closed:
            return

        df_candle = convert_to_dataframe(candle, self.interval_delta)

        self.que.put_nowait({
            'type': 'candle_data',
            'data': df_candle,
            'closed': is_closed,
            'run_time': df_candle.index[0],
            'symbol': data['s'],
            'time_interval': self.time_interval,
            'trade_type': self.trade_type,
            'recv_time': now_time()
        })

    def add_symbols(self, *symbols):
        for symbol in symbols:
            self.symbols.add(symbol)

    def remove_symbols(self, *symbols):
        for symbol in symbols:
            if symbol in self.symbols:
                self.symbols.remove(symbol)

    def reconnect(self):
        self.req_reconnect = True


class MarketListener:

    def __init__(self, market='usdt_perp', data_callback=None, exginfo_callback=None):
        self.main_queue = asyncio.Queue()
        self.market = market
        self.listeners = {}
        self.symbols = None
        self._data_batches = {}
        self._batch_status = {}
        self.data_callback = data_callback
        self.exginfo_callback = exginfo_callback

    async def build_and_run(self):
        if self.symbols is None:
            self.symbols = set(await self.get_exginfo())

        self.listeners = self.create_listeners(market=self.market, symbols=self.symbols, que=self.main_queue)
        listen_tasks = [v.start_listen() for k, v in self.listeners.items()]
        dispatcher_task = self.dispatcher()
        scheduled_task = self.scheduled_run()

        logger.ok(f'{self.market} Market Listener initialized...')

        tasks = listen_tasks + [dispatcher_task, scheduled_task]

        await asyncio.gather(*tasks)

    def _collect_kline_data(self, run_time: datetime, symbol: str, kline_data: Dict[str, Any]) -> None:
        try:
            if run_time not in self._data_batches:
                self._data_batches[run_time] = {}
                self._batch_status[run_time] = {
                    'collected_symbols': set(),
                }

            self._data_batches[run_time][symbol] = kline_data
            self._batch_status[run_time]['collected_symbols'].add(symbol)

            collected = len(self._batch_status[run_time]['collected_symbols'])
            if collected % 100 == 0:
                logger.debug(f"[Websocket] {self.market}, process: {collected} / {len(self.symbols)}")

        except Exception as e:
            logger.error(f"收集kline数据失败: {e}, run_time: {run_time}, symbol: {symbol}")

    def _check_batch_completeness(self, run_time: datetime) -> bool:
        try:
            if run_time not in self._batch_status:
                return False

            status = self._batch_status[run_time]
            collected_symbols = status['collected_symbols']

            is_complete = (len(collected_symbols) == len(self.symbols) and collected_symbols == self.symbols)

            if is_complete:
                logger.ok(f"[Websocket] {self.market} - process done. total: {len(collected_symbols)}")

            return is_complete

        except Exception as e:
            logger.error(f"检查批次完整性失败: {e}, run_time: {run_time}")
            return False

    async def _batch_write_to_db(self, run_time: datetime) -> None:
        if self.data_callback:
            await self.data_callback(run_time, self._data_batches[run_time], self.market)
        else:
            logger.warning('_batch_write_to_db has not callback function.')


    def _cleanup_batch_data(self, run_time: datetime) -> None:
        try:
            if run_time in self._data_batches:
                del self._data_batches[run_time]
            if run_time in self._batch_status:
                del self._batch_status[run_time]
        except Exception as e:
            logger.error(f"清理数据批次失败: {e}, run_time: {run_time}")

    async def dispatcher(self):
        while True:
            req = await self.main_queue.get()
            run_time = req['run_time']
            req_type = req['type']
            if req_type == 'candle_data':
                self._collect_kline_data(run_time, req['symbol'], req)
                if self._check_batch_completeness(run_time):
                    await self._batch_write_to_db(run_time)
                    self._cleanup_batch_data(run_time)

    async def scheduled_run(self):
        while True:
            next_time = next_run_time(KLINE_INTERVAL)
            divider(f"[{self.market} Listener] next fetch runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
            await async_sleep_until_run_time(next_time)
            symbols_trading = await self.get_exginfo()
            await asyncio.sleep(10)
            await self.update_exginfo(symbols_trading)
            await asyncio.sleep(1)

    async def get_exginfo(self):
        async with create_aiohttp_session(10) as session:
            fetcher = BinanceFetcher(self.market, session)
            if self.exginfo_callback:
                symbols_trading = await self.exginfo_callback(fetcher, self.market)
            else:
                exginfo = await fetcher.get_exchange_info()
                symbols_trading = TRADE_TYPE_MAP[self.market][0](exginfo)
        logger.info(f'Fetched {len(symbols_trading)} trading symbols from exchange info')
        return symbols_trading

    async def update_exginfo(self, symbols_trading):
        delist = self.symbols - set(symbols_trading)
        onboard = set(symbols_trading) - self.symbols

        changed_groups = set()

        if delist:
            logger.warning(f'Symbols delist: {delist}')
            for symbol in delist:
                group_id = hash(symbol) % FETCH_CONCURRENCY
                if group_id in self.listeners:
                    listener: CandleListener = self.listeners[group_id]
                    listener.remove_symbols(symbol)
                    changed_groups.add(group_id)

        if onboard:
            logger.warning(f'Symbols onboard: {onboard}')
            for symbol in onboard:
                group_id = hash(symbol) % FETCH_CONCURRENCY
                if group_id not in self.listeners:
                    self.listeners[group_id] = CandleListener(self.market, [symbol], KLINE_INTERVAL, self.main_queue)
                    logger.info(f'Created new listener group {group_id} for symbol {symbol}')
                else:
                    listener: CandleListener = self.listeners[group_id]
                    listener.add_symbols(symbol)
                changed_groups.add(group_id)

        for group_id in changed_groups:
            if group_id in self.listeners:
                listener: CandleListener = self.listeners[group_id]
                listener.reconnect()

        self.symbols = set(symbols_trading)

    @staticmethod
    def create_listeners(market, symbols, que) -> dict[int, CandleListener]:
        groups = [[] for _ in range(FETCH_CONCURRENCY)]
        for sym in symbols:
            group_id = hash(sym) % FETCH_CONCURRENCY
            groups[group_id].append(sym)
        listeners = {}
        for idx, grp in enumerate(groups):
            num = len(grp)
            if num > 0:
                logger.debug(f'Create WS listen group {idx}, {num} symbols')
                listeners[idx] = CandleListener(market, grp, KLINE_INTERVAL, que)
        return listeners

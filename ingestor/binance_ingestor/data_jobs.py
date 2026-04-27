"""
DataJobs — Binance data fetching & ingestion via duckport-rs.

Refactored from duckport/core/flight_func/flight_data_jobs.py:
  - Removed local DuckDB dependency (no db_manager)
  - Removed FlightActions / FlightGets dependency
  - duck_time is initialised from DuckportClient.read_duck_times()
  - All writes go through DuckportClient only
"""

import abc
import asyncio
import gc
import threading
from datetime import *
from typing import Dict, Optional

import pandas as pd

from binance_ingestor.duckport_client import DuckportClient
from binance_ingestor.utils import next_run_time, async_sleep_until_run_time, now_time, create_aiohttp_session
from binance_ingestor.utils.log_kit import logger, divider
from binance_ingestor.config import (
    KLINE_INTERVAL_MINUTES, KLINE_INTERVAL,
    START_DATE, GENESIS_TIME, DATA_SOURCES, FETCH_CONCURRENCY,
)
from binance_ingestor.component.candle_fetcher import BinanceFetcher, OptimizedKlineFetcher
from binance_ingestor.component.candle_listener import MarketListener
from binance_ingestor.bus import TRADE_TYPE_MAP
from binance_ingestor.utils.timer import timer, func_timer

KLINE_MARKETS = ('usdt_perp', 'usdt_spot')


def get_enabled_kline_markets():
    return [market for market in KLINE_MARKETS if market in DATA_SOURCES]


class DataJobs:
    """
    Base class for Binance data ingestion.

    All writes go through ``self._client`` (DuckportClient).
    ``duck_time`` is an in-memory dict initialised from the remote
    ``data.config_dict`` table and updated after each successful write.
    """

    def __init__(self, client: DuckportClient):
        self._client = client
        self._enabled_kline_markets = get_enabled_kline_markets()

        self.duck_time: Dict[str, pd.Timestamp] = self._init_duck_time()

        # In-memory exginfo cache (market -> DataFrame)
        self.exginfo: Dict[str, pd.DataFrame] = {}

        logger.info(f'binance ingestor 启用市场: {self._enabled_kline_markets}')

        self.init_history_data()
        self.update_recent_data()

    # ── duck_time initialisation ────────────────────────────────────

    def _init_duck_time(self) -> Dict[str, pd.Timestamp]:
        remote_times = self._client.read_duck_times(self._enabled_kline_markets)
        result = {}
        for market in self._enabled_kline_markets:
            dt = remote_times.get(market)
            if dt and dt != GENESIS_TIME:
                result[market] = dt
                logger.info(f"{market} duck_time loaded: {dt}")
            else:
                result[market] = GENESIS_TIME
                logger.warning(f"{market} duck_time not found, using GENESIS_TIME")
        return result

    # ── History data bootstrap ──────────────────────────────────────

    def init_history_data(self):
        if not self._enabled_kline_markets:
            logger.info("未启用 usdt_perp/usdt_spot，跳过历史数据初始化")
            return

        logger.info(f"开始初始化历史数据, 数据起始时间为: {START_DATE}")

        async def history_data():
            latest_times = self._get_latest_data_time()
            current_time = next_run_time(KLINE_INTERVAL) - timedelta(minutes=KLINE_INTERVAL_MINUTES)
            async with create_aiohttp_session(10) as session:
                fetchers = {}
                for market in self._enabled_kline_markets:
                    fetchers[market] = BinanceFetcher(market, session)
                    await fetchers[market].get_exchange_info()

                for market in self._enabled_kline_markets:
                    if latest_times[market] and latest_times[market] != GENESIS_TIME:
                        await self._update_historical_klines(
                            fetchers[market], None,
                            latest_times[market], market, current_time,
                        )

        asyncio.run(history_data())
        logger.info("历史数据初始化完成")

    def _get_latest_data_time(self) -> Dict[str, Optional[pd.Timestamp]]:
        latest_times = {}
        for market in self._enabled_kline_markets:
            try:
                duck_time = self.duck_time[market]
                if duck_time and duck_time != GENESIS_TIME:
                    latest_times[market] = duck_time
                else:
                    logger.warning(f"{market} 使用 START_DATE: {START_DATE}")
                    latest_times[market] = pd.to_datetime(START_DATE).tz_localize(tz=timezone.utc)
            except Exception as e:
                logger.error(f"Failed to _get_latest_data_time: {e}")
                latest_times[market] = pd.to_datetime(START_DATE).tz_localize(tz=timezone.utc)
        return latest_times

    async def _update_historical_klines(self, fetcher, symbols, start_time, market, current_time):
        if symbols is None:
            exginfo = await fetcher.get_exchange_info()
            symbols = TRADE_TYPE_MAP[market][0](exginfo)

        optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=FETCH_CONCURRENCY)
        res = await optimized_fetcher.get_all_klines(symbols, start_time=start_time, interval=KLINE_INTERVAL,
                                                     limit=499)
        successful = [i['data'] for i in res if i.get('success', False)]
        if not successful:
            logger.warning(f"{market} 无成功结果，跳过历史数据写入")
            return
        df = pd.concat(successful)
        df.sort_values(by=['open_time'], inplace=True)
        df = df[df['open_time'] < current_time]
        self.write_kline(df, market, current_time)

    # ── Write operations ────────────────────────────────────────────

    def write_kline(self, df, market, current_time):
        with timer("write to duckport-rs"):
            self._client.write_kline(df, market, KLINE_INTERVAL, current_time)
            self.duck_time[market] = pd.to_datetime(current_time)
            logger.info(f"已保存 {market} 的K线数据和当前时间: {current_time}")

    @func_timer
    async def save_exginfo(self, fetcher, market):
        exginfo = await fetcher.get_exchange_info()
        symbols_trading = TRADE_TYPE_MAP[market][0](exginfo)
        infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
        symbols_trading_df = pd.DataFrame.from_records(infos_trading)

        self.exginfo[market] = symbols_trading_df

        try:
            self._client.save_exginfo(symbols_trading_df, market)
            logger.info(f"symbols_trading数据已保存: {market}, {len(symbols_trading_df)} 条记录")
        except Exception as e:
            logger.error(f"保存symbols_trading数据失败: {e}")
        return symbols_trading

    # ── Periodic fetch ──────────────────────────────────────────────

    async def _duckdb_periodic_fetch_async(self):
        next_time = next_run_time(KLINE_INTERVAL)
        divider(f"[Scheduler] next fetch runtime: {next_time:%Y-%m-%d %H:%M:%S}", display_time=False)
        await async_sleep_until_run_time(next_time)
        if not self._enabled_kline_markets:
            logger.info("[Scheduler] 未启用 usdt_perp/usdt_spot，跳过 periodic fetch")
            return
        try:
            for market in self._enabled_kline_markets:
                await self._fetch_and_insert_binance_data_async(
                    market=market,
                    current_time=next_time,
                    interval=KLINE_INTERVAL
                )
        except Exception as e:
            logger.error(f"Scheduler Error: {e}")

    async def _fetch_and_insert_binance_data_async(self, market, current_time, interval='5m'):
        async with create_aiohttp_session(10) as session:
            fetcher = BinanceFetcher(market, session)
            symbols_trading = await self.save_exginfo(fetcher, market)
            optimized_fetcher = OptimizedKlineFetcher(fetcher, max_concurrent=FETCH_CONCURRENCY)
            gap_time = current_time - self.duck_time[market]
            if gap_time <= timedelta(minutes=KLINE_INTERVAL_MINUTES*99):
                results = await optimized_fetcher.get_all_klines(symbols_trading, interval=interval, limit=99)
            elif gap_time <= timedelta(minutes=KLINE_INTERVAL_MINUTES*499) or self.duck_time[market] == GENESIS_TIME:
                results = await optimized_fetcher.get_all_klines(symbols_trading, interval=interval, limit=499)
            else:
                logger.error("It has been a long time since last ducktime! please restart")
                return
            successful_results = [r['data'] for r in results if r.get('success', False)]
            if not successful_results:
                logger.info("没有成功的结果，无法保存")
                return

            df = pd.concat(successful_results)
            df = df[df['open_time'] < current_time]
            self.write_kline(df, market, current_time)

    @abc.abstractmethod
    def update_recent_data(self):
        raise NotImplementedError()


class RestfulDataJobs(DataJobs):
    def __init__(self, client: DuckportClient):
        logger.info('Using Restful API for DataJobs')
        super().__init__(client)

    def update_recent_data(self):
        threading.Thread(target=self.duckdb_periodic_fetch_policy, daemon=True).start()

    def duckdb_periodic_fetch_policy(self):
        async def periodic():
            while True:
                await self._duckdb_periodic_fetch_async()

        asyncio.run(periodic())


class WebsocketsDataJobs(DataJobs):
    def __init__(self, client: DuckportClient):
        logger.warning('Using Websockets for DataJobs [Beta]')
        super().__init__(client)
        self._perp_listener = None
        self._spot_listener = None

    def update_recent_data(self):
        threading.Thread(target=self._run_websocket_data_collection, daemon=True).start()

    async def _start_market_listener(self, market):
        while True:
            try:
                logger.info(f"启动 {market} 市场的WebSocket监听器")

                async def data_callback(run_time: datetime, batch_data: dict, _market: str):
                    if run_time - self.duck_time[_market] > timedelta(minutes=KLINE_INTERVAL_MINUTES):
                        logger.warning(f'do remedy. [{market}] runtime: {run_time}, ducktime: {self.duck_time[_market]}')
                        await self._fetch_and_insert_binance_data_async(market=_market, current_time=run_time,
                                                                        interval=KLINE_INTERVAL)
                        return
                    market_df = []
                    for symbol, kline_data in batch_data.items():
                        converted_df = self._convert_market_data(kline_data, _market)
                        market_df.append(converted_df)
                    market_df = pd.concat(market_df)
                    if market_df is not None:
                        self.write_kline(market_df, market, run_time)
                        logger.debug(f"已写入 {_market} 市场数据{len(market_df)}条: at {now_time()}")

                listener = MarketListener(
                    market=market,
                    data_callback=data_callback,
                    exginfo_callback=self.save_exginfo,
                )

                await listener.build_and_run()

            except Exception as e:
                logger.error(f"{market} 市场监听器异常: {e}, 20秒后重试")
                gc.collect()
                await asyncio.sleep(20)

    def _run_websocket_data_collection(self):
        async def run_async():
            try:
                if not self._enabled_kline_markets:
                    logger.info("未启用 usdt_perp/usdt_spot，跳过 WebSocket 数据收集")
                    return

                market_tasks = [
                    asyncio.create_task(self._start_market_listener(market))
                    for market in self._enabled_kline_markets
                ]
                await asyncio.gather(*market_tasks)
            except Exception as e:
                logger.error(f"WebSocket数据收集失败: {e}")

        asyncio.run(run_async())

    @staticmethod
    def _convert_market_data(kline_data, market):
        try:
            df_candle = kline_data['data']
            symbol = kline_data['symbol']

            converted_data = {
                'open_time': [df_candle['open_time'].iloc[0]],
                'symbol': [symbol],
                'open': [float(df_candle['open'].iloc[0])],
                'high': [float(df_candle['high'].iloc[0])],
                'low': [float(df_candle['low'].iloc[0])],
                'close': [float(df_candle['close'].iloc[0])],
                'volume': [float(df_candle['volume'].iloc[0])],
                'quote_volume': [float(df_candle['quote_volume'].iloc[0])],
                'trade_num': [int(df_candle['trade_num'].iloc[0])],
                'taker_buy_base_asset_volume': [float(df_candle['taker_buy_base_asset_volume'].iloc[0])],
                'taker_buy_quote_asset_volume': [float(df_candle['taker_buy_quote_asset_volume'].iloc[0])],
                'avg_price': [float(df_candle['quote_volume'].iloc[0]) / float(df_candle['volume'].iloc[0]) if float(
                    df_candle['volume'].iloc[0]) > 0 else 0.0]
            }

            return pd.DataFrame(converted_data)
        except Exception as e:
            logger.error(f"转换市场数据失败: {e}, market: {market}, symbol: {kline_data.get('symbol', 'unknown')}")
            return None

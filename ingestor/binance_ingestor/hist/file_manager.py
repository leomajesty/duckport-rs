"""
File management — paths, Parquet conversion, and cleanup.
"""

import os
from datetime import date
from glob import glob

import pandas as pd
from dateutil.relativedelta import relativedelta
from joblib import Parallel, delayed
from numpy import int64
from tqdm import tqdm

from binance_ingestor.config import RESOURCE_PATH, PARQUET_DIR, PARQUET_FILE_PERIOD, CONCURRENCY
from binance_ingestor.utils.date_partition import get_available_years_months
from binance_ingestor.utils.log_kit import logger


def get_local_path(root_path, trading_type, market_data_type, time_period, symbol, interval='5m'):
    trade_type_folder = trading_type + '_' + interval
    path = os.path.join(root_path, trade_type_folder, f'{time_period}_{market_data_type}')
    if symbol:
        path = os.path.join(path, symbol.upper())
    return path


def clean_old_daily_zip(local_daily_path, symbols, interval):
    today = date.today()
    this_month_first_day = date(today.year, today.month, 1)
    daily_end = this_month_first_day - relativedelta(months=1)

    for symbol in symbols:
        local_daily_symbol_path = os.path.join(local_daily_path, symbol)
        if os.path.exists(local_daily_symbol_path):
            zip_file_path = os.path.join(
                local_daily_symbol_path,
                "{}-{}-{}.zip".format(symbol.upper(), interval, daily_end),
            )
            for item in os.listdir(local_daily_symbol_path):
                item_path = os.path.join(local_daily_symbol_path, item)
                if item_path < zip_file_path:
                    os.remove(item_path)
            if not os.listdir(local_daily_symbol_path):
                os.rmdir(local_daily_symbol_path)


def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def read_symbol_csv(symbol, zip_path, interval='5m', ydashm=None):
    reg = '-'.join([part for part in [symbol, interval, ydashm] if isinstance(part, str) and part])
    zip_list = glob(os.path.join(zip_path, 'monthly_klines', f'{symbol}/{reg}*.zip'))
    daily_files = glob(os.path.join(zip_path, 'daily_klines', f'{symbol}/{reg}*.zip'))
    if daily_files:
        zip_list.extend(daily_files)

    if not zip_list:
        return pd.DataFrame()

    df = pd.concat([pd.read_csv(
        path_, header=None, encoding="utf-8", compression='zip',
        names=['open_time', 'open', 'high', 'low', 'close', 'volume',
               'close_time', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
               'ignore']
    ) for path_ in zip_list], ignore_index=True)

    df = df[df['open_time'] != 'open_time']
    df = df.astype(dtype={
        'open_time': int64, 'open': float, 'high': float, 'low': float,
        'close': float, 'volume': float, 'quote_volume': float,
        'trade_num': int, 'taker_buy_base_asset_volume': float,
        'taker_buy_quote_asset_volume': float,
    })
    df['avg_price'] = df['quote_volume'] / df['volume']
    df['open_time'] = df['open_time'].apply(lambda x: int(str(x)[0:13]))
    df.drop(columns=['close_time', 'ignore'], inplace=True)
    df.sort_values(by='open_time', inplace=True)
    df.drop_duplicates(subset=['open_time'], inplace=True, keep='last')
    df.reset_index(drop=True, inplace=True)
    return df


def to_pqt(yms: list[str], interval: str = '5m', market: str = 'usdt_perp'):
    if not yms:
        return
    filename = f'{market}_{yms[0]}_{len(yms)}M.parquet'
    latest_pqt = get_latest_parquet(market, interval)
    if os.path.exists(os.path.join(PARQUET_DIR, f'{market}_{interval}', filename)) and filename != latest_pqt:
        logger.info(f'跳过 {filename}')
        return
    logger.info(f'开始转换 {market}_{interval} {yms[0]}数据...')
    data_path = os.path.join(RESOURCE_PATH, f'{market}_{interval}')
    monthly_data_path = os.path.join(data_path, 'monthly_klines')
    os.makedirs(monthly_data_path, exist_ok=True)
    daily_data_path = os.path.join(data_path, 'daily_klines')
    os.makedirs(daily_data_path, exist_ok=True)
    symbols = set(os.listdir(monthly_data_path)).union(set(os.listdir(daily_data_path)))
    dfs = []

    def process_symbol_month(syb, ydashm):
        df = read_symbol_csv(syb, data_path, interval, ydashm)
        if df.empty:
            return None
        df['symbol'] = syb
        df['candle_begin_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)
        return df

    results = Parallel(n_jobs=CONCURRENCY)(
        delayed(process_symbol_month)(syb, ydashm)
        for syb in tqdm(symbols)
        for ydashm in yms
    )
    dfs.extend([df for df in results if df is not None])

    dfs = pd.concat(dfs, ignore_index=True)
    dfs.rename(columns={"candle_begin_time": "open_time"}, inplace=True)
    dfs = dfs[['open_time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
               'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'avg_price']]

    ensure_parquet_directories(market, interval)
    dfs.to_parquet(f'{PARQUET_DIR}/{market}_{interval}/{filename}', index=False)
    logger.info(f'{market}_{interval} {yms[0]}数据转换完成')


def get_latest_parquet(market: str, interval: str = '5m'):
    parquet_dir = os.path.join(PARQUET_DIR, f'{market}_{interval}')
    if not os.path.exists(parquet_dir):
        return None
    parquet_files = glob(os.path.join(parquet_dir, '*.parquet'))
    if not parquet_files:
        return None
    files = [os.path.basename(pf) for pf in parquet_files]
    return max(files)


def ensure_parquet_directories(market: str, interval: str = '5m'):
    parquet_dir = os.path.join(PARQUET_DIR, f'{market}_{interval}')
    if not os.path.exists(parquet_dir):
        os.makedirs(parquet_dir, exist_ok=True)
        logger.info(f'创建parquet目录: {parquet_dir}')
    return parquet_dir


def batch_convert_to_parquet(market: str, interval: str = '5m'):
    logger.info(f'开始批量转换 {market}_{interval} 数据为parquet格式...')
    ensure_parquet_directories(market, interval)

    available_yms = get_available_years_months(PARQUET_FILE_PERIOD)
    if not available_yms:
        logger.warning(f'{market}_{interval} 没有检测到可用数据')
        return 0, 0

    success_count = 0
    error_count = 0

    for k, v in available_yms.items():
        try:
            to_pqt(v, interval=interval, market=market)
            success_count += 1
        except Exception as e:
            error_count += 1
            logger.error(f'{market}_{interval} {k}数据转换失败: {e}')

    logger.info(f'{market}_{interval} 批量转换完成: 成功 {success_count} 个, 失败 {error_count} 个')
    return success_count, error_count


def batch_process_data(market: str, interval: str = '5m'):
    logger.info(f'开始初始化 {market}_{interval} 数据...')
    return batch_convert_to_parquet(market, interval)

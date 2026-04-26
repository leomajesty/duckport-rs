from .ws_basics import ReconnectingWebsocket
from binance_ingestor.config import proxy

SPOT_STREAM_URL = 'wss://stream.binance.com:9443/'
USDT_FUTURES_FSTREAM_URL = 'wss://fstream.binance.com/'
COIN_FUTURES_DSTREAM_URL = 'wss://dstream.binance.com/'


def get_coin_futures_multi_candlesticks_socket(symbols, time_inteval):
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=COIN_FUTURES_DSTREAM_URL,
        prefix='stream?streams=',
        proxy=proxy
    )


def get_usdt_futures_multi_candlesticks_socket(symbols, time_inteval):
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=USDT_FUTURES_FSTREAM_URL,
        prefix='market/stream?streams=',
        proxy=proxy
    )

def get_spot_multi_candlesticks_socket(symbols, time_inteval):
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=SPOT_STREAM_URL,
        prefix='stream?streams=',
        proxy=proxy
    )

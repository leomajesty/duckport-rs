"""
日志工具 — 直接从 duckport 迁移，不做改动。

使用方式:
    from binance_ingestor.utils.log_kit import logger, divider
"""

import logging
import sys
import time
import unicodedata
from datetime import datetime

from colorama import Fore, Style, init

init(autoreset=True)

OK_LEVEL = 25
logging.addLevelName(OK_LEVEL, "OK")
def ok(self, message, *args, **kwargs):
    if self.isEnabledFor(OK_LEVEL):
        self._log(OK_LEVEL, message, args, **kwargs)

logging.Logger.ok = ok

QUERY_LEVEL = 21
logging.addLevelName(QUERY_LEVEL, "QUERY")
def query(self, message, *args, **kwargs):
    if self.isEnabledFor(QUERY_LEVEL):
        self._log(QUERY_LEVEL, message, args, **kwargs)

logging.Logger.query = query


def get_display_width(text: str) -> int:
    width = 0
    for char in text:
        if unicodedata.east_asian_width(char) in ('F', 'W', 'A'):
            width += 1.685
        else:
            width += 1
    return int(width)


class SimonsFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG: ('⚙️', ''),
        logging.INFO: (Fore.BLUE, "🔵 "),
        logging.WARNING: (Fore.YELLOW, "🔔 "),
        logging.ERROR: (Fore.RED, "❌ "),
        logging.CRITICAL: (Fore.RED + Style.BRIGHT, "⭕ "),
        OK_LEVEL: (Fore.GREEN, "✅ "),
        QUERY_LEVEL: (Fore.LIGHTBLACK_EX, "🔍 "),
    }

    def format(self, record):
        color, prefix = self.FORMATS.get(record.levelno, (Fore.WHITE, ""))
        record.msg = f"{color}{prefix}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


class SimonsConsoleHandler(logging.StreamHandler):

    def emit(self, record):
        if record.levelno == logging.DEBUG:
            print(record.msg, flush=True)
        elif record.levelno == OK_LEVEL:
            super().emit(record)
            print()
        else:
            super().emit(record)


class SimonsLogger:
    _instance = dict()

    def __new__(cls, name='Log'):
        if cls._instance.get(name) is None:
            cls._instance[name] = super(SimonsLogger, cls).__new__(cls)
            cls._instance[name]._initialize_logger(name)
        return cls._instance[name]

    def _initialize_logger(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        console_handler = SimonsConsoleHandler(sys.stdout)
        console_handler.setFormatter(SimonsFormatter("%(message)s"))
        self.logger.addHandler(console_handler)


def get_logger(name=None) -> logging.Logger:
    if name is None:
        name = 'BinanceDataTool'
    return SimonsLogger(name).logger


def divider(name='', sep='=', logger_=None, display_time=True) -> None:
    seperator_len = 72
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if display_time:
        middle = f' {name} {now} '
    else:
        middle = f' {name} '
    middle_width = get_display_width(middle)
    decoration_count = max(4, (seperator_len - middle_width) // 2)
    line = sep * decoration_count + middle + sep * decoration_count

    if get_display_width(line) < seperator_len:
        line += sep

    if logger_:
        logger_.debug(line)
    else:
        logger.debug(line)
    time.sleep(0.05)


logger = get_logger('binance_ingestor')

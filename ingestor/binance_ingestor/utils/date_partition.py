from datetime import datetime, timedelta
import os
from typing import Optional

import pandas as pd

from binance_ingestor.config import START_DATE, PARQUET_FILE_PERIOD, PARQUET_DIR, KLINE_INTERVAL


def get_available_years_months(period_month: int = 6):
    assert 12 % period_month == 0, "period_month error"
    yms = []
    current_date = datetime.now()
    init_date = pd.to_datetime('2019-01-01')

    current = init_date.replace(day=1)

    while current <= current_date:
        yms.append(f'{current.year}-{current.month:02d}')
        if current.month + period_month > 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + period_month)

    first = f'{START_DATE.year}-{START_DATE.month:02d}'
    earlier = [ym for ym in yms if ym <= first]
    # 若 START_DATE 早于 init_date（2019-01），earlier 为空，直接从最早分区开始
    anchor = [earlier[-1]] if earlier else []
    yms = anchor + [ym for ym in yms if ym > first]

    yms_dict = {}
    for ym in yms:
        month = int(ym.split('-')[1])
        yms_values = []
        for range_ym in range(month, month + period_month):
            yms_value = f'{ym.split("-")[0]}-{range_ym:02d}'
            if yms_value >= first:
                yms_values.append(yms_value)
        if yms_values:  # 跳过全部早于 START_DATE 的分区
            yms_dict[ym] = yms_values

    return yms_dict


def get_parquet_cutoff_date(
    current_date: Optional[datetime] = None,
    days_before: int = 30,
) -> Optional[datetime]:
    """Return the latest Parquet-boundary date that is at least *days_before* in the past."""
    if current_date is None:
        current_date = datetime.now()
    theoretical_cutoff = current_date - pd.Timedelta(days=days_before)
    return _adjust_to_parquet_boundary(theoretical_cutoff)


def _adjust_to_parquet_boundary(date: datetime) -> datetime:
    period_months = PARQUET_FILE_PERIOD
    months_since_start = (date.year - START_DATE.year) * 12 + (date.month - START_DATE.month)
    complete_periods = months_since_start // period_months

    cutoff_year = START_DATE.year + (complete_periods * period_months) // 12
    cutoff_month = START_DATE.month + (complete_periods * period_months) % 12

    if cutoff_month > 12:
        cutoff_year += 1
        cutoff_month -= 12

    return datetime(cutoff_year, cutoff_month, 1)


def get_latest_complete_parquet_file(
    given_date: datetime,
    market: str = 'usdt_perp',
    interval: str = '5m',
) -> Optional[dict]:
    if given_date.date() < START_DATE:
        return None

    theoretical_file = _calculate_theoretical_latest_file(given_date, market, interval)

    if theoretical_file is None:
        return None

    file_path = os.path.join(PARQUET_DIR, f"{market}_{interval}", theoretical_file['filename'])
    theoretical_file['file_path'] = file_path
    theoretical_file['exists'] = os.path.exists(file_path)

    return theoretical_file


def _calculate_theoretical_latest_file(given_date: datetime, market: str, interval: str) -> Optional[dict]:
    try:
        period_months = PARQUET_FILE_PERIOD
        months_since_start = (given_date.year - START_DATE.year) * 12 + (given_date.month - START_DATE.month)
        complete_periods = months_since_start // period_months

        complete_periods -= 1
        if months_since_start % period_months == 0 and given_date.day == 1:
            complete_periods -= 1

        if complete_periods < 0:
            return None

        period_start_year = START_DATE.year + (complete_periods * period_months) // 12
        period_start_month = START_DATE.month + (complete_periods * period_months) % 12

        if period_start_month > 12:
            period_start_year += 1
            period_start_month -= 12

        period_end_year = period_start_year + (period_months - 1) // 12
        period_end_month = period_start_month + (period_months - 1) % 12

        if period_end_month > 12:
            period_end_year += 1
            period_end_month -= 12

        period_start_str = f"{period_start_year}-{period_start_month:02d}"
        filename = f"{market}_{period_start_str}_{period_months}M.parquet"

        start_date_str = f"{period_start_year}-{period_start_month:02d}-01"
        if period_end_month == 12:
            next_month_year = period_end_year + 1
            next_month_month = 1
        else:
            next_month_year = period_end_year
            next_month_month = period_end_month + 1
        last_day_dt = datetime(next_month_year, next_month_month, 1)
        end_date_str = last_day_dt.strftime('%Y-%m-%d')

        return {
            'filename': filename,
            'period_start': start_date_str,
            'period_end': end_date_str,
            'period_months': period_months,
            'file_path': '',
            'exists': None
        }

    except Exception:
        return None

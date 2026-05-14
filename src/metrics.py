"""
指标计算模块
基于净值序列计算: 最大回撤、年化收益、波动率、夏普比率、熊市表现等
"""
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# 熊市定义: (起始日, 结束日)
BEAR_MARKETS = [
    ('2018-01-29', '2019-01-04'),   # 2018 贸易战熊市
    ('2021-12-13', '2022-10-31'),   # 2022 流动性危机
    ('2023-08-01', '2024-02-05'),   # 2024 政策预期低迷
]


def calc_max_drawdown(nav_series):
    """计算最大回撤(绝对值,百分比)"""
    if len(nav_series) < 2:
        return 0
    nav = pd.Series(nav_series).dropna()
    if len(nav) < 2:
        return 0
    cummax = nav.cummax()
    drawdown = (nav - cummax) / cummax
    return abs(drawdown.min()) * 100


def calc_annual_return(nav_series, dates):
    """年化收益率"""
    if len(nav_series) < 2 or len(dates) < 2:
        return 0
    nav = pd.Series(nav_series).dropna()
    if len(nav) < 2:
        return 0
    days = (pd.to_datetime(dates[-1]) - pd.to_datetime(dates[0])).days
    if days <= 0:
        return 0
    total_return = nav.iloc[-1] / nav.iloc[0]
    return (total_return ** (365 / days) - 1) * 100


def calc_volatility(nav_series):
    """年化波动率"""
    if len(nav_series) < 2:
        return 0
    nav = pd.Series(nav_series).dropna()
    if len(nav) < 2:
        return 0
    daily_returns = nav.pct_change().dropna()
    return daily_returns.std() * np.sqrt(252) * 100


def calc_sharpe(nav_series, dates, risk_free=0.02):
    """夏普比率"""
    annual_ret = calc_annual_return(nav_series, dates) / 100
    vol = calc_volatility(nav_series) / 100
    if vol == 0:
        return 0
    return (annual_ret - risk_free) / vol


def calc_bear_market_count(nav_df, manager_start_date=None):
    """
    计算该基金/经理经历过的熊市数(0-3)
    nav_df: 包含 '净值日期' 和 '单位净值' 两列
    manager_start_date: 经理任职起始日,如果指定则只算之后的熊市
    """
    if nav_df is None or len(nav_df) == 0:
        return 0

    nav_df = nav_df.copy()
    date_col = '净值日期' if '净值日期' in nav_df.columns else nav_df.columns[0]
    nav_df[date_col] = pd.to_datetime(nav_df[date_col], errors='coerce')
    nav_df = nav_df.dropna(subset=[date_col])

    if manager_start_date is not None:
        manager_start = pd.to_datetime(manager_start_date)
    else:
        manager_start = pd.Timestamp.min

    count = 0
    for bear_start, bear_end in BEAR_MARKETS:
        bs, be = pd.to_datetime(bear_start), pd.to_datetime(bear_end)
        # 经理任职覆盖该熊市
        if manager_start <= bs and nav_df[date_col].max() >= be:
            count += 1
    return count


def calc_recent_drawdown(nav_df, years=3):
    """计算近 N 年最大回撤(百分比绝对值)"""
    if nav_df is None or len(nav_df) == 0:
        return 0
    nav_df = nav_df.copy()
    date_col = '净值日期' if '净值日期' in nav_df.columns else nav_df.columns[0]
    nav_col = '单位净值' if '单位净值' in nav_df.columns else nav_df.columns[1]

    nav_df[date_col] = pd.to_datetime(nav_df[date_col], errors='coerce')
    nav_df = nav_df.dropna(subset=[date_col]).sort_values(date_col)

    cutoff = pd.Timestamp.now() - pd.Timedelta(days=365 * years)
    recent = nav_df[nav_df[date_col] >= cutoff]

    if len(recent) < 2:
        return 0

    return calc_max_drawdown(recent[nav_col].values)


def calc_annual_returns_by_year(nav_df, years=5):
    """计算最近 N 年每年的收益率(用于业绩稳定性评估)"""
    if nav_df is None or len(nav_df) == 0:
        return []

    nav_df = nav_df.copy()
    date_col = '净值日期' if '净值日期' in nav_df.columns else nav_df.columns[0]
    nav_col = '单位净值' if '单位净值' in nav_df.columns else nav_df.columns[1]

    nav_df[date_col] = pd.to_datetime(nav_df[date_col], errors='coerce')
    nav_df = nav_df.dropna(subset=[date_col]).sort_values(date_col)
    nav_df['year'] = nav_df[date_col].dt.year

    current_year = datetime.now().year
    target_years = list(range(current_year - years, current_year))

    annual_returns = []
    for y in target_years:
        year_data = nav_df[nav_df['year'] == y]
        if len(year_data) >= 2:
            ret = (year_data[nav_col].iloc[-1] / year_data[nav_col].iloc[0] - 1) * 100
            annual_returns.append((y, ret))
    return annual_returns


def calc_performance_rank_percentile(fund_returns_list, all_funds_returns_list):
    """
    计算业绩排名分位
    fund_returns_list: 该基金近 N 年每年收益 [(year, return), ...]
    all_funds_returns_list: 所有同类基金近 N 年每年收益,字典 {year: [return1, return2, ...]}
    返回平均排名分位 (0=最好,100=最差)
    """
    if not fund_returns_list:
        return 100

    percentiles = []
    for year, fund_ret in fund_returns_list:
        if year in all_funds_returns_list and len(all_funds_returns_list[year]) > 1:
            all_rets = all_funds_returns_list[year]
            # 计算分位: 该基金收益超过多少比例的基金,越高越好
            better_than = sum(1 for r in all_rets if fund_ret > r)
            percentile = (1 - better_than / len(all_rets)) * 100
            percentiles.append(percentile)

    return np.mean(percentiles) if percentiles else 100

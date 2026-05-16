"""
指标计算模块
基于净值序列计算: 最大回撤、年化收益、波动率、夏普比率、熊市表现等
"""
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# 熊市定义 (起始日, 结束日) — 从 config 引入, 支持月度维护
from .config import BEAR_MARKETS as BEAR_MARKETS  # noqa: E402,F401


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


def calc_market_percentile(value, full_market_values):
    """
    计算单个指标值在全市场中的分位
    返回 0=最好/最高, 100=最差/最低; 数据不足返回 NaN
    """
    if pd.isna(value):
        return np.nan
    vals = [v for v in full_market_values if pd.notna(v)]
    if len(vals) < 2:
        return np.nan
    better_than = sum(1 for v in vals if value > v)
    return (1 - better_than / len(vals)) * 100


def calc_calmar(annual_return_pct, max_drawdown_pct):
    """
    卡玛比率 = 年化收益率 / 最大回撤
    同时考虑收益与回撤,比夏普更直观
    """
    if pd.isna(annual_return_pct) or pd.isna(max_drawdown_pct):
        return np.nan
    if max_drawdown_pct <= 0:
        return np.nan
    return annual_return_pct / max_drawdown_pct


def calc_bear_period_drawdown(nav_df, bear_periods=None):
    """
    计算每轮熊市期间该基金的最大回撤, 返回 dict {(bear_start, bear_end): drawdown_pct}
    基金净值未覆盖某轮熊市则该项缺失
    """
    if bear_periods is None:
        bear_periods = BEAR_MARKETS
    if nav_df is None or len(nav_df) == 0:
        return {}

    nav_df = nav_df.copy()
    date_col = '净值日期' if '净值日期' in nav_df.columns else nav_df.columns[0]
    nav_col = '单位净值' if '单位净值' in nav_df.columns else nav_df.columns[1]

    nav_df[date_col] = pd.to_datetime(nav_df[date_col], errors='coerce')
    nav_df[nav_col] = pd.to_numeric(nav_df[nav_col], errors='coerce')
    nav_df = nav_df.dropna(subset=[date_col, nav_col]).sort_values(date_col)

    result = {}
    for bs, be in bear_periods:
        bs_ts, be_ts = pd.to_datetime(bs), pd.to_datetime(be)
        period = nav_df[(nav_df[date_col] >= bs_ts) & (nav_df[date_col] <= be_ts)]
        if len(period) >= 2:
            result[(bs, be)] = calc_max_drawdown(period[nav_col].values)
    return result


def calc_avg_bear_drawdown(bear_dd_dict):
    """对所有经历过的熊市回撤取平均(基金未覆盖的熊市不计入)"""
    if not bear_dd_dict:
        return np.nan
    return float(np.mean(list(bear_dd_dict.values())))


def detect_bear_markets(index_series, params=None):
    """
    基于一条合成"指数序列"自动检测熊市段。
    index_series: pd.Series, index 是日期(可转 datetime), value 是合成指数值(如候选池中位 NAV)
    params: dict, 见 config.BEAR_DETECT_PARAMS
    返回 list[(start_str, end_str, drawdown_pct)], 已按时间排序。

    算法 (Peak-to-Trough drawdown clustering):
    1. 对序列遍历, 维护当前 cummax 和"自该 max 以来的最低谷"
    2. 当下跌幅度 >= min_drawdown_pct 时, 标记进入熊市
    3. 直到价格重新创新高(超过原 peak) → 熊市结束(end = 最低谷日期)
    4. 持续天数 < min_duration_days 的段过滤
    """
    if params is None:
        from .config import BEAR_DETECT_PARAMS
        params = BEAR_DETECT_PARAMS

    s = pd.Series(index_series).copy()
    s.index = pd.to_datetime(s.index, errors='coerce')
    s = s[~s.index.isna()].sort_index()
    s = pd.to_numeric(s, errors='coerce').dropna()
    if len(s) < 60:
        return []

    min_dd = float(params.get('min_drawdown_pct', 18.0))
    min_dur = int(params.get('min_duration_days', 60))

    segments = []
    peak_val = s.iloc[0]
    peak_date = s.index[0]
    trough_val = s.iloc[0]
    trough_date = s.index[0]
    in_bear = False
    bear_start = None

    for date, val in s.items():
        if val >= peak_val:
            # 新高 → 若处熊市中, 段结束
            if in_bear:
                dd = (peak_val - trough_val) / peak_val * 100
                duration = (trough_date - bear_start).days
                if duration >= min_dur and dd >= min_dd:
                    segments.append((
                        bear_start.strftime('%Y-%m-%d'),
                        trough_date.strftime('%Y-%m-%d'),
                        round(float(dd), 2),
                    ))
                in_bear = False
            peak_val = val
            peak_date = date
            trough_val = val
            trough_date = date
        else:
            if val < trough_val:
                trough_val = val
                trough_date = date
            dd_now = (peak_val - val) / peak_val * 100
            if not in_bear and dd_now >= min_dd:
                in_bear = True
                bear_start = peak_date

    # 收尾: 当前仍在熊市且未恢复
    if in_bear:
        dd = (peak_val - trough_val) / peak_val * 100
        duration = (trough_date - bear_start).days
        if duration >= min_dur and dd >= min_dd:
            segments.append((
                bear_start.strftime('%Y-%m-%d'),
                trough_date.strftime('%Y-%m-%d'),
                round(float(dd), 2),
            ))

    return segments


def diff_bear_markets(detected, configured, overlap_days=30):
    """
    返回 detected 中"未在 configured 列表里"的新段。
    overlap_days: 与已配置段允许的最大日期重叠 (避免微小窗口差异重复提示)
    """
    configured_ranges = []
    for s, e in (configured or []):
        try:
            configured_ranges.append((pd.to_datetime(s), pd.to_datetime(e)))
        except Exception:
            continue

    new_segments = []
    for s, e, dd in detected:
        ds, de = pd.to_datetime(s), pd.to_datetime(e)
        overlap = False
        for cs, ce in configured_ranges:
            # 任一端在已配置段内, 或 detected 段完全包含 configured 段
            inter_start = max(ds, cs)
            inter_end = min(de, ce)
            if (inter_end - inter_start).days > -overlap_days:
                overlap = True
                break
        if not overlap:
            new_segments.append((s, e, dd))
    return new_segments


def calc_industry_similarity(allocations):
    """
    计算多期行业配置的稳定性(余弦相似度均值)
    allocations: list of dict, 每个 dict 为 {industry_name: weight_pct}
    至少需要 2 期数据,否则返回 NaN; 返回 0-1 越大越稳定
    """
    if not allocations or len(allocations) < 2:
        return np.nan

    all_industries = set()
    for alloc in allocations:
        all_industries.update(alloc.keys())
    industries = sorted(all_industries)
    if len(industries) == 0:
        return np.nan

    vectors = []
    for alloc in allocations:
        v = np.array([alloc.get(ind, 0.0) for ind in industries], dtype=float)
        norm = np.linalg.norm(v)
        if norm > 0:
            v = v / norm
        vectors.append(v)

    sims = []
    for i in range(len(vectors)):
        for j in range(i + 1, len(vectors)):
            sims.append(float(np.dot(vectors[i], vectors[j])))
    return float(np.mean(sims)) if sims else np.nan

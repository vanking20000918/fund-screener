"""
回测验证 (Point-In-Time)

思路:
- 选历史时点 T (默认 2024-01-01)
- 对每只基金取完整 NAV, 切成 history(<T) 与 holdout(>=T)
- 仅用 history 重算所有"能从 NAV 推出"的指标(收益/回撤/Calmar/Sharpe/熊市), 这是真正的 PIT
- 经理任职年限: 用"经理任职起始"字段计算 (as_of - start), 任职起始 > T 的视为"当时不是该经理", 直接 NaN
- 基金成立日期: 同样从 as_of 倒推年龄
- 基金规模/经理在管基金数: 用当前值作代理 (已知偏差, 留作 v2 改进)
- 用 PIT 数据跑同样的硬筛 + 软评分 + Top N
- 用 holdout 计算 [T, hold_end] 持有期实际收益, 对比基准
- 基准: (a) 通过硬筛的候选池等权平均  (b) 整个候选宇宙等权平均

注: 全市场基金宇宙仍用"当前"排名作为代理, 存在存活者偏差;
此版本主要回答"评分逻辑相对池内基准是否真的挑出超额收益的赢家"
"""
import argparse
import logging
import os
import sys
from datetime import timedelta

import numpy as np
import pandas as pd

from . import data_fetcher_eastmoney as df_module
from . import metrics
from .config import (
    HARD_FILTER, SCORE_WEIGHTS, TOP_N, PERF_CONFIG, POOL_RANK_WEIGHTS, FUND_TYPES,
)

logger = logging.getLogger(__name__)


# ============================================================================
# NAV 切分与 PIT 窗口指标
# ============================================================================

def _normalize_nav(nav_df):
    if nav_df is None or len(nav_df) == 0:
        return None
    d = nav_df.copy()
    d['净值日期'] = pd.to_datetime(d['净值日期'], errors='coerce')
    d['单位净值'] = pd.to_numeric(d['单位净值'], errors='coerce')
    d = d.dropna(subset=['净值日期', '单位净值']).sort_values('净值日期').reset_index(drop=True)
    return d if len(d) else None


def _split_nav(nav_df, as_of):
    """切分 NAV: history=as_of 之前(含同日 <), holdout=as_of 及之后"""
    d = _normalize_nav(nav_df)
    if d is None:
        return None, None
    history = d[d['净值日期'] < as_of].reset_index(drop=True)
    holdout = d[d['净值日期'] >= as_of].reset_index(drop=True)
    return history, holdout


def _ret_window(history, end_date, days):
    """取 [end_date - days, end_date] 内 NAV 收益率(%), 不足返回 NaN"""
    if history is None or len(history) < 2:
        return np.nan
    start = end_date - pd.Timedelta(days=days)
    sub = history[(history['净值日期'] >= start) & (history['净值日期'] <= end_date)]
    if len(sub) < 2:
        return np.nan
    return (sub['单位净值'].iloc[-1] / sub['单位净值'].iloc[0] - 1) * 100


def _ytd_ret(history, end_date):
    if history is None or len(history) < 2:
        return np.nan
    yr_start = pd.Timestamp(year=end_date.year, month=1, day=1)
    sub = history[(history['净值日期'] >= yr_start) & (history['净值日期'] <= end_date)]
    if len(sub) < 2:
        return np.nan
    return (sub['单位净值'].iloc[-1] / sub['单位净值'].iloc[0] - 1) * 100


def _max_dd_window(history, end_date, years=3):
    if history is None or len(history) < 2:
        return np.nan
    cutoff = end_date - pd.Timedelta(days=int(365 * years))
    sub = history[(history['净值日期'] >= cutoff) & (history['净值日期'] <= end_date)]
    if len(sub) < 2:
        return np.nan
    return metrics.calc_max_drawdown(sub['单位净值'].values)


def _bear_periods_before(as_of):
    """只取在 as_of 之前已完全结束的熊市段, 避免用未来信息"""
    return [(s, e) for s, e in metrics.BEAR_MARKETS if pd.to_datetime(e) < as_of]


def _holdout_ret(holdout, hold_end):
    if holdout is None or len(holdout) < 2:
        return np.nan
    sub = holdout[holdout['净值日期'] <= hold_end]
    if len(sub) < 2:
        return np.nan
    return (sub['单位净值'].iloc[-1] / sub['单位净值'].iloc[0] - 1) * 100


# ============================================================================
# 主管线
# ============================================================================

def _fetch_universe():
    """拉取当前全市场基金清单 (作为可投宇宙代理, 存在存活者偏差)"""
    pieces = []
    if '股票型' in FUND_TYPES:
        d = df_module.fetch_fund_rank_stock()
        if d is not None and len(d):
            d = d.copy(); d['基金类型'] = '股票型'; pieces.append(d)
    if '混合型' in FUND_TYPES:
        d = df_module.fetch_fund_rank_mixed()
        if d is not None and len(d):
            d = d.copy(); d['基金类型'] = '混合型'; pieces.append(d)
    if 'QDII' in FUND_TYPES:
        d = df_module.fetch_fund_rank_qdii()
        if d is not None and len(d):
            d = d.copy(); d['基金类型'] = 'QDII'; pieces.append(d)
    if not pieces:
        return pd.DataFrame()
    df = pd.concat(pieces, ignore_index=True)

    if '基金代码' not in df.columns:
        c = next((c for c in df.columns if '代码' in c), None)
        if c:
            df = df.rename(columns={c: '基金代码'})
    if '基金简称' not in df.columns:
        for kw in ('简称', '名称'):
            c = next((c for c in df.columns if kw in c), None)
            if c:
                df = df.rename(columns={c: '基金简称'}); break
    df['基金代码'] = df['基金代码'].astype(str).str.zfill(6)

    founded_col = next((c for c in df.columns if '成立日' in c), None)
    if founded_col:
        df['成立日期'] = pd.to_datetime(df[founded_col], errors='coerce')
    return df


def _fund_age_prefilter(universe, as_of):
    """按 as_of - min_fund_age 预过滤成立日期"""
    if '成立日期' not in universe.columns:
        return universe
    cutoff = as_of - pd.Timedelta(days=int(365 * HARD_FILTER['min_fund_age']))
    mask = universe['成立日期'].isna() | (universe['成立日期'] <= cutoff)
    n0 = len(universe)
    out = universe[mask].copy()
    logger.info(f'按 (as_of - {HARD_FILTER["min_fund_age"]} 年) 预过滤成立日期: {n0} → {len(out)}')
    return out


def _composite_rank(df, as_of):
    """用 PIT 的 近3年/近1年/今年来 收益, 按 POOL_RANK_WEIGHTS 综合排名"""
    composite = pd.Series(0.0, index=df.index)
    weight_used = 0.0
    label_col = {'近3年': '近3年_PIT', '近1年': '近1年_PIT', '今年来': '今年来_PIT'}
    for label, col in label_col.items():
        w = POOL_RANK_WEIGHTS.get(label, 0)
        if w == 0 or col not in df.columns:
            continue
        rank_pct = df[col].rank(ascending=True, pct=True, na_option='keep').fillna(0)
        composite = composite + rank_pct * w
        weight_used += w
    if weight_used > 0:
        composite = composite / weight_used
    df = df.copy()
    df['_composite'] = composite
    return df.sort_values('_composite', ascending=False)


def _compute_pit_metrics(history, as_of, bear_periods_pit):
    """单只基金: 用 history 算 PIT 指标 dict"""
    if history is None or len(history) < 30:
        return None

    cutoff = as_of - pd.Timedelta(days=365 * 3)
    recent = history[history['净值日期'] >= cutoff]
    if len(recent) >= 2:
        ann_ret = metrics.calc_annual_return(recent['单位净值'].values, recent['净值日期'].values)
        sharpe = metrics.calc_sharpe(recent['单位净值'].values, recent['净值日期'].values)
        vol = metrics.calc_volatility(recent['单位净值'].values)
    else:
        ann_ret = sharpe = vol = np.nan

    max_dd = _max_dd_window(history, as_of, years=3)
    calmar = metrics.calc_calmar(ann_ret, max_dd)

    bear_dd = metrics.calc_bear_period_drawdown(history, bear_periods_pit)
    bear_avg = metrics.calc_avg_bear_drawdown(bear_dd)
    bear_count = len(bear_dd)

    return {
        '近3年最大回撤_PIT': max_dd if (max_dd is not None and max_dd > 0) else np.nan,
        '年化收益率_PIT': ann_ret,
        '年化波动率_PIT': vol,
        '夏普比率_PIT': sharpe,
        '卡玛比率_PIT': calmar,
        '熊市数_PIT': bear_count,
        '熊市平均回撤_PIT': bear_avg,
    }


def _enrich_manager_pit(df, as_of):
    """从 fetch_fund_details_batch 拿 经理任职起始/规模, 算 PIT 任职年限"""
    details = df_module.fetch_fund_details_batch(df['基金代码'].tolist())
    if details is None or len(details) == 0:
        df['基金经理'] = ''
        df['经理任职年限_PIT'] = np.nan
        df['经理在管基金数'] = np.nan
        df['基金规模'] = np.nan
        return df

    details = details.copy()
    details['基金代码'] = details['基金代码'].astype(str).str.zfill(6)

    if '经理任职起始' in details.columns:
        start = pd.to_datetime(details['经理任职起始'], errors='coerce')
        tenure = (as_of - start).dt.days / 365
        # 起始日期 > as_of 说明当时不是这个经理, 视为缺失
        tenure = tenure.where(tenure >= 0, np.nan)
        details['经理任职年限_PIT'] = tenure
    else:
        details['经理任职年限_PIT'] = np.nan

    if '基金经理' in details.columns:
        details['经理在管基金数'] = details['基金经理'].map(details['基金经理'].value_counts())
    else:
        details['经理在管基金数'] = np.nan
        details['基金经理'] = ''

    if '基金规模' not in details.columns:
        details['基金规模'] = np.nan

    merge_cols = ['基金代码', '基金经理', '经理任职年限_PIT', '经理在管基金数', '基金规模']
    merge_cols = [c for c in merge_cols if c in details.columns]
    out = df.merge(details[merge_cols].drop_duplicates('基金代码'), on='基金代码', how='left')
    return out


def _apply_hard_filter_pit(df):
    hf = HARD_FILTER
    df = df.copy()
    df['硬筛通过'] = True
    df['淘汰原因'] = ''

    def fail(mask, reason):
        df.loc[mask, '硬筛通过'] = False
        df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + reason + ';'

    fail(df['经理任职年限_PIT'].notna() & (df['经理任职年限_PIT'] < hf['min_manager_years']),
         f'经理任职<{hf["min_manager_years"]}年')
    fail(df['基金年龄_PIT'].notna() & (df['基金年龄_PIT'] < hf['min_fund_age']),
         f'基金成立<{hf["min_fund_age"]}年')
    scale_num = pd.to_numeric(df['基金规模'], errors='coerce')
    fail(scale_num.notna() & ((scale_num < hf['min_scale']) | (scale_num > hf['max_scale'])),
         f'规模不在{hf["min_scale"]}-{hf["max_scale"]}亿')
    count_num = pd.to_numeric(df['经理在管基金数'], errors='coerce')
    fail(count_num.notna() & (count_num > hf['max_funds_per_manager']),
         f'在管>{hf["max_funds_per_manager"]}只')
    fail(df['近3年最大回撤_PIT'].notna() & (df['近3年最大回撤_PIT'] > hf['max_drawdown_pct']),
         f'近3年回撤>{hf["max_drawdown_pct"]}%')
    fail(df['近1年_PIT'].notna() & (df['近1年_PIT'] < hf['min_recent_1y_return']),
         f'近1年收益<{hf["min_recent_1y_return"]}%')
    return df


def _soft_score_pit(df):
    MED = 50
    def s_stab(p):
        if pd.isna(p): return MED
        return 100 if p <= 10 else 88 if p <= 25 else 70 if p <= 50 else 50 if p <= 70 else 25
    def s_frame(c):
        if pd.isna(c): return MED
        return 100 if c >= 0.8 else 85 if c >= 0.5 else 70 if c >= 0.3 else 55 if c >= 0.1 else 40 if c >= 0 else 20
    def s_scale(s):
        """与 screener.score_scale 保持一致: 钟形分段线性"""
        if pd.isna(s): return MED
        if s < 2:   return max(20.0, 30 + s / 2 * 30)
        if s < 5:   return 60 + (s - 2) / 3 * 40       # 2→60, 5→100
        if s <= 30: return 100                          # 甜区
        if s <= 50: return 100 - (s - 30) / 20 * 15    # 30→100, 50→85
        if s <= 100: return 85 - (s - 50) / 50 * 30   # 50→85, 100→55
        if s <= 200: return 55 - (s - 100) / 100 * 25 # 100→55, 200→30
        return 30
    def s_tenure(y):
        """与 screener.score_tenure 保持一致: 分段线性, 边际递减"""
        if pd.isna(y): return MED
        if y < 3:    return max(20.0, 40 + y / 3 * 15)
        if y < 5:    return 55 + (y - 3) / 2 * 17       # 3→55, 5→72
        if y < 7:    return 72 + (y - 5) / 2 * 13       # 5→72, 7→85
        if y < 10:   return 85 + (y - 7) / 3 * 10       # 7→85, 10→95
        if y < 15:   return 95 + (y - 10) / 5 * 5       # 10→95, 15→100
        return 100
    def s_style_vol(v):
        if pd.isna(v): return MED
        return 90 if v < 18 else 75 if v < 22 else 60 if v < 26 else 40

    # 业绩排名分位: 候选池内近3年 PIT 收益的 0=最好,100=最差
    df = df.copy()
    df['业绩排名分位_PIT'] = (1 - df['近3年_PIT'].rank(ascending=True, pct=True, na_option='keep')) * 100

    # 熊市分位 (低回撤=好)
    bear_pct = df['熊市平均回撤_PIT'].rank(ascending=True, pct=True, na_option='keep') * 100
    def s_bear(p, count):
        if pd.isna(p): return MED
        base = 100 if p <= 20 else 82 if p <= 40 else 65 if p <= 60 else 45 if p <= 80 else 25
        bonus = (min(count or 0, 3) - 1) * 2.5
        return float(min(100, max(0, base + bonus)))

    df['得分_稳定性'] = df['业绩排名分位_PIT'].apply(s_stab)
    df['得分_框架'] = df['卡玛比率_PIT'].apply(s_frame)
    df['得分_风格'] = df['年化波动率_PIT'].apply(s_style_vol)
    df['得分_熊市'] = [s_bear(p, c) for p, c in zip(bear_pct, df['熊市数_PIT'])]
    df['得分_规模'] = df['基金规模'].apply(s_scale)
    df['得分_任职'] = df['经理任职年限_PIT'].apply(s_tenure)

    w = SCORE_WEIGHTS
    df['综合得分'] = (
        df['得分_稳定性'] * w['stability']
        + df['得分_风格'] * w['style']
        + df['得分_框架'] * w['framework']
        + df['得分_熊市'] * w['bear_perf']
        + df['得分_规模'] * w['scale']
        + df['得分_任职'] * w['tenure']
    )
    return df


def _prerank_universe(universe, cap):
    """用宇宙表自带的 近1年/近3年/今年来 列做综合排名, 取前 cap 只
    这是为了避免给全市场 3000+ 只基金都拉 NAV (开销巨大)。
    注: 使用当前(非 PIT)排名,等价于原 screener 的预筛思路,会引入 look-ahead bias。
    """
    if cap is None or len(universe) <= cap:
        return universe
    def _find(*kw):
        for c in universe.columns:
            if all(k in c for k in kw): return c
        return None
    col_3y, col_1y, col_ytd = _find('近3年'), _find('近1年'), _find('今年来') or _find('今年')
    composite = pd.Series(0.0, index=universe.index)
    used = 0.0
    for label, col in [('近3年',col_3y),('近1年',col_1y),('今年来',col_ytd)]:
        w = POOL_RANK_WEIGHTS.get(label, 0)
        if col is None or w == 0: continue
        v = pd.to_numeric(universe[col], errors='coerce')
        composite += v.rank(ascending=True, pct=True, na_option='keep').fillna(0) * w
        used += w
    if used > 0:
        composite = composite / used
    out = universe.assign(_pre=composite).sort_values('_pre', ascending=False).head(cap).drop(columns='_pre')
    logger.info(f'预筛宇宙(按当前综合排名 cap={cap}): {len(universe)} → {len(out)}')
    return out


def run_backtest(as_of_date, hold_end_date=None, candidate_pool_size=None, top_n=None, max_universe=None):
    """主入口: 返回 dict(summary, top_n, passed, all)"""
    as_of = pd.to_datetime(as_of_date)
    hold_end = pd.to_datetime(hold_end_date) if hold_end_date else pd.Timestamp.now().normalize()
    pool_size = candidate_pool_size or PERF_CONFIG['candidate_pool_size']
    top_n = top_n or TOP_N

    logger.info('=' * 70)
    logger.info(f'回测: T={as_of.date()}, 持有至={hold_end.date()} ({(hold_end-as_of).days} 天)')
    logger.info(f'候选池规模={pool_size}, Top N={top_n}, 宇宙上限={max_universe}')
    logger.info('=' * 70)

    # 1. 拉宇宙
    logger.info('[1/6] 拉取基金宇宙...')
    universe = _fetch_universe()
    if len(universe) == 0:
        logger.error('未拿到基金宇宙数据')
        return None
    universe = _fund_age_prefilter(universe, as_of)
    universe = _prerank_universe(universe, max_universe)
    logger.info(f'宇宙: {len(universe)} 只')

    # 2. 抓 NAV, 切分 PIT/holdout, 算窗口收益(用于综合排名)
    bear_periods_pit = _bear_periods_before(as_of)
    logger.info(f'[2/6] 抓 NAV + 切分 PIT (适用熊市段 {len(bear_periods_pit)} 段)...')
    records = []
    total = len(universe)
    for i, (_, row) in enumerate(universe.iterrows()):
        code = row['基金代码']
        try:
            nav = df_module.fetch_fund_nav(code)
            history, holdout = _split_nav(nav, as_of)
            if history is None or len(history) < 30:
                continue
            r = {
                '基金代码': code,
                '基金简称': row.get('基金简称', ''),
                '基金类型': row.get('基金类型', ''),
                '成立日期': row.get('成立日期', pd.NaT),
                '_nav_history': history,
                '_nav_holdout': holdout,
                '近3年_PIT': _ret_window(history, as_of, 365 * 3),
                '近1年_PIT': _ret_window(history, as_of, 365),
                '今年来_PIT': _ytd_ret(history, as_of),
            }
            records.append(r)
            if (i + 1) % 100 == 0:
                logger.info(f'  进度: {i+1}/{total}, 已纳入: {len(records)}')
        except Exception as e:
            logger.debug(f'{code}: {e}')
    df = pd.DataFrame(records)
    logger.info(f'有效 NAV 基金: {len(df)} / {total}')
    if len(df) == 0:
        return None

    # 3. 综合排名取候选池
    logger.info('[3/6] 综合排名取候选池...')
    df = _composite_rank(df, as_of).head(pool_size).reset_index(drop=True)
    logger.info(f'候选池: {len(df)} 只')

    # 4. 算 PIT 指标 + 经理 + 规模
    logger.info('[4/6] 算 PIT 指标 + 经理信息...')
    pit_metrics = []
    for _, r in df.iterrows():
        m = _compute_pit_metrics(r['_nav_history'], as_of, bear_periods_pit)
        if m is None:
            m = {k: np.nan for k in ['近3年最大回撤_PIT','年化收益率_PIT','年化波动率_PIT',
                                      '夏普比率_PIT','卡玛比率_PIT','熊市平均回撤_PIT']}
            m['熊市数_PIT'] = 0
        m['基金代码'] = r['基金代码']
        pit_metrics.append(m)
    df = df.merge(pd.DataFrame(pit_metrics), on='基金代码', how='left')
    df = _enrich_manager_pit(df, as_of)
    df['基金年龄_PIT'] = (as_of - pd.to_datetime(df['成立日期'], errors='coerce')).dt.days / 365

    # 5. 硬筛 + 软评分
    logger.info('[5/6] 硬筛 + 软评分...')
    df = _apply_hard_filter_pit(df)
    passed_n = int(df['硬筛通过'].sum())
    logger.info(f'硬筛: {len(df)} → {passed_n}')
    df = _soft_score_pit(df)

    # 6. 持有期收益 + 基准对比
    logger.info('[6/6] 算持有期收益 + 基准对比...')
    df['持有期收益'] = df['_nav_holdout'].apply(lambda h: _holdout_ret(h, hold_end))

    passed = df[df['硬筛通过']].sort_values('综合得分', ascending=False).copy()
    top_df = passed.head(top_n).copy()

    universe_avg = df['持有期收益'].mean()
    universe_median = df['持有期收益'].median()
    pool_avg = passed['持有期收益'].mean()
    pool_median = passed['持有期收益'].median()
    top_avg = top_df['持有期收益'].mean()
    top_median = top_df['持有期收益'].median()

    win_vs_pool_median = float((top_df['持有期收益'] > pool_median).mean() * 100) if pd.notna(pool_median) else np.nan
    win_vs_universe_median = float((top_df['持有期收益'] > universe_median).mean() * 100) if pd.notna(universe_median) else np.nan

    summary = {
        'as_of': str(as_of.date()),
        'hold_end': str(hold_end.date()),
        'hold_days': int((hold_end - as_of).days),
        'pool_universe_size': int(len(df)),
        'pool_passed_hard_filter': passed_n,
        'top_n_size': int(len(top_df)),
        'top_n_avg_return_pct': round(float(top_avg), 2) if pd.notna(top_avg) else None,
        'top_n_median_return_pct': round(float(top_median), 2) if pd.notna(top_median) else None,
        'pool_passed_avg_return_pct': round(float(pool_avg), 2) if pd.notna(pool_avg) else None,
        'pool_passed_median_return_pct': round(float(pool_median), 2) if pd.notna(pool_median) else None,
        'universe_avg_return_pct': round(float(universe_avg), 2) if pd.notna(universe_avg) else None,
        'universe_median_return_pct': round(float(universe_median), 2) if pd.notna(universe_median) else None,
        'excess_top_vs_pool_avg': round(float(top_avg - pool_avg), 2) if pd.notna(top_avg) and pd.notna(pool_avg) else None,
        'excess_top_vs_universe_avg': round(float(top_avg - universe_avg), 2) if pd.notna(top_avg) and pd.notna(universe_avg) else None,
        'win_rate_vs_pool_median_pct': round(win_vs_pool_median, 1) if pd.notna(win_vs_pool_median) else None,
        'win_rate_vs_universe_median_pct': round(win_vs_universe_median, 1) if pd.notna(win_vs_universe_median) else None,
    }

    logger.info('\n' + '=' * 70)
    logger.info('回测汇总')
    logger.info('=' * 70)
    for k, v in summary.items():
        logger.info(f'  {k}: {v}')
    logger.info('\nTop N 持有期表现:')
    cols = [c for c in ['基金代码','基金简称','基金类型','综合得分','持有期收益'] if c in top_df.columns]
    for _, r in top_df[cols].iterrows():
        logger.info(f'  {r["基金代码"]} {str(r.get("基金简称",""))[:24]:<24s} 得分={r["综合得分"]:.1f} 持有={r["持有期收益"]:.2f}%')

    return {
        'summary': summary,
        'top_n': top_df,
        'passed': passed,
        'all': df,
    }


# ============================================================================
# CLI
# ============================================================================

def _write_excel(result, out_path):
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    drop_internal = ['_nav_history', '_nav_holdout', '_composite']
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        pd.DataFrame([result['summary']]).T.rename(columns={0: 'value'}).to_excel(writer, sheet_name='summary')
        result['top_n'].drop(columns=drop_internal, errors='ignore').to_excel(writer, sheet_name='top_n', index=False)
        result['passed'].drop(columns=drop_internal, errors='ignore').to_excel(writer, sheet_name='passed', index=False)
        result['all'].drop(columns=drop_internal, errors='ignore').to_excel(writer, sheet_name='all', index=False)
    logger.info(f'已写出: {out_path}')


def main():
    parser = argparse.ArgumentParser(description='Fund Screener — 回测验证')
    parser.add_argument('--date', default='2024-01-01', help='回测起点 T (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='持有期结束 (YYYY-MM-DD), 默认今天')
    parser.add_argument('--pool-size', type=int, default=None, help='候选池规模, 默认从 config 取')
    parser.add_argument('--top-n', type=int, default=None, help='Top N, 默认从 config 取')
    parser.add_argument('--max-universe', type=int, default=600, help='宇宙上限(避免给 3000+ 只都抓 NAV), 默认 600')
    parser.add_argument('--out', default='./output/backtest_result.xlsx', help='Excel 输出路径')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    result = run_backtest(args.date, args.end, args.pool_size, args.top_n, args.max_universe)
    if result is None:
        logger.error('回测失败, 未生成结果')
        sys.exit(1)
    _write_excel(result, args.out)


if __name__ == '__main__':
    main()

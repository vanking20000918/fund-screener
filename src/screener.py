"""
筛选与评分主逻辑
流程:
1. 拉取全市场股票型+混合型+QDII基金排名
2. 应用初步过滤(规模、收益率排名)缩小候选池
3. 对候选池获取详细数据(净值、经理信息、持仓)
4. 应用 6 项硬性筛选
5. 计算 7 个维度软性评分
6. 加权综合排序输出 Top N
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd

from .config import (
    DATA_SOURCE, FUND_TYPES, HARD_FILTER, SCORE_WEIGHTS, TOP_N, PERF_CONFIG, POOL_RANK_WEIGHTS
)
from . import metrics

# 并发抓取工作线程数(与 data_fetcher_eastmoney.HTTP_WORKERS 对齐)
HTTP_WORKERS = 8

# 根据配置选择数据源
if DATA_SOURCE == 'eastmoney':
    from . import data_fetcher_eastmoney as df_module
    logger_prefix = '[天天基金]'
else:
    from . import data_fetcher as df_module
    logger_prefix = '[AKShare]'

logger = logging.getLogger(__name__)


def _find_col(df, *keywords):
    """从 DataFrame 找包含所有 keywords 的列名"""
    for col in df.columns:
        if all(k in col for k in keywords):
            return col
    return None


# 模块级缓存,供软评分阶段引用全市场参考数据
_FULL_MARKET_REF = {
    'rank_3y': None,    # 全市场近3年收益率(去 NaN), 用于 stability 全市场分位 (保留作 fallback)
    'rank_1y': None,
    'rank_ytd': None,
}

# 候选池合成指数 (按日期对齐的归一化 NAV 中位序列), 供熊市自动检测
_POOL_INDEX_NAV = None


def get_candidate_pool():
    """
    第一步: 获取候选池
    使用综合排名 (近3年 50% + 近1年 30% + 今年来 20%) 取代单一近3年排序
    同时缓存全市场参考数据供后续分位计算
    """
    logger.info('=' * 60)
    logger.info('Step 1: 获取候选池')
    logger.info('=' * 60)

    pieces = []
    if '股票型' in FUND_TYPES:
        df1 = df_module.fetch_fund_rank_stock()
        if df1 is not None and len(df1) > 0:
            df1['基金类型'] = '股票型'
            pieces.append(df1)
    if '混合型' in FUND_TYPES:
        df2 = df_module.fetch_fund_rank_mixed()
        if df2 is not None and len(df2) > 0:
            df2['基金类型'] = '混合型'
            pieces.append(df2)
    if 'QDII' in FUND_TYPES:
        df3 = df_module.fetch_fund_rank_qdii()
        if df3 is not None and len(df3) > 0:
            df3['基金类型'] = 'QDII'
            pieces.append(df3)

    if not pieces:
        logger.error('未获取到任何基金排名数据')
        return pd.DataFrame()

    df = pd.concat(pieces, ignore_index=True)
    logger.info(f'全市场基金合计: {len(df)} 只')

    # 标准化列名
    if '基金代码' not in df.columns:
        c = _find_col(df, '代码')
        if c:
            df = df.rename(columns={c: '基金代码'})
    if '基金简称' not in df.columns:
        for kw in ('简称', '名称'):
            c = _find_col(df, kw)
            if c:
                df = df.rename(columns={c: '基金简称'})
                break

    df['基金代码'] = df['基金代码'].astype(str).str.zfill(6)

    # 预过滤: 仅保留成立 ≥ min_fund_age 年的基金, 避免新基金"成立以来"收益挤占候选池
    # (eastmoney 对新基金的 近3年 列实为"成立以来"收益, 不预过滤会导致硬筛阶段全军覆没)
    founded_col = _find_col(df, '成立日期') or _find_col(df, '成立日')
    if founded_col:
        from .config import HARD_FILTER as _HF
        min_age = _HF.get('min_fund_age', 3)
        df['_founded'] = pd.to_datetime(df[founded_col], errors='coerce')
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=int(365 * min_age))
        n_before = len(df)
        # 成立日期缺失的基金保留(由后续硬筛处理), 仅过滤掉确认 < min_age 年的
        keep_mask = df['_founded'].isna() | (df['_founded'] <= cutoff)
        df = df[keep_mask].copy().drop(columns=['_founded'])
        logger.info(f'按成立日期 ≥ {min_age} 年预过滤: {n_before} → {len(df)} 只')

    # 找各期收益列(eastmoney 列名: 近1年/近3年/今年来; akshare 类似)
    col_3y = _find_col(df, '近3年')
    col_1y = _find_col(df, '近1年')
    col_ytd = _find_col(df, '今年来') or _find_col(df, '今年')

    for c in (col_3y, col_1y, col_ytd):
        if c:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # 缓存全市场各期收益, 供软评分 stability 分位计算 (近3年优先, 近1年/今年来 兜底)
    if col_3y:
        _FULL_MARKET_REF['rank_3y'] = df[col_3y].dropna().tolist()
    if col_1y:
        _FULL_MARKET_REF['rank_1y'] = df[col_1y].dropna().tolist()
    if col_ytd:
        _FULL_MARKET_REF['rank_ytd'] = df[col_ytd].dropna().tolist()
    logger.info(
        f'全市场参考分位样本数: 近3年={len(_FULL_MARKET_REF["rank_3y"] or [])}, '
        f'近1年={len(_FULL_MARKET_REF["rank_1y"] or [])}, '
        f'今年来={len(_FULL_MARKET_REF["rank_ytd"] or [])}'
    )

    # 综合排名: 各期收益分位加权
    # NaN 值: 显式 fillna(0) 以避免数据缺失被排到"最好"(bug fix)
    composite_score = pd.Series(0.0, index=df.index)
    weight_used = 0.0
    label_map = {'近3年': col_3y, '近1年': col_1y, '今年来': col_ytd}
    for label, col in label_map.items():
        w = POOL_RANK_WEIGHTS.get(label, 0)
        if col is None or w == 0:
            continue
        rank_pct = df[col].rank(ascending=True, pct=True, na_option='keep').fillna(0)
        composite_score = composite_score + rank_pct * w
        weight_used += w

    if weight_used == 0:
        logger.warning('无可用收益列, 回退到原始顺序')
    else:
        df['_composite'] = composite_score / weight_used
        df = df.sort_values('_composite', ascending=False, na_position='last')

    pool_size = PERF_CONFIG['candidate_pool_size']
    candidate = df.head(pool_size).copy()
    logger.info(f'综合排名筛出候选池: {len(candidate)} 只 (权重: {POOL_RANK_WEIGHTS})')

    # 固化近1年收益到标准列名供硬筛使用
    if col_1y and '近1年收益率' not in candidate.columns:
        candidate['近1年收益率'] = candidate[col_1y]
    return candidate


def enrich_with_manager_info(candidate_df):
    """第二步: 给候选池加上基金经理信息(任职年限、在管基金数)"""
    logger.info('=' * 60)
    logger.info('Step 2: 补充基金经理信息')
    logger.info('=' * 60)

    if DATA_SOURCE == 'eastmoney':
        return _enrich_manager_eastmoney(candidate_df)
    else:
        return _enrich_manager_akshare(candidate_df)


def _enrich_manager_eastmoney(candidate_df):
    """天天基金模式: 从详情页批量获取经理信息"""
    codes = candidate_df['基金代码'].tolist()
    details_df = df_module.fetch_fund_details_batch(codes)

    if details_df is None or len(details_df) == 0:
        candidate_df['经理任职年限'] = np.nan
        candidate_df['经理在管基金数'] = np.nan
        candidate_df['基金经理'] = ''
        return candidate_df

    # 计算任职年限
    if '经理任职起始' in details_df.columns:
        def _calc_tenure_from_date(d):
            if pd.isna(d) or not d:
                return np.nan
            try:
                start = pd.to_datetime(d)
                return (datetime.now() - start).days / 365
            except Exception:
                return np.nan
        details_df['经理任职年限'] = details_df['经理任职起始'].apply(_calc_tenure_from_date)
    elif '任职期间' in details_df.columns:
        details_df['经理任职年限'] = details_df['任职期间'].apply(_parse_tenure)

    # 计算在管基金数(按经理名分组)
    if '基金经理' in details_df.columns:
        manager_counts = details_df['基金经理'].value_counts().to_dict()
        details_df['经理在管基金数'] = details_df['基金经理'].map(manager_counts).fillna(0).astype(int)

    # 合并到候选池
    merge_cols = ['基金代码']
    for col in ['基金经理', '经理任职年限', '经理在管基金数', '基金规模', '成立日期']:
        if col in details_df.columns:
            merge_cols.append(col)

    details_df['基金代码'] = details_df['基金代码'].astype(str).str.zfill(6)
    candidate_df = candidate_df.merge(
        details_df[merge_cols].drop_duplicates(subset='基金代码', keep='first'),
        on='基金代码', how='left'
    )

    for col, default in [('经理任职年限', np.nan), ('经理在管基金数', np.nan), ('基金经理', '')]:
        if col not in candidate_df.columns:
            candidate_df[col] = default

    logger.info(f'已补充经理信息,有效任职年限数: {candidate_df["经理任职年限"].notna().sum()}/{len(candidate_df)}')
    return candidate_df


def _enrich_manager_akshare(candidate_df):
    """AKShare 模式: 从全量经理表获取"""
    try:
        manager_df = df_module.fetch_all_managers()
    except Exception as e:
        logger.error(f'获取经理数据失败: {e}')
        candidate_df['经理任职年限'] = np.nan
        candidate_df['经理在管基金数'] = np.nan
        candidate_df['基金经理'] = ''
        return candidate_df

    if manager_df is None or len(manager_df) == 0:
        candidate_df['经理任职年限'] = np.nan
        candidate_df['经理在管基金数'] = np.nan
        candidate_df['基金经理'] = ''
        return candidate_df

    code_col = '现任基金代码' if '现任基金代码' in manager_df.columns else None
    name_col = '姓名' if '姓名' in manager_df.columns else None
    tenure_col = '累计从业时间' if '累计从业时间' in manager_df.columns else None

    if not code_col:
        for col in manager_df.columns:
            if '代码' in col and not code_col:
                code_col = col
            elif ('姓名' in col or '基金经理' == col) and not name_col:
                name_col = col
            elif '累计' in col and not tenure_col:
                tenure_col = col

    if code_col:
        manager_df['_code'] = manager_df[code_col].astype(str).str.zfill(6)
        if name_col:
            fund_count = manager_df.groupby(name_col)['_code'].nunique().reset_index()
            fund_count.columns = [name_col, '经理在管基金数']
            manager_df = manager_df.merge(fund_count, on=name_col, how='left')
        if tenure_col:
            manager_df['经理任职年限'] = manager_df[tenure_col].apply(_parse_tenure)

        sub_cols = ['_code']
        if name_col:
            manager_df = manager_df.rename(columns={name_col: '基金经理'})
            sub_cols.append('基金经理')
        if '经理在管基金数' in manager_df.columns:
            sub_cols.append('经理在管基金数')
        if '经理任职年限' in manager_df.columns:
            sub_cols.append('经理任职年限')

        sub = manager_df[sub_cols].drop_duplicates(subset='_code', keep='first')
        candidate_df = candidate_df.merge(sub, left_on='基金代码', right_on='_code', how='left')
        candidate_df = candidate_df.drop(columns=['_code'], errors='ignore')

        if '经理任职年限' not in candidate_df.columns:
            candidate_df['经理任职年限'] = np.nan
    else:
        candidate_df['经理任职年限'] = np.nan
        candidate_df['经理在管基金数'] = np.nan
        candidate_df['基金经理'] = ''

    logger.info(f'已补充经理信息,有效任职年限数: {candidate_df["经理任职年限"].notna().sum()}/{len(candidate_df)}')
    return candidate_df


def _parse_tenure(val):
    """
    解析任职时长为年数。
    支持: 纯天数(1352)、'X年X天'、'X年又X天'、纯数字字符串
    """
    if pd.isna(val):
        return np.nan
    if isinstance(val, (int, float)):
        v = float(val)
        return v / 365 if v > 100 else v
    s = str(val).strip()
    # 去掉 "又" 字: "3年又166天" → "3年166天"
    s = s.replace('又', '')
    try:
        years = 0
        if '年' in s:
            parts = s.split('年')
            years = float(parts[0])
            if len(parts) > 1 and '天' in parts[1]:
                days = float(parts[1].replace('天', ''))
                years += days / 365
        elif '天' in s:
            years = float(s.replace('天', '')) / 365
        else:
            v = float(s)
            return v / 365 if v > 100 else v
        return years
    except (ValueError, IndexError):
        return np.nan


def _compute_nav_metrics_one(code):
    """单只基金: 拉净值 + 计算所有净值类指标. 线程安全, 返回 (result_dict, annual_rets_list, normalized_nav_series)"""
    try:
        nav_df = df_module.fetch_fund_nav(code)
        if nav_df is None or len(nav_df) == 0:
            return _empty_nav_metrics(code), [], None

        max_dd = metrics.calc_recent_drawdown(nav_df, years=3)

        date_col = '净值日期' if '净值日期' in nav_df.columns else nav_df.columns[0]
        nav_col = '单位净值' if '单位净值' in nav_df.columns else nav_df.columns[1]
        dates = pd.to_datetime(nav_df[date_col], errors='coerce')
        nav_values = pd.to_numeric(nav_df[nav_col], errors='coerce')
        valid = dates.notna() & nav_values.notna()
        dates_arr = dates[valid].values
        nav_values_arr = nav_values[valid].values

        if len(nav_values_arr) > 1:
            annual_ret = metrics.calc_annual_return(nav_values_arr, dates_arr)
            vol = metrics.calc_volatility(nav_values_arr)
            sharpe = metrics.calc_sharpe(nav_values_arr, dates_arr)
        else:
            annual_ret = vol = sharpe = np.nan

        calmar = metrics.calc_calmar(annual_ret, max_dd)
        bear_count = metrics.calc_bear_market_count(nav_df)
        bear_dd_map = metrics.calc_bear_period_drawdown(nav_df)
        bear_avg_dd = metrics.calc_avg_bear_drawdown(bear_dd_map)
        annual_rets = metrics.calc_annual_returns_by_year(nav_df, years=5)

        # 归一化 NAV 序列 (从首日 1.0 起算), 供合成候选池中位指数
        norm_nav = None
        if len(nav_values_arr) >= 30 and nav_values_arr[0] > 0:
            norm_nav = pd.Series(nav_values_arr / nav_values_arr[0], index=pd.to_datetime(dates_arr))
            norm_nav = norm_nav[~norm_nav.index.duplicated(keep='first')].sort_index()

        return {
            '基金代码': code,
            '近3年最大回撤': max_dd if max_dd and max_dd > 0 else np.nan,
            '年化收益率': annual_ret,
            '年化波动率': vol,
            '夏普比率': sharpe,
            '卡玛比率': calmar,
            '熊市数': bear_count,
            '熊市平均回撤': bear_avg_dd,
            '年度收益': annual_rets,
        }, annual_rets, norm_nav
    except Exception as e:
        logger.warning(f'  基金 {code} 计算失败: {e}')
        return _empty_nav_metrics(code), [], None


def enrich_with_nav_metrics(candidate_df):
    """第三步: 对候选池每只基金计算净值类指标(回撤/收益/夏普/卡玛/熊市表现), 并发抓取"""
    logger.info('=' * 60)
    logger.info('Step 3: 计算净值类指标(回撤、收益、卡玛、熊市表现)')
    logger.info('=' * 60)

    results = []
    annual_returns_pool = {}  # 候选池年度收益, 作为辅助参考
    norm_navs = {}            # code -> normalized NAV Series, 用于合成候选池中位指数
    total = len(candidate_df)
    codes = candidate_df['基金代码'].tolist()

    logger.info(f'并发抓取净值 (workers={HTTP_WORKERS}), 共 {total} 只...')
    completed = 0
    with ThreadPoolExecutor(max_workers=HTTP_WORKERS) as ex:
        future_to_code = {ex.submit(_compute_nav_metrics_one, c): c for c in codes}
        for fut in as_completed(future_to_code):
            res, annual_rets, norm_nav = fut.result()
            results.append(res)
            # 主线程汇总, 无并发写竞争
            for y, r in annual_rets:
                annual_returns_pool.setdefault(y, []).append(r)
            if norm_nav is not None and len(norm_nav) >= 30:
                norm_navs[res['基金代码']] = norm_nav
            completed += 1
            if completed % 50 == 0 or completed == total:
                logger.info(f'  进度: {completed}/{total}')

    # 合成候选池中位 NAV 指数 (按日期外连接 → 中位数), 供熊市自动检测
    if norm_navs:
        wide = pd.concat(norm_navs.values(), axis=1, join='outer')
        global _POOL_INDEX_NAV
        _POOL_INDEX_NAV = wide.median(axis=1).dropna()
        logger.info(f'候选池中位 NAV 指数已合成: 长度={len(_POOL_INDEX_NAV)}, '
                    f'日期范围={_POOL_INDEX_NAV.index.min().date()} → {_POOL_INDEX_NAV.index.max().date()}')

    metrics_df = pd.DataFrame(results)
    candidate_df = candidate_df.merge(metrics_df, on='基金代码', how='left')

    # 业绩排名分位: 优先候选池内分位(0=最好,100=最差), 兜底全市场分位.
    # 候选池本身已是全市场前 ~10%, 直接用全市场分位会把全员压到 100 分(评分饱和).
    logger.info('计算业绩排名分位 (候选池内优先 + 全市场兜底)...')
    rank_3y_col = _find_col(candidate_df, '近3年')
    rank_1y_col = _find_col(candidate_df, '近1年')
    rank_ytd_col = _find_col(candidate_df, '今年来') or _find_col(candidate_df, '今年')

    pool_rank_3y = pool_rank_1y = pool_rank_ytd = None
    if rank_3y_col:
        v = pd.to_numeric(candidate_df[rank_3y_col], errors='coerce')
        pool_rank_3y = (1 - v.rank(ascending=True, pct=True, na_option='keep')) * 100
    if rank_1y_col:
        v = pd.to_numeric(candidate_df[rank_1y_col], errors='coerce')
        pool_rank_1y = (1 - v.rank(ascending=True, pct=True, na_option='keep')) * 100
    if rank_ytd_col:
        v = pd.to_numeric(candidate_df[rank_ytd_col], errors='coerce')
        pool_rank_ytd = (1 - v.rank(ascending=True, pct=True, na_option='keep')) * 100

    fallback_chain = [
        ('近3年', rank_3y_col, _FULL_MARKET_REF.get('rank_3y') or []),
        ('近1年', rank_1y_col, _FULL_MARKET_REF.get('rank_1y') or []),
        ('今年来', rank_ytd_col, _FULL_MARKET_REF.get('rank_ytd') or []),
    ]

    def _stability_percentile(idx, row):
        # 优先: 候选池内近3年→近1年→今年来分位
        for pool_pct in (pool_rank_3y, pool_rank_1y, pool_rank_ytd):
            if pool_pct is not None:
                p = pool_pct.get(idx)
                if pd.notna(p):
                    return p
        # 兜底: 全市场分位
        for _label, col_name, ref in fallback_chain:
            if col_name and ref:
                val = pd.to_numeric(row.get(col_name), errors='coerce')
                p = metrics.calc_market_percentile(val, ref)
                if pd.notna(p):
                    return p
        ar = row.get('年度收益', [])
        if ar:
            return metrics.calc_performance_rank_percentile(ar, annual_returns_pool)
        return np.nan

    candidate_df['业绩排名分位'] = [
        _stability_percentile(idx, row) for idx, row in candidate_df.iterrows()
    ]
    valid_n = candidate_df['业绩排名分位'].notna().sum()
    logger.info(f'业绩排名分位有效数 (候选池内): {valid_n}/{len(candidate_df)}')
    return candidate_df


def _empty_nav_metrics(code):
    return {
        '基金代码': code,
        '近3年最大回撤': np.nan,
        '年化收益率': np.nan,
        '年化波动率': np.nan,
        '夏普比率': np.nan,
        '卡玛比率': np.nan,
        '熊市数': 0,
        '熊市平均回撤': np.nan,
        '年度收益': [],
    }


def enrich_with_industry(candidate_df, only_passed=True):
    """
    第三B步: 拉取近 2 年行业配置, 计算风格(行业)一致性
    only_passed=True 时仅给硬筛通过的基金抓取(性能优化, 省 60-70% API 调用)
    数据缺失返回 NaN; 软评分阶段会回退到波动率代理
    """
    logger.info('=' * 60)
    logger.info('Step 7: 计算行业配置稳定性(风格一致性)')
    logger.info('=' * 60)

    candidate_df = candidate_df.copy()
    if '行业稳定性' not in candidate_df.columns:
        candidate_df['行业稳定性'] = np.nan

    if not hasattr(df_module, 'fetch_recent_industry_allocations'):
        logger.info(f'{logger_prefix} 数据源不支持行业配置, 跳过(风格维度将由波动率代理填充)')
        return candidate_df

    # 决定哪些基金需要拉取行业数据
    if only_passed and '硬筛通过' in candidate_df.columns:
        target_mask = candidate_df['硬筛通过'] == True
        n_target = int(target_mask.sum())
        logger.info(f'仅给硬筛通过的 {n_target} 只基金抓行业配置 (节省 ~{len(candidate_df) - n_target} 次 API 调用)')
    else:
        target_mask = pd.Series(True, index=candidate_df.index)
        n_target = len(candidate_df)

    if n_target == 0:
        logger.info('无目标基金, 跳过行业配置抓取')
        return candidate_df

    def _industry_one(idx, code):
        try:
            allocs = df_module.fetch_recent_industry_allocations(code, years=2)
            sim = metrics.calc_industry_similarity(allocs) if allocs else np.nan
        except Exception as e:
            logger.debug(f'  基金 {code} 行业稳定性计算失败: {e}')
            sim = np.nan
        return idx, sim

    targets = [(idx, row['基金代码']) for idx, row in candidate_df[target_mask].iterrows()]
    sims = {}
    fetched = 0
    failed = 0
    logger.info(f'并发抓取行业配置 (workers={HTTP_WORKERS}), 共 {n_target} 只...')
    with ThreadPoolExecutor(max_workers=HTTP_WORKERS) as ex:
        futures = [ex.submit(_industry_one, idx, code) for idx, code in targets]
        for fut in as_completed(futures):
            idx, sim = fut.result()
            sims[idx] = sim
            if pd.isna(sim):
                failed += 1
            else:
                fetched += 1
            done = fetched + failed
            if done % 20 == 0 or done == n_target:
                logger.info(f'  进度: {done}/{n_target} (成功={fetched}, 失败={failed})')

    candidate_df.loc[list(sims.keys()), '行业稳定性'] = pd.Series(sims)
    logger.info(f'行业稳定性最终完整度: 成功={fetched}, 失败={failed}, 跳过={len(candidate_df) - n_target}')
    return candidate_df


def enrich_with_basics(candidate_df):
    """第四步: 补充基金规模、成立时间、费率等基础信息"""
    logger.info('=' * 60)
    logger.info('Step 4: 补充基金基础信息(规模、成立时间、费率)')
    logger.info('=' * 60)

    # 1. 规模: 检查是否已存在(天天基金模式下详情页已获取)
    if '基金规模' in candidate_df.columns:
        candidate_df['基金规模'] = pd.to_numeric(candidate_df['基金规模'], errors='coerce')
    else:
        scale_col = None
        for col in candidate_df.columns:
            if '规模' in col or '净资产' in col:
                scale_col = col
                break
        if scale_col:
            candidate_df['基金规模'] = pd.to_numeric(candidate_df[scale_col], errors='coerce')
        elif DATA_SOURCE == 'akshare':
            try:
                manager_df = df_module.fetch_all_managers()
                if manager_df is not None and '现任基金资产总规模' in manager_df.columns:
                    scale_sub = manager_df[['现任基金代码', '现任基金资产总规模']].copy()
                    scale_sub['现任基金代码'] = scale_sub['现任基金代码'].astype(str).str.zfill(6)
                    scale_sub['现任基金资产总规模'] = pd.to_numeric(scale_sub['现任基金资产总规模'], errors='coerce')
                    scale_sub = scale_sub.drop_duplicates(subset='现任基金代码', keep='first')
                    candidate_df = candidate_df.merge(
                        scale_sub.rename(columns={'现任基金代码': '_sc', '现任基金资产总规模': '基金规模'}),
                        left_on='基金代码', right_on='_sc', how='left'
                    )
                    candidate_df = candidate_df.drop(columns=['_sc'], errors='ignore')
                else:
                    candidate_df['基金规模'] = np.nan
            except Exception as e:
                logger.warning(f'从经理表获取规模失败: {e}')
                candidate_df['基金规模'] = np.nan
        else:
            candidate_df['基金规模'] = np.nan

    # 2. 成立时间: 检查是否已存在
    if '成立日期' in candidate_df.columns:
        candidate_df['成立日期'] = pd.to_datetime(candidate_df['成立日期'], errors='coerce')
        candidate_df['基金年龄'] = (datetime.now() - candidate_df['成立日期']).dt.days / 365
    else:
        founded_col = None
        for col in candidate_df.columns:
            if '成立日' in col:
                founded_col = col
                break
        if founded_col:
            candidate_df['成立日期'] = pd.to_datetime(candidate_df[founded_col], errors='coerce')
            candidate_df['基金年龄'] = (datetime.now() - candidate_df['成立日期']).dt.days / 365
        else:
            if '经理任职年限' in candidate_df.columns:
                candidate_df['基金年龄'] = candidate_df['经理任职年限']
            else:
                candidate_df['基金年龄'] = np.nan
            candidate_df['成立日期'] = pd.NaT

    return candidate_df


def apply_hard_filter(df):
    """
    第五步: 硬性筛选
    设计: NaN 一律放行(由软评分通过中位数降权); 用绝对阈值代替"同类中位数"
    """
    logger.info('=' * 60)
    logger.info('Step 5: 应用硬性筛选')
    logger.info('=' * 60)

    n0 = len(df)
    df = df.copy()
    df['硬筛通过'] = True
    df['淘汰原因'] = ''

    hf = HARD_FILTER

    # 数据完整度诊断
    logger.info('硬筛输入数据完整度 (有效/总数, 数值列含 min/median/max):')
    for col in ['经理任职年限', '基金年龄', '基金规模', '经理在管基金数', '近3年最大回撤', '近1年收益率']:
        if col not in df.columns:
            logger.info(f'  {col}: 列不存在 (该项硬筛将跳过)')
            continue
        series = pd.to_numeric(df[col], errors='coerce')
        valid = series.notna().sum()
        if valid > 0:
            logger.info(
                f'  {col}: {valid}/{len(df)} 有效, '
                f'min={series.min():.2f} median={series.median():.2f} max={series.max():.2f}'
            )
        else:
            logger.info(f'  {col}: 0/{len(df)} 全部 NaN (该项硬筛将全部放行)')

    def _fail(mask, reason):
        df.loc[mask, '硬筛通过'] = False
        df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + reason + ';'

    # 1. 经理任职年限
    _fail(
        df['经理任职年限'].notna() & (df['经理任职年限'] < hf['min_manager_years']),
        f'经理任职<{hf["min_manager_years"]}年',
    )

    # 2. 基金年龄
    _fail(
        df['基金年龄'].notna() & (df['基金年龄'] < hf['min_fund_age']),
        f'基金成立<{hf["min_fund_age"]}年',
    )

    # 3. 规模
    _fail(
        df['基金规模'].notna() & ((df['基金规模'] < hf['min_scale']) | (df['基金规模'] > hf['max_scale'])),
        f'规模不在{hf["min_scale"]}-{hf["max_scale"]}亿',
    )

    # 4. 经理在管基金数
    if '经理在管基金数' in df.columns:
        count_num = pd.to_numeric(df['经理在管基金数'], errors='coerce')
        _fail(
            count_num.notna() & (count_num > hf['max_funds_per_manager']),
            f'在管>{hf["max_funds_per_manager"]}只',
        )

    # 5. 回撤(绝对阈值)
    _fail(
        df['近3年最大回撤'].notna() & (df['近3年最大回撤'] > hf['max_drawdown_pct']),
        f'近3年回撤>{hf["max_drawdown_pct"]}%',
    )

    # 6. 近1年收益兜底(防长期好但近期暴雷)
    if '近1年收益率' in df.columns:
        r1y = pd.to_numeric(df['近1年收益率'], errors='coerce')
        _fail(
            r1y.notna() & (r1y < hf['min_recent_1y_return']),
            f'近1年收益<{hf["min_recent_1y_return"]}%',
        )

    passed = df['硬筛通过'].sum()
    logger.info(f'硬筛结果: {n0} 只 → {passed} 只通过')

    # 淘汰原因统计 (一只基金可能因多条规则淘汰, 各条都计数)
    failed_df = df[~df['硬筛通过']]
    if len(failed_df) > 0:
        reason_counts = {}
        for r in failed_df['淘汰原因']:
            for reason in r.rstrip(';').split(';'):
                reason = reason.strip()
                if reason:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        logger.info('硬筛淘汰原因 Top:')
        for reason, n in sorted(reason_counts.items(), key=lambda x: -x[1]):
            logger.info(f'  {reason}: {n} 只')
    return df


def calc_soft_score(df):
    """
    第六步: 软性评分(NaN 一律返回 50, 不再分散两套策略)
    """
    logger.info('=' * 60)
    logger.info('Step 6: 计算软性评分')
    logger.info('=' * 60)

    df = df.copy()
    MEDIAN = 50

    def score_stability(percentile):
        """业绩排名分位 → 得分 (评分卡 v2.1: 候选池内分位 + 阈值密化)
        候选池本身是全市场前 ~10%, 进一步在池内排序;
        阈值密化避免顶部全员 100, 让评分卡恢复区分度。"""
        if pd.isna(percentile):
            return MEDIAN
        if percentile <= 5:    return 100
        if percentile <= 10:   return 92
        if percentile <= 20:   return 82
        if percentile <= 35:   return 68
        if percentile <= 55:   return 55
        if percentile <= 75:   return 40
        if percentile <= 90:   return 28
        return 18

    def score_framework(calmar):
        # 卡玛比率 = 年化收益 / 最大回撤
        if pd.isna(calmar):
            return MEDIAN
        if calmar >= 0.8:
            return 100
        elif calmar >= 0.5:
            return 85
        elif calmar >= 0.3:
            return 70
        elif calmar >= 0.1:
            return 55
        elif calmar >= 0:
            return 40
        else:
            return 20

    def score_scale(scale):
        """规模适中度: 钟形分段线性
        甜区 [5,30] 亿满分; [2,5] 缓升; [30,50] 轻度下滑;
        [50,100] 调仓难度上升明显; >100 亿超大型继续衰减; ≥200 封底 30"""
        if pd.isna(scale):
            return MEDIAN
        if scale < 2:
            # 硬筛会过滤, 兜底 NaN 放行场景
            return max(20.0, 30 + scale / 2 * 30)
        if scale < 5:
            return 60 + (scale - 2) / 3 * 40       # 2→60, 5→100
        if scale <= 30:
            return 100                              # 甜区
        if scale <= 50:
            return 100 - (scale - 30) / 20 * 15    # 30→100, 50→85
        if scale <= 100:
            return 85 - (scale - 50) / 50 * 30     # 50→85, 100→55
        if scale <= 200:
            return 55 - (scale - 100) / 100 * 25   # 100→55, 200→30
        return 30

    def score_tenure(years):
        """经理任职年限: 分段线性, 边际递减
        关键点 3年=55, 5年=72, 7年=85, 10年=95, 15+年=100"""
        if pd.isna(years):
            return MEDIAN
        if years < 3:
            # 硬筛会过滤, 兜底 NaN 放行场景
            return max(20.0, 40 + years / 3 * 15)
        if years < 5:
            return 55 + (years - 3) / 2 * 17       # 3→55, 5→72
        if years < 7:
            return 72 + (years - 5) / 2 * 13       # 5→72, 7→85
        if years < 10:
            return 85 + (years - 7) / 3 * 10       # 7→85, 10→95
        if years < 15:
            return 95 + (years - 10) / 5 * 5       # 10→95, 15→100
        return 100

    # 风格一致性: 候选池内行业相似度分位 (v2.1) → 阈值密化
    # 主动基金 industry_similarity 普遍 0.85-1.0, 绝对阈值无区分度
    sim_series = pd.to_numeric(df.get('行业稳定性'), errors='coerce')
    valid_sim = sim_series.dropna()
    if len(valid_sim) >= 10:
        # rank 高 = 行业稳定 = 好 → 转成 0=最好,100=最差 的池内分位
        style_pct = (1 - sim_series.rank(ascending=True, pct=True, na_option='keep')) * 100
        df['_行业稳定性分位'] = style_pct
    else:
        df['_行业稳定性分位'] = np.nan

    def score_style_industry_pool(pct):
        if pd.isna(pct):
            return None
        if pct <= 10:   return 100
        if pct <= 25:   return 88
        if pct <= 45:   return 72
        if pct <= 65:   return 55
        if pct <= 85:   return 40
        return 22

    def score_style_volatility(vol):
        if pd.isna(vol):
            return MEDIAN
        if vol < 18:    return 88
        if vol < 22:    return 72
        if vol < 26:    return 55
        if vol < 30:    return 40
        return 28

    def score_style(row):
        s = score_style_industry_pool(row.get('_行业稳定性分位'))
        if s is not None:
            return s
        return score_style_volatility(row.get('年化波动率'))

    # 熊市相对表现: 候选池内对"熊市平均回撤"做分位 + 阈值密化
    bear_dd_series = pd.to_numeric(df['熊市平均回撤'], errors='coerce')
    valid_bear = bear_dd_series.dropna()
    if len(valid_bear) >= 5:
        df['熊市回撤分位'] = bear_dd_series.rank(ascending=True, pct=True) * 100
    else:
        df['熊市回撤分位'] = np.nan

    def score_bear_perf(percentile, bear_count):
        """池内熊市回撤分位 → 得分 (v2.1 阈值密化)"""
        if pd.isna(percentile):
            return MEDIAN
        if percentile <= 8:    base = 100
        elif percentile <= 18: base = 90
        elif percentile <= 35: base = 75
        elif percentile <= 55: base = 60
        elif percentile <= 75: base = 42
        else:                  base = 28
        # 经验加成弱化: 0/1/2/3 轮 → -2/0/2/4
        bonus = (min(bear_count or 0, 3) - 1) * 2
        return float(min(100, max(0, base + bonus)))

    df['得分_稳定性'] = df['业绩排名分位'].apply(score_stability)
    df['得分_框架'] = df['卡玛比率'].apply(score_framework)
    df['得分_风格'] = df.apply(score_style, axis=1)
    df['得分_熊市'] = df.apply(lambda r: score_bear_perf(r.get('熊市回撤分位'), r.get('熊市数', 0)), axis=1)
    df['得分_规模'] = df['基金规模'].apply(score_scale)
    df['得分_任职'] = df['经理任职年限'].apply(score_tenure)

    w = SCORE_WEIGHTS
    df['综合得分'] = (
        df['得分_稳定性'] * w['stability']
        + df['得分_风格'] * w['style']
        + df['得分_框架'] * w['framework']
        + df['得分_熊市'] * w['bear_perf']
        + df['得分_规模'] * w['scale']
        + df['得分_任职'] * w['tenure']
    )

    def grade(s, passed):
        if not passed:
            return '未通过硬筛'
        if s >= 85:
            return 'A 级'
        elif s >= 70:
            return 'B 级'
        elif s >= 60:
            return 'C 级'
        else:
            return 'D 级'

    df['评级'] = df.apply(lambda r: grade(r['综合得分'], r['硬筛通过']), axis=1)
    return df


def add_explanation(df):
    """
    为每只基金生成人话版"入选关键原因".
    优先用具体数字 (业绩排位/熊市抗跌/卡玛/任职), 取 2-3 个最强项。
    fallback: 维度×分×权重 (评分卡机械版)
    """
    score_dim_to_weight = {
        '稳定性': SCORE_WEIGHTS['stability'],
        '熊市': SCORE_WEIGHTS['bear_perf'],
        '任职': SCORE_WEIGHTS['tenure'],
        '框架': SCORE_WEIGHTS['framework'],
        '风格': SCORE_WEIGHTS['style'],
        '规模': SCORE_WEIGHTS['scale'],
    }

    def _humanize(row):
        cards = []  # (priority_score, text) — priority 高的优先放在前面

        # 业绩排名 (越小越好, 0=最好)
        perf_pct = row.get('业绩排名分位')
        score_stab = row.get('得分_稳定性', 50)
        if pd.notna(perf_pct):
            if perf_pct <= 10:
                cards.append((score_stab + 20, f'近3年业绩排候选池前 {perf_pct:.0f}%'))
            elif perf_pct <= 30:
                cards.append((score_stab + 10, f'近3年业绩排候选池前 {perf_pct:.0f}%'))
            elif perf_pct <= 60:
                cards.append((score_stab, f'近3年业绩居候选池中游 ({perf_pct:.0f}% 分位)'))

        # 熊市抗跌
        bear_pct = row.get('熊市回撤分位')
        bear_dd = row.get('熊市平均回撤')
        bear_cnt = row.get('熊市数', 0)
        score_bear = row.get('得分_熊市', 50)
        if pd.notna(bear_dd) and pd.notna(bear_pct) and (bear_cnt or 0) >= 1:
            cnt_int = int(bear_cnt)
            if bear_pct <= 25:
                cards.append((score_bear + 15,
                              f'历 {cnt_int} 轮熊市平均仅回撤 {bear_dd:.1f}% (池内前 {bear_pct:.0f}%)'))
            elif bear_pct <= 55:
                cards.append((score_bear,
                              f'历 {cnt_int} 轮熊市平均回撤 {bear_dd:.1f}% (池内 {bear_pct:.0f}% 分位)'))

        # 卡玛
        calmar = row.get('卡玛比率')
        score_fr = row.get('得分_框架', 50)
        if pd.notna(calmar):
            if calmar >= 0.8:
                cards.append((score_fr + 10, f'卡玛比 {calmar:.2f} 收益/回撤匹配优秀'))
            elif calmar >= 0.5:
                cards.append((score_fr, f'卡玛比 {calmar:.2f} 风险收益均衡'))
            elif calmar >= 0.3:
                cards.append((score_fr - 5, f'卡玛比 {calmar:.2f} 中等'))

        # 任职
        tenure = row.get('经理任职年限')
        score_t = row.get('得分_任职', 50)
        if pd.notna(tenure):
            if tenure >= 10:
                cards.append((score_t + 8, f'经理任职 {tenure:.0f} 年, 完整周期老将'))
            elif tenure >= 7:
                cards.append((score_t, f'经理任职 {tenure:.0f} 年, 跨多轮市场'))
            elif tenure >= 5:
                cards.append((score_t - 3, f'经理任职 {tenure:.0f} 年'))

        # 规模
        scale = row.get('基金规模')
        score_sc = row.get('得分_规模', 50)
        if pd.notna(scale):
            if 5 <= scale <= 30:
                cards.append((score_sc - 10, f'规模 {scale:.1f} 亿 调仓灵活'))
            elif 30 < scale <= 60:
                cards.append((score_sc - 15, f'规模 {scale:.1f} 亿 偏大'))

        # 行业稳定性 (主动管理稳定度)
        ind_sim = row.get('行业稳定性')
        score_st = row.get('得分_风格', 50)
        if pd.notna(ind_sim) and ind_sim >= 0.95 and score_st >= 70:
            cards.append((score_st - 10, f'近2年行业配置稳定度 {ind_sim:.2f}'))

        # 取 priority 最高的 2-3 张卡片
        cards.sort(reverse=True)
        if cards:
            return '; '.join(c[1] for c in cards[:3])

        # fallback: 机械版
        contributions = []
        for dim, w in score_dim_to_weight.items():
            score = row.get(f'得分_{dim}')
            if pd.notna(score):
                contributions.append((dim, score, w, score * w))
        contributions.sort(key=lambda x: x[3], reverse=True)
        top3 = contributions[:3]
        return ' | '.join(f'{d}{s:.0f}×{w:.2f}' for d, s, w, _ in top3)

    df = df.copy()
    df['入选原因'] = df.apply(_humanize, axis=1)
    return df


def _auto_detect_bear_markets_if_enabled():
    """基于候选池中位 NAV 自动检测熊市段, 与 config.BEAR_MARKETS 对比给出建议.
    仅日志输出建议, 不会改变本次评分 (确保回测可复现)."""
    from .config import BEAR_DETECT_PARAMS, BEAR_MARKETS as CONF_BEAR
    if not BEAR_DETECT_PARAMS.get('enable', False):
        return
    if _POOL_INDEX_NAV is None or len(_POOL_INDEX_NAV) < 60:
        logger.info('候选池中位 NAV 不足, 跳过熊市自动检测')
        return
    try:
        detected = metrics.detect_bear_markets(_POOL_INDEX_NAV, BEAR_DETECT_PARAMS)
        new_segs = metrics.diff_bear_markets(
            detected, CONF_BEAR,
            overlap_days=BEAR_DETECT_PARAMS.get('recovery_overlap_days', 30),
        )
        if detected:
            logger.info(f'候选池中位 NAV 自动检测到 {len(detected)} 段熊市:')
            for s, e, dd in detected:
                logger.info(f'  {s} → {e} 回撤 {dd:.1f}%')
        if new_segs:
            logger.warning('=' * 60)
            logger.warning(f'⚠️  发现 {len(new_segs)} 段未在 config.BEAR_MARKETS 中的新熊市:')
            for s, e, dd in new_segs:
                logger.warning(f"    ('{s}', '{e}'),    # 自动检测, 池内回撤 {dd:.1f}%")
            logger.warning('如确认有效, 请把上述行追加到 src/config.py BEAR_MARKETS 列表')
            logger.warning('=' * 60)
    except Exception as e:
        logger.warning(f'熊市自动检测失败 (不影响月度评分): {e}')


def run_screening():
    """完整流程: 返回 Top N 基金 + 全部评分结果"""
    logger.info('\n' + '*' * 70)
    logger.info('开始基金筛选与评分')
    logger.info('*' * 70)

    candidate = get_candidate_pool()
    if len(candidate) == 0:
        return pd.DataFrame(), pd.DataFrame()

    candidate = enrich_with_manager_info(candidate)
    candidate = enrich_with_basics(candidate)
    candidate = enrich_with_nav_metrics(candidate)
    candidate = apply_hard_filter(candidate)
    # 行业配置抓取放在硬筛之后, 仅处理通过的基金以节省时间
    candidate = enrich_with_industry(candidate, only_passed=True)
    candidate = calc_soft_score(candidate)
    candidate = add_explanation(candidate)

    # 熊市自动检测 (基于候选池中位 NAV)
    _auto_detect_bear_markets_if_enabled()

    # 清理临时分位列
    candidate = candidate.drop(columns=['_行业稳定性分位'], errors='ignore')

    # 排序
    passed = candidate[candidate['硬筛通过']].copy()
    passed = passed.sort_values('综合得分', ascending=False)
    top_n = passed.head(TOP_N)

    logger.info(f'\n最终结果: Top {len(top_n)} 基金')
    if len(top_n) > 0:
        for i, (_, row) in enumerate(top_n.iterrows(), 1):
            logger.info(
                f'  {i:2d}. {row["基金代码"]} {row["基金简称"]:30s} '
                f'得分={row["综合得分"]:.1f} 评级={row["评级"]} | {row.get("入选原因", "")}'
            )

    return top_n, candidate

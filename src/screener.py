"""
筛选与评分主逻辑
流程:
1. 拉取全市场主动股票+偏股混合基金排名
2. 应用初步过滤(规模、收益率排名)缩小候选池
3. 对候选池获取详细数据(净值、经理信息、持仓)
4. 应用 6 项硬性筛选
5. 计算 7 个维度软性评分
6. 加权综合排序输出 Top N
"""
import logging
import time
from datetime import datetime

import numpy as np
import pandas as pd

from .config import (
    DATA_SOURCE, FUND_TYPES, HARD_FILTER, SCORE_WEIGHTS, TOP_N, PERF_CONFIG
)
from . import metrics

# 根据配置选择数据源
if DATA_SOURCE == 'eastmoney':
    from . import data_fetcher_eastmoney as df_module
    logger_prefix = '[天天基金]'
else:
    from . import data_fetcher as df_module
    logger_prefix = '[AKShare]'

logger = logging.getLogger(__name__)


def get_candidate_pool():
    """
    第一步: 获取候选池
    合并主动股票型+偏股混合型,做初步过滤
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

    if not pieces:
        logger.error('未获取到任何基金排名数据')
        return pd.DataFrame()

    df = pd.concat(pieces, ignore_index=True)
    logger.info(f'全市场主动权益基金合计: {len(df)} 只')

    # 标准化列名
    if '基金代码' not in df.columns:
        for col in df.columns:
            if '代码' in col:
                df = df.rename(columns={col: '基金代码'})
                break
    if '基金简称' not in df.columns:
        for col in df.columns:
            if '简称' in col or '名称' in col:
                df = df.rename(columns={col: '基金简称'})
                break

    # 确保基金代码是 6 位字符串
    df['基金代码'] = df['基金代码'].astype(str).str.zfill(6)

    # 初步过滤: 用近1年和近3年收益率排序,取前 N 名进入详细分析池
    # 因为对全市场每只都查询会非常慢
    return_cols = [c for c in df.columns if '近3年' in c]
    if return_cols:
        sort_col = return_cols[0]
        # 转为数值
        df[sort_col] = pd.to_numeric(df[sort_col], errors='coerce')
        df = df.sort_values(sort_col, ascending=False, na_position='last')

    pool_size = PERF_CONFIG['candidate_pool_size']
    candidate = df.head(pool_size).copy()
    logger.info(f'初步过滤后候选池: {len(candidate)} 只(按近3年收益率排序取前{pool_size})')
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


def enrich_with_nav_metrics(candidate_df):
    """第三步: 对候选池每只基金计算净值类指标"""
    logger.info('=' * 60)
    logger.info('Step 3: 计算净值类指标(回撤、收益、夏普、熊市数)')
    logger.info('=' * 60)

    results = []
    annual_returns_pool = {}  # {year: [所有基金收益列表]}

    total = len(candidate_df)
    for i, row in candidate_df.iterrows():
        code = row['基金代码']
        try:
            nav_df = df_module.fetch_fund_nav(code)
            if nav_df is None or len(nav_df) == 0:
                results.append({
                    '基金代码': code,
                    '近3年最大回撤': np.nan,
                    '年化收益率': np.nan,
                    '年化波动率': np.nan,
                    '夏普比率': np.nan,
                    '熊市数': 0,
                    '年度收益': [],
                })
                continue

            max_dd = metrics.calc_recent_drawdown(nav_df, years=3)

            date_col = '净值日期' if '净值日期' in nav_df.columns else nav_df.columns[0]
            nav_col = '单位净值' if '单位净值' in nav_df.columns else nav_df.columns[1]
            dates = pd.to_datetime(nav_df[date_col], errors='coerce')
            nav_values = pd.to_numeric(nav_df[nav_col], errors='coerce')
            valid = dates.notna() & nav_values.notna()
            dates = dates[valid].values
            nav_values = nav_values[valid].values

            annual_ret = metrics.calc_annual_return(nav_values, dates) if len(nav_values) > 1 else 0
            vol = metrics.calc_volatility(nav_values) if len(nav_values) > 1 else 0
            sharpe = metrics.calc_sharpe(nav_values, dates) if len(nav_values) > 1 else 0
            bear_count = metrics.calc_bear_market_count(nav_df)
            annual_rets = metrics.calc_annual_returns_by_year(nav_df, years=5)

            # 汇总到 pool 用于后续分位计算
            for y, r in annual_rets:
                annual_returns_pool.setdefault(y, []).append(r)

            results.append({
                '基金代码': code,
                '近3年最大回撤': max_dd,
                '年化收益率': annual_ret,
                '年化波动率': vol,
                '夏普比率': sharpe,
                '熊市数': bear_count,
                '年度收益': annual_rets,
            })

            if (i + 1) % 20 == 0:
                logger.info(f'  进度: {i+1}/{total}')

        except Exception as e:
            logger.warning(f'  基金 {code} 计算失败: {e}')
            results.append({
                '基金代码': code,
                '近3年最大回撤': np.nan,
                '年化收益率': np.nan,
                '年化波动率': np.nan,
                '夏普比率': np.nan,
                '熊市数': 0,
                '年度收益': [],
            })

    metrics_df = pd.DataFrame(results)
    candidate_df = candidate_df.merge(metrics_df, on='基金代码', how='left')

    # 计算业绩排名分位
    logger.info('计算业绩排名分位...')
    candidate_df['业绩排名分位'] = candidate_df['年度收益'].apply(
        lambda x: metrics.calc_performance_rank_percentile(x, annual_returns_pool) if x else 100
    )
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

    # 3. 费率: 默认行业均值
    candidate_df['综合费率'] = 1.75

    # 4. 机构持有比例: 默认值
    candidate_df['机构持有比例'] = 30

    return candidate_df


def apply_hard_filter(df):
    """第五步: 应用 6 项硬性筛选"""
    logger.info('=' * 60)
    logger.info('Step 5: 应用硬性筛选')
    logger.info('=' * 60)

    n0 = len(df)
    df = df.copy()
    df['硬筛通过'] = True
    df['淘汰原因'] = ''

    hf = HARD_FILTER

    # 1. 经理任职年限
    mask = df['经理任职年限'].notna() & (df['经理任职年限'] < hf['min_manager_years'])
    df.loc[mask, '硬筛通过'] = False
    df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + f'经理任职<{hf["min_manager_years"]}年;'

    # 2. 基金年龄
    mask = df['基金年龄'].notna() & (df['基金年龄'] < hf['min_fund_age'])
    df.loc[mask, '硬筛通过'] = False
    df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + f'基金成立<{hf["min_fund_age"]}年;'

    # 3. 规模
    mask = df['基金规模'].notna() & ((df['基金规模'] < hf['min_scale']) | (df['基金规模'] > hf['max_scale']))
    df.loc[mask, '硬筛通过'] = False
    df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + f'规模不在{hf["min_scale"]}-{hf["max_scale"]}亿;'

    # 4. 经理在管基金数
    if '经理在管基金数' in df.columns:
        count_num = pd.to_numeric(df['经理在管基金数'], errors='coerce')
        mask = count_num.notna() & (count_num > hf['max_funds_per_manager'])
        df.loc[mask, '硬筛通过'] = False
        df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + f'在管>{hf["max_funds_per_manager"]}只;'

    # 5. 回撤(对比同类中位数)
    median_dd = df['近3年最大回撤'].median()
    if pd.notna(median_dd):
        mask = df['近3年最大回撤'].notna() & (df['近3年最大回撤'] > median_dd * hf['max_drawdown_ratio'])
        df.loc[mask, '硬筛通过'] = False
        df.loc[mask, '淘汰原因'] = df.loc[mask, '淘汰原因'] + f'回撤>同类中位数*{hf["max_drawdown_ratio"]};'

    # 6. 机构持有比例(默认值时跳过该项,以免误杀)
    # 此处由于真实数据获取成本高,默认填的 30,所以这条筛选实际放行所有

    passed = df['硬筛通过'].sum()
    logger.info(f'硬筛结果: {n0} 只 → {passed} 只通过')
    return df


def calc_soft_score(df):
    """第六步: 软性评分"""
    logger.info('=' * 60)
    logger.info('Step 6: 计算软性评分')
    logger.info('=' * 60)

    df = df.copy()

    def score_stability(percentile):
        if pd.isna(percentile):
            return 0
        if percentile <= 25:
            return 100
        elif percentile <= 50:
            return 70
        elif percentile <= 70:
            return 40
        else:
            return 15

    def score_style(vol):
        # 用波动率作为风格稳定性代理(波动小说明持仓稳定)
        # 优秀: <20, 一般: 20-25, 较差: >25
        if pd.isna(vol):
            return 50
        if vol < 18:
            return 90
        elif vol < 22:
            return 75
        elif vol < 26:
            return 60
        else:
            return 40

    def score_framework(sharpe):
        # 用夏普比率作为投资框架质量代理
        if pd.isna(sharpe):
            return 50
        if sharpe >= 1.0:
            return 95
        elif sharpe >= 0.7:
            return 80
        elif sharpe >= 0.4:
            return 65
        elif sharpe >= 0:
            return 50
        else:
            return 30

    def score_fee(fee):
        if pd.isna(fee):
            return 60
        if fee <= 1.0:
            return 100
        elif fee <= 1.5:
            return 80
        elif fee <= 2.0:
            return 60
        else:
            return 30

    def score_bear(count):
        return min(max(count, 0), 3) / 3 * 100

    def score_scale(scale):
        if pd.isna(scale):
            return 50
        if 5 <= scale <= 30:
            return 100
        elif 2 <= scale <= 50:
            return 80
        elif scale <= 100:
            return 60
        else:
            return 0

    def score_tenure(years):
        if pd.isna(years):
            return 50
        if years >= 10:
            return 100
        elif years >= 7:
            return 85
        elif years >= 5:
            return 70
        else:
            return 50

    df['得分_稳定性'] = df['业绩排名分位'].apply(score_stability)
    df['得分_风格'] = df['年化波动率'].apply(score_style)
    df['得分_框架'] = df['夏普比率'].apply(score_framework)
    df['得分_费率'] = df['综合费率'].apply(score_fee)
    df['得分_熊市'] = df['熊市数'].apply(score_bear)
    df['得分_规模'] = df['基金规模'].apply(score_scale)
    df['得分_任职'] = df['经理任职年限'].apply(score_tenure)

    w = SCORE_WEIGHTS
    df['综合得分'] = (
        df['得分_稳定性'] * w['stability']
        + df['得分_风格'] * w['style']
        + df['得分_框架'] * w['framework']
        + df['得分_费率'] * w['fee']
        + df['得分_熊市'] * w['bear_market']
        + df['得分_规模'] * w['scale']
        + df['得分_任职'] * w['tenure']
    )

    # 评级
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
    candidate = calc_soft_score(candidate)

    # 排序
    passed = candidate[candidate['硬筛通过']].copy()
    passed = passed.sort_values('综合得分', ascending=False)
    top_n = passed.head(TOP_N)

    logger.info(f'\n最终结果: Top {len(top_n)} 基金')
    if len(top_n) > 0:
        for i, (_, row) in enumerate(top_n.iterrows(), 1):
            logger.info(f'  {i:2d}. {row["基金代码"]} {row["基金简称"]:30s} 得分={row["综合得分"]:.1f} 评级={row["评级"]}')

    return top_n, candidate

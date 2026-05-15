"""
数据获取模块 - 基于 AKShare
关键设计:
1. 使用缓存避免重复请求
2. 失败重试机制
3. 限速避免触发反爬
"""
import os
import time
import pickle
import logging
from datetime import datetime, timedelta
from functools import wraps

import akshare as ak
import pandas as pd

from .config import CACHE_DIR, CACHE_DAYS_RANK, CACHE_DAYS_NAV, PERF_CONFIG

logger = logging.getLogger(__name__)


def _cache_path(filename):
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, filename)


def _is_cache_valid(path, days):
    if not os.path.exists(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mtime < timedelta(days=days)


def cache_result(filename, days=7):
    """缓存装饰器:把函数返回值序列化到 pkl"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            path = _cache_path(filename)
            if _is_cache_valid(path, days):
                try:
                    with open(path, 'rb') as f:
                        logger.info(f'[Cache HIT] {filename}')
                        return pickle.load(f)
                except Exception as e:
                    logger.warning(f'读取缓存失败 {filename}: {e}')

            result = func(*args, **kwargs)
            try:
                with open(path, 'wb') as f:
                    pickle.dump(result, f)
            except Exception as e:
                logger.warning(f'写入缓存失败 {filename}: {e}')
            return result
        return wrapper
    return decorator


def retry(max_retries=None, delay=None):
    """失败重试装饰器"""
    if max_retries is None:
        max_retries = PERF_CONFIG['max_retries']
    if delay is None:
        delay = PERF_CONFIG['retry_delay']

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if i < max_retries - 1:
                        time.sleep(delay)
            logger.error(f'{func.__name__} 重试 {max_retries} 次仍失败: {last_exc}')
            return None
        return wrapper
    return decorator


# ============================================================================
# 基础数据获取接口
# ============================================================================

@cache_result('fund_rank_stock.pkl', days=CACHE_DAYS_RANK)
def fetch_fund_rank_stock():
    """主动股票型基金排名"""
    logger.info('拉取主动股票型基金排名...')
    df = ak.fund_open_fund_rank_em(symbol="股票型")
    return df


@cache_result('fund_rank_mixed.pkl', days=CACHE_DAYS_RANK)
def fetch_fund_rank_mixed():
    """混合型基金排名"""
    logger.info('拉取混合型基金排名...')
    df = ak.fund_open_fund_rank_em(symbol="混合型")
    return df


@cache_result('fund_rank_qdii.pkl', days=CACHE_DAYS_RANK)
def fetch_fund_rank_qdii():
    """QDII基金排名"""
    logger.info('拉取QDII基金排名...')
    df = ak.fund_open_fund_rank_em(symbol="QDII")
    return df


@cache_result('fund_basics.pkl', days=CACHE_DAYS_RANK)
def fetch_all_fund_basics():
    """所有基金的基本信息(代码、名称、类型)"""
    logger.info('拉取所有基金基本信息...')
    df = ak.fund_name_em()
    return df


@retry()
def fetch_fund_nav(code):
    """单只基金的历史净值"""
    time.sleep(PERF_CONFIG['request_delay'])
    df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
    return df


@retry()
def fetch_fund_basic_info(code):
    """单只基金的详细基本信息(规模、成立时间、费率等,雪球数据)"""
    time.sleep(PERF_CONFIG['request_delay'])
    df = ak.fund_individual_basic_info_xq(symbol=code)
    return df


@cache_result('fund_managers.pkl', days=CACHE_DAYS_RANK)
def fetch_all_managers():
    """全市场基金经理信息(包含任职年限、在管基金数)"""
    logger.info('拉取所有基金经理信息...')
    df = ak.fund_manager_em()
    return df


@retry()
def fetch_fund_portfolio(code, year=None):
    """基金前十大持仓(可计算行业集中度)"""
    if year is None:
        year = datetime.now().year
    time.sleep(PERF_CONFIG['request_delay'])
    df = ak.fund_portfolio_hold_em(symbol=code, date=str(year))
    return df


@retry()
def fetch_fund_industry_allocation(code, year=None):
    """基金行业配置(指定年份)"""
    if year is None:
        year = datetime.now().year
    time.sleep(PERF_CONFIG['request_delay'])
    df = ak.fund_portfolio_industry_allocation_em(symbol=code, date=str(year))
    return df


def fetch_recent_industry_allocations(code, years=2):
    """
    获取近 N 年的行业配置, 返回 list of dict [{industry: weight, ...}, ...] (老→新)
    """
    current_year = datetime.now().year
    out = []
    for y in range(current_year - years, current_year + 1):
        try:
            df = fetch_fund_industry_allocation(code, year=y)
            if df is None or len(df) == 0:
                continue
            name_col = next((c for c in df.columns if '行业' in c and ('类别' in c or '名称' in c)), None)
            weight_col = next((c for c in df.columns if '占净值' in c or '占股票' in c), None)
            if name_col is None or weight_col is None:
                continue
            if '截止日期' in df.columns or '公告日期' in df.columns:
                date_col = '截止日期' if '截止日期' in df.columns else '公告日期'
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                latest = df[date_col].max()
                df = df[df[date_col] == latest]
            alloc = {}
            for _, row in df.iterrows():
                ind = str(row[name_col]).strip()
                try:
                    w = float(row[weight_col])
                except (ValueError, TypeError):
                    continue
                if ind and ind != 'nan':
                    alloc[ind] = alloc.get(ind, 0.0) + w
            if alloc:
                out.append(alloc)
        except Exception as e:
            logger.debug(f'  基金 {code} 行业配置 {y} 获取失败: {e}')
    return out

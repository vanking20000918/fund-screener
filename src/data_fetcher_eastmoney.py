"""
数据获取模块 - 直接爬取天天基金网
不依赖 AKShare,直接调用天天基金网 API
"""
import os
import re
import json
import time
import pickle
import logging
from datetime import datetime, timedelta
from functools import wraps

import requests
import pandas as pd

from .config import CACHE_DIR, CACHE_DAYS_RANK, CACHE_DAYS_NAV, PERF_CONFIG

logger = logging.getLogger(__name__)

# 通用请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://fund.eastmoney.com/',
}

NAV_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://fundf10.eastmoney.com/',
}


# ============================================================================
# 缓存与重试(复用逻辑)
# ============================================================================

def _cache_path(filename):
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, filename)


def _is_cache_valid(path, days):
    if not os.path.exists(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mtime < timedelta(days=days)


def cache_result(filename, days=7):
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
                        logger.warning(f'{func.__name__} 第{i+1}次失败: {e}, 重试中...')
                        time.sleep(delay)
            logger.error(f'{func.__name__} 重试 {max_retries} 次仍失败: {last_exc}')
            return None
        return wrapper
    return decorator


# ============================================================================
# 基金排名数据
# ============================================================================

def _fetch_fund_rank_raw(ft, pn=10000):
    """
    从天天基金网排名接口获取数据
    ft: gp=股票型, hh=混合型, zq=债券型, zs=指数型
    pn: 每页数量
    返回 DataFrame
    """
    url = 'https://fund.eastmoney.com/data/rankhandler.aspx'
    params = {
        'op': 'ph',
        'dt': 'kf',       # 开放基金
        'ft': ft,
        'rs': '',
        'gs': 0,
        'sc': '3nzf',     # 按近3年排序
        'st': 'desc',
        'pi': 1,
        'pn': pn,
        'dx': 1,
    }
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    text = resp.text

    # 响应格式: var rankData = {datas:["...","..."],allRecords:1234,...}
    match = re.search(r'datas:\[(.*?)\]', text, re.DOTALL)
    if not match:
        logger.error(f'排名接口解析失败, ft={ft}')
        return pd.DataFrame()

    raw_items = re.findall(r'"([^"]*)"', match.group(1))
    if not raw_items:
        return pd.DataFrame()

    # 解析每条记录 (逗号分隔)
    # 实测字段顺序:
    # 0:基金代码, 1:基金简称, 2:拼音缩写, 3:日期, 4:单位净值, 5:累计净值,
    # 6:日增长率, 7:近1周, 8:近1月, 9:近3月, 10:近6月, 11:近1年,
    # 12:近2年, 13:近3年, 14:今年来, 15:成立来, 16:成立日期,
    # 17:?, 18:自定义, 19:管理费率, 20:手续费, 21-23:?, 24:规模?
    rows = []
    for item in raw_items:
        fields = item.split(',')
        if len(fields) < 16:
            continue
        rows.append({
            '基金代码': fields[0],
            '基金简称': fields[1],
            '日期': fields[3],
            '单位净值': _safe_float(fields[4]),
            '累计净值': _safe_float(fields[5]),
            '日增长率': _safe_float(fields[6]),
            '近1周': _safe_float(fields[7]),
            '近1月': _safe_float(fields[8]),
            '近3月': _safe_float(fields[9]),
            '近6月': _safe_float(fields[10]),
            '近1年': _safe_float(fields[11]),
            '近2年': _safe_float(fields[12]),
            '近3年': _safe_float(fields[13]),
            '今年来': _safe_float(fields[14]),
            '成立来': _safe_float(fields[15]),
            '成立日期': fields[16] if len(fields) > 16 else '',
            '手续费': fields[20] if len(fields) > 20 else '',
        })

    df = pd.DataFrame(rows)
    logger.info(f'天天基金排名数据: ft={ft}, 获取 {len(df)} 只')
    return df


def _safe_float(val):
    try:
        return float(val) if val and val.strip() else None
    except (ValueError, TypeError):
        return None


@cache_result('em_fund_rank_stock.pkl', days=CACHE_DAYS_RANK)
def fetch_fund_rank_stock():
    """主动股票型基金排名"""
    logger.info('[天天基金] 拉取主动股票型基金排名...')
    return _fetch_fund_rank_raw('gp')


@cache_result('em_fund_rank_mixed.pkl', days=CACHE_DAYS_RANK)
def fetch_fund_rank_mixed():
    """混合型基金排名"""
    logger.info('[天天基金] 拉取混合型基金排名...')
    return _fetch_fund_rank_raw('hh')


@cache_result('em_fund_rank_qdii.pkl', days=CACHE_DAYS_RANK)
def fetch_fund_rank_qdii():
    """QDII基金排名"""
    logger.info('[天天基金] 拉取QDII基金排名...')
    return _fetch_fund_rank_raw('qdii')


# ============================================================================
# 基金净值数据
# ============================================================================

@retry()
def fetch_fund_nav(code):
    """
    单只基金的全部历史净值
    使用 pingzhongdata 接口,一次请求返回全部净值数据
    """
    time.sleep(PERF_CONFIG['request_delay'])

    url = f'https://fund.eastmoney.com/pingzhongdata/{code}.js'
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    text = resp.text

    # 提取 Data_netWorthTrend 变量 (单位净值走势)
    # 格式: [{x: timestamp_ms, y: nav, equityReturn: pct, unitMoney: ""}, ...]
    m = re.search(r'var Data_netWorthTrend = (\[.*?\]);', text, re.DOTALL)
    if not m:
        return None

    data = json.loads(m.group(1))
    if not data:
        return None

    rows = []
    for item in data:
        ts = item.get('x', 0)
        nav = item.get('y', None)
        if ts and nav is not None:
            date = datetime.fromtimestamp(ts / 1000)
            rows.append({
                '净值日期': date,
                '单位净值': float(nav),
            })

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df = df.sort_values('净值日期').reset_index(drop=True)
    return df


# ============================================================================
# 基金详情(经理、规模、成立日期)
# ============================================================================

@retry()
def fetch_fund_detail(code):
    """
    从基金经理页面获取: 当前经理姓名、任职起始日、任职天数
    从基本概况页面获取: 基金规模、成立日期
    """
    time.sleep(PERF_CONFIG['request_delay'])
    info = {'基金代码': code}

    # 1. 基金经理页: jjjl_CODE.html
    try:
        url = f'https://fundf10.eastmoney.com/jjjl_{code}.html'
        resp = requests.get(url, headers=NAV_HEADERS, timeout=30)
        resp.encoding = 'utf-8'
        html = resp.text

        # 从历任经理表格提取当前经理(截止期="至今"的那行)
        # 格式: 起始期 | 截止期 | 基金经理 | 任职期间 | 任职回报
        current = re.findall(
            r'(\d{4}-\d{2}-\d{2})\s*.*?至今\s*.*?'
            r'<a[^>]*>([^<]+)</a>.*?'
            r'(\d+年又\d+天|\d+天)',
            html, re.DOTALL
        )
        if current:
            info['经理任职起始'] = current[0][0]
            info['基金经理'] = current[0][1].strip()
            info['任职期间'] = current[0][2]
        else:
            # 备用: 尝试更宽松的匹配
            m = re.search(r'(\d{4}-\d{2}-\d{2})\s*</td>\s*<td[^>]*>\s*至今', html)
            if m:
                info['经理任职起始'] = m.group(1)
            names = re.findall(r'<a href="/manager/\w+\.html">([^<]+)</a>', html)
            if names:
                info['基金经理'] = names[0].strip()
    except Exception as e:
        logger.debug(f'基金 {code} 经理信息获取失败: {e}')

    # 2. 基本概况页: jbgk_CODE.html (规模和成立日期)
    try:
        time.sleep(PERF_CONFIG['request_delay'] * 0.5)
        url2 = f'https://fundf10.eastmoney.com/jbgk_{code}.html'
        resp2 = requests.get(url2, headers=NAV_HEADERS, timeout=30)
        resp2.encoding = 'utf-8'
        html2 = resp2.text

        m = re.search(r'资产规模.*?([\d.]+)\s*亿元', html2, re.DOTALL)
        if m:
            info['基金规模'] = float(m.group(1))

        m = re.search(r'成立日期/规模.*?(\d{4}年\d{2}月\d{2}日)', html2, re.DOTALL)
        if not m:
            m = re.search(r'成立日期.*?(\d{4}[年\-]\d{2}[月\-]\d{2})', html2, re.DOTALL)
        if m:
            d = m.group(1).replace('年', '-').replace('月', '-').replace('日', '')
            info['成立日期'] = d
    except Exception as e:
        logger.debug(f'基金 {code} 基本信息获取失败: {e}')

    return info


def fetch_fund_details_batch(codes):
    """批量获取基金详情(经理、规模、成立日期)"""
    logger.info(f'[天天基金] 批量获取基金详情, 共 {len(codes)} 只...')
    results = []
    for i, code in enumerate(codes):
        detail = fetch_fund_detail(code)
        if detail:
            results.append(detail)
        if (i + 1) % 30 == 0:
            logger.info(f'  详情进度: {i+1}/{len(codes)}')
    return pd.DataFrame(results) if results else pd.DataFrame()


# ============================================================================
# 行业配置(用于"风格一致性"评分维度)
# ============================================================================

# 不使用 @retry: 行业数据可缺失,失败时直接 NaN,由软评分波动率代理兜底
# 重试 3x2s 在 300 只基金上会导致 30+ 分钟超时
def fetch_recent_industry_allocations(code, years=2):
    """
    从天天基金 API 获取近 N 年的行业配置历史
    返回 list of dict [{industry: weight, ...}, ...] (按期排序, 老→新)
    数据缺失或解析失败返回 [] (不重试, 失败快速跳过)
    """
    time.sleep(PERF_CONFIG['request_delay'])

    url = 'https://api.fund.eastmoney.com/f10/HYPZ/'
    params = {
        'fundCode': code,
        'OSVersion': '14.3',
        'deviceid': 'Wap',
    }
    headers = {
        'User-Agent': HEADERS['User-Agent'],
        'Referer': f'https://fundf10.eastmoney.com/hytz_{code}.html',
        'Accept': '*/*',
    }

    try:
        # 短超时:行业数据非关键,失败立刻跳过
        resp = requests.get(url, params=params, headers=headers, timeout=8)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.debug(f'  基金 {code} 行业配置 API 失败: {e}')
        return []

    if not isinstance(data, dict):
        return []
    if data.get('ErrCode') not in (0, None, '0'):
        return []

    detail = (data.get('Data') or {}).get('HYPZDetail') or []
    if not detail:
        return []

    # 取最近 years*2 期 (每年大约 2 期: 中报+年报)
    max_periods = max(2, years * 2)
    out = []
    for period in detail[:max_periods]:
        industries = (period.get('FundIndustry')
                      or period.get('Industries')
                      or period.get('HYPZ')
                      or [])
        if not industries:
            continue

        alloc = {}
        for item in industries:
            if not isinstance(item, dict):
                continue
            name = (item.get('HYMC') or item.get('Industry') or item.get('HYDM') or '').strip()
            weight_raw = (item.get('ZJZBL') or item.get('JZBL')
                          or item.get('Weight') or item.get('NetValueRatio') or '0')
            try:
                w = float(str(weight_raw).replace('%', '').strip()) if weight_raw else 0
            except (ValueError, TypeError):
                continue
            if name and w > 0:
                alloc[name] = alloc.get(name, 0.0) + w
        if alloc:
            out.append(alloc)

    out.reverse()  # API 返回新→老, 反转为 老→新
    return out

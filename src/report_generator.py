"""
报告生成模块
1. HTML 邮件正文(Top N 推荐列表+评分+各指标明细)
2. Excel 附件(完整候选池详细数据)
"""
import os
import logging
from datetime import datetime

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import ColorScaleRule

from .config import OUTPUT_DIR, TOP_N, SCORE_WEIGHTS

logger = logging.getLogger(__name__)


# ============================================================================
# HTML 邮件正文
# ============================================================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
    body {{ font-family: -apple-system, "Microsoft YaHei", Arial; color: #333; line-height: 1.6; max-width: 900px; margin: 0 auto; padding: 20px; }}
    h1 {{ color: #1f4e78; border-bottom: 3px solid #2e75b6; padding-bottom: 10px; }}
    h2 {{ color: #1f4e78; margin-top: 30px; }}
    .summary-box {{ background: #e2efda; border-left: 4px solid #00b050; padding: 15px; margin: 20px 0; border-radius: 4px; }}
    .warning-box {{ background: #fff2cc; border-left: 4px solid #ffc000; padding: 12px; margin: 15px 0; border-radius: 4px; font-size: 13px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 15px 0; font-size: 13px; }}
    th {{ background: #2e75b6; color: white; padding: 10px 8px; text-align: center; }}
    td {{ padding: 8px; border-bottom: 1px solid #d9d9d9; text-align: center; }}
    tr:nth-child(even) {{ background: #f8f9fa; }}
    .rank {{ font-weight: bold; color: #1f4e78; width: 40px; }}
    .grade-A {{ background: #00b050; color: white; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
    .grade-B {{ background: #92d050; color: white; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
    .grade-C {{ background: #ffc000; color: white; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
    .grade-D {{ background: #c00000; color: white; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
    .score {{ font-weight: bold; color: #1f4e78; }}
    .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #d9d9d9; font-size: 12px; color: #888; }}
    .name {{ text-align: left; }}
    .small {{ font-size: 11px; color: #888; }}
</style>
</head>
<body>
    <h1>📊 主动基金月度筛选报告</h1>
    <p>报告日期: <strong>{report_date}</strong></p>

    <div class="summary-box">
        <strong>📌 本期摘要</strong><br>
        • 候选池规模: <strong>{candidate_count}</strong> 只(股票型 + 混合型 + QDII)<br>
        • 通过硬性筛选: <strong>{passed_count}</strong> 只<br>
        • 推荐 Top {top_n} 平均得分: <strong>{avg_score:.1f}</strong><br>
        • A 级数量: {a_count}; B 级数量: {b_count}
    </div>

    <h2>🏆 Top {top_n} 推荐列表</h2>
    <table>
        <thead>
            <tr>
                <th>排名</th>
                <th>基金代码</th>
                <th>基金简称</th>
                <th>基金经理</th>
                <th>综合得分</th>
                <th>评级</th>
                <th>入选关键原因 (维度×权重)</th>
            </tr>
        </thead>
        <tbody>
            {top_rows}
        </tbody>
    </table>

    <h2>📈 各项指标明细</h2>
    <table>
        <thead>
            <tr>
                <th>代码</th>
                <th>规模(亿)</th>
                <th>经理<br>任职(年)</th>
                <th>年化<br>收益(%)</th>
                <th>近3年<br>回撤(%)</th>
                <th>卡玛<br>比率</th>
                <th>波动率<br>(%)</th>
                <th>排名<br>分位</th>
                <th>熊市平均<br>回撤(%)</th>
                <th>行业<br>稳定性</th>
            </tr>
        </thead>
        <tbody>
            {detail_rows}
        </tbody>
    </table>

    <h2>🔍 各维度评分明细</h2>
    <table>
        <thead>
            <tr>
                <th>代码</th>
                <th>稳定性<br>(28%)</th>
                <th>熊市表现<br>(20%)</th>
                <th>任职<br>(15%)</th>
                <th>框架(卡玛)<br>(15%)</th>
                <th>风格(行业)<br>(12%)</th>
                <th>规模<br>(10%)</th>
                <th>综合</th>
            </tr>
        </thead>
        <tbody>
            {score_rows}
        </tbody>
    </table>

    {backtest_section}

    <div class="warning-box">
        <strong>⚠️ 重要提醒</strong><br>
        • 本报告基于历史数据回测,<strong>过去业绩不代表未来表现</strong>。<br>
        • 业绩排名分位基于<strong>全市场近3年收益</strong>计算;熊市表现基于历轮熊市回撤分位。<br>
        • 行业稳定性需 akshare 数据源支持,eastmoney 模式下回退到波动率代理。<br>
        • 建议结合人工核查后再做投资决策,主动基金合理仓位不超过权益部分30%。
    </div>

    <div class="footer">
        本报告由自动化脚本生成 · 数据来源: 天天基金网 / AKShare · 评分卡 v2.0<br>
        如需调整筛选参数,请修改 src/config.py
    </div>
</body>
</html>
"""


def _grade_class(grade):
    if 'A' in grade:
        return 'A'
    if 'B' in grade:
        return 'B'
    if 'C' in grade:
        return 'C'
    if 'D' in grade:
        return 'D'
    return ''


def _fmt(val, fmt='{:.2f}', default='—'):
    """格式化数值,NaN 返回 default"""
    if val is None or pd.isna(val):
        return default
    try:
        return fmt.format(val)
    except (ValueError, TypeError):
        return str(val)


def _fmt_pct(v):
    """带正负号的百分点格式化, 用于超额收益等带符号场景"""
    if v is None or pd.isna(v):
        return '—'
    return f'{v:+.2f}'


def _build_backtest_html(backtest):
    """构造 HTML 邮件里的回测验证区块. backtest 为 None 时返回空串"""
    if not backtest or 'summary' not in backtest:
        return ''
    s = backtest['summary']

    def _v(key, fmt='{:.2f}'):
        val = s.get(key)
        if val is None or pd.isna(val):
            return '—'
        try:
            return fmt.format(val)
        except (ValueError, TypeError):
            return str(val)

    return f"""
    <h2>🔬 评分体系回测验证</h2>
    <p class="small">PIT 回测: 用 <strong>{s.get('as_of', '—')}</strong> 时点的评分体系挑 Top {s.get('top_n_size', '—')},
    持有至 <strong>{s.get('hold_end', '—')}</strong> ({s.get('hold_days', '—')} 天)。
    宇宙规模 {s.get('pool_universe_size', '—')} 只,硬筛通过 {s.get('pool_passed_hard_filter', '—')} 只。</p>
    <table>
        <thead>
            <tr>
                <th>口径</th>
                <th>Top {s.get('top_n_size', 'N')}</th>
                <th>通过硬筛池子</th>
                <th>整宇宙</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>平均持有期收益(%)</td>
                <td class="score">{_v('top_n_avg_return_pct')}</td>
                <td>{_v('pool_passed_avg_return_pct')}</td>
                <td>{_v('universe_avg_return_pct')}</td>
            </tr>
            <tr>
                <td>中位数(%)</td>
                <td>{_v('top_n_median_return_pct')}</td>
                <td>{_v('pool_passed_median_return_pct')}</td>
                <td>{_v('universe_median_return_pct')}</td>
            </tr>
            <tr>
                <td>Top 超额(pct)</td>
                <td colspan="2">vs 池子: <strong>{_fmt_pct(s.get('excess_top_vs_pool_avg'))}</strong></td>
                <td>vs 宇宙: <strong>{_fmt_pct(s.get('excess_top_vs_universe_avg'))}</strong></td>
            </tr>
            <tr>
                <td>Top 胜率 vs 中位数(%)</td>
                <td colspan="2">vs 池子: {_v('win_rate_vs_pool_median_pct', '{:.1f}')}</td>
                <td>vs 宇宙: {_v('win_rate_vs_universe_median_pct', '{:.1f}')}</td>
            </tr>
        </tbody>
    </table>
    <p class="small">说明: 回测使用历史时点的净值数据 (PIT) 重跑评分,持有期收益反映"若当时按本评分体系选 Top N,持有至今的实际表现"。
    宇宙存活者偏差不可避免;规模/在管基金数用当前值代理,有已知偏差。</p>
    """


def _fmt_bear_dd(row):
    """熊市平均回撤显示: NaN 且熊市数=0 时显示'未经历熊市',否则显示'—'"""
    dd = row.get('熊市平均回撤')
    if pd.notna(dd):
        return f'{dd:.1f}'
    bc = row.get('熊市数', 0)
    if pd.isna(bc):
        bc = 0
    return '未经历熊市' if int(bc) == 0 else '—'


def generate_html_report(top_n_df, all_df, backtest=None):
    """生成 HTML 邮件正文. backtest: src.backtest.run_backtest 返回值, 可选"""
    if len(top_n_df) == 0:
        return _generate_empty_report(all_df)

    candidate_count = len(all_df)
    passed_count = int(all_df['硬筛通过'].sum()) if '硬筛通过' in all_df.columns else 0
    avg_score = top_n_df['综合得分'].mean()
    a_count = int((top_n_df['评级'] == 'A 级').sum())
    b_count = int((top_n_df['评级'] == 'B 级').sum())

    # Top 排名表
    top_rows = []
    for i, (_, row) in enumerate(top_n_df.iterrows(), 1):
        gc = _grade_class(row['评级'])
        manager = row.get('基金经理', '')
        if pd.isna(manager):
            manager = ''
        reason = row.get('入选原因', '') or ''
        top_rows.append(f"""
            <tr>
                <td class="rank">{i}</td>
                <td>{row['基金代码']}</td>
                <td class="name">{row['基金简称']}</td>
                <td>{manager}</td>
                <td class="score">{row['综合得分']:.1f}</td>
                <td><span class="grade-{gc}">{row['评级']}</span></td>
                <td class="small name">{reason}</td>
            </tr>
        """)

    # 指标明细表
    detail_rows = []
    for _, row in top_n_df.iterrows():
        detail_rows.append(f"""
            <tr>
                <td>{row['基金代码']}</td>
                <td>{_fmt(row.get('基金规模'), '{:.1f}')}</td>
                <td>{_fmt(row.get('经理任职年限'), '{:.1f}')}</td>
                <td>{_fmt(row.get('年化收益率'), '{:.2f}')}</td>
                <td>{_fmt(row.get('近3年最大回撤'), '{:.2f}')}</td>
                <td>{_fmt(row.get('卡玛比率'), '{:.2f}')}</td>
                <td>{_fmt(row.get('年化波动率'), '{:.1f}')}</td>
                <td>{_fmt(row.get('业绩排名分位'), '{:.0f}')}</td>
                <td>{_fmt_bear_dd(row)}</td>
                <td>{_fmt(row.get('行业稳定性'), '{:.2f}')}</td>
            </tr>
        """)

    # 评分明细表
    score_rows = []
    for _, row in top_n_df.iterrows():
        score_rows.append(f"""
            <tr>
                <td>{row['基金代码']}</td>
                <td>{_fmt(row.get('得分_稳定性'), '{:.0f}')}</td>
                <td>{_fmt(row.get('得分_熊市'), '{:.0f}')}</td>
                <td>{_fmt(row.get('得分_任职'), '{:.0f}')}</td>
                <td>{_fmt(row.get('得分_框架'), '{:.0f}')}</td>
                <td>{_fmt(row.get('得分_风格'), '{:.0f}')}</td>
                <td>{_fmt(row.get('得分_规模'), '{:.0f}')}</td>
                <td class="score">{row['综合得分']:.1f}</td>
            </tr>
        """)

    html = HTML_TEMPLATE.format(
        report_date=datetime.now().strftime('%Y-%m-%d'),
        candidate_count=candidate_count,
        passed_count=passed_count,
        top_n=len(top_n_df),
        avg_score=avg_score,
        a_count=a_count,
        b_count=b_count,
        top_rows=''.join(top_rows),
        detail_rows=''.join(detail_rows),
        score_rows=''.join(score_rows),
        backtest_section=_build_backtest_html(backtest),
    )
    return html


def _generate_empty_report(all_df):
    """没有基金通过筛选时的报告"""
    return f"""
    <html><body style="font-family: 'Microsoft YaHei'; padding: 20px;">
        <h2>📊 主动基金月度筛选报告</h2>
        <p>报告日期: {datetime.now().strftime('%Y-%m-%d')}</p>
        <div style="background: #ffe4e1; padding: 15px; border-left: 4px solid #c00;">
            <strong>⚠️ 本期未有基金通过筛选</strong><br>
            候选池规模: {len(all_df)}<br>
            建议检查数据源是否正常,或调整 config.py 的筛选阈值。
        </div>
    </body></html>
    """


# ============================================================================
# Excel 详细报告
# ============================================================================
def generate_excel_report(top_n_df, all_df, output_path, backtest=None):
    """生成 Excel 详细附件. backtest: src.backtest.run_backtest 返回值, 可选,
    存在时新增 "回测验证" sheet"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    wb = Workbook()
    ws_top = wb.active
    ws_top.title = 'Top推荐'

    # 字体样式
    F_TITLE = Font(name='微软雅黑', size=14, bold=True, color='FFFFFF')
    F_HEADER = Font(name='微软雅黑', size=11, bold=True, color='FFFFFF')
    F_BODY = Font(name='微软雅黑', size=10)
    FILL_TITLE = PatternFill('solid', start_color='1F4E78')
    FILL_HEADER = PatternFill('solid', start_color='2E75B6')
    ALIGN_C = Alignment(horizontal='center', vertical='center', wrap_text=True)
    ALIGN_L = Alignment(horizontal='left', vertical='center', wrap_text=True)
    thin = Side(border_style='thin', color='BFBFBF')
    BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)

    # ========== Sheet 1: Top 推荐 ==========
    top_cols = [
        '排名', '基金代码', '基金简称', '基金类型', '基金经理',
        '基金规模', '经理任职年限', '年化收益率', '近3年最大回撤',
        '卡玛比率', '夏普比率', '年化波动率', '业绩排名分位',
        '熊市数', '熊市平均回撤', '行业稳定性',
        '得分_稳定性', '得分_熊市', '得分_任职',
        '得分_框架', '得分_风格', '得分_规模',
        '综合得分', '评级', '入选原因',
    ]

    last_col_letter = get_column_letter(len(top_cols))
    ws_top.merge_cells(f'A1:{last_col_letter}1')
    ws_top['A1'] = f"主动基金月度筛选报告 · Top {len(top_n_df)} 推荐"
    ws_top['A1'].font = F_TITLE
    ws_top['A1'].fill = FILL_TITLE
    ws_top['A1'].alignment = ALIGN_C
    ws_top.row_dimensions[1].height = 28

    ws_top.merge_cells(f'A2:{last_col_letter}2')
    ws_top['A2'] = f"报告日期: {datetime.now().strftime('%Y-%m-%d')}    评分卡 v2.0"
    ws_top['A2'].font = Font(name='微软雅黑', size=10, italic=True, color='595959')
    ws_top['A2'].alignment = ALIGN_L

    # 表头
    for j, col in enumerate(top_cols, 1):
        c = ws_top.cell(row=4, column=j, value=col)
        c.font = F_HEADER
        c.fill = FILL_HEADER
        c.alignment = ALIGN_C
        c.border = BORDER

    # 数据行
    for i, (_, row) in enumerate(top_n_df.iterrows(), start=5):
        ws_top.cell(row=i, column=1, value=i - 4)
        for j, col in enumerate(top_cols[1:], 2):
            val = row.get(col, '')
            # 熊市平均回撤 NaN 时给出温柔提示
            if col == '熊市平均回撤' and pd.isna(val):
                bc = row.get('熊市数', 0)
                if pd.isna(bc):
                    bc = 0
                val = '未经历熊市' if int(bc) == 0 else ''
            elif pd.isna(val):
                val = ''
            cell = ws_top.cell(row=i, column=j, value=val)
            cell.font = F_BODY
            cell.alignment = ALIGN_C
            cell.border = BORDER
            # 数值格式
            if col in ('基金规模', '经理任职年限', '年化波动率', '熊市平均回撤'):
                cell.number_format = '0.00'
            elif col in ('年化收益率', '近3年最大回撤'):
                cell.number_format = '0.00'
            elif col in ('夏普比率', '卡玛比率', '行业稳定性'):
                cell.number_format = '0.00'
            elif col == '综合得分':
                cell.number_format = '0.0'
            elif col.startswith('得分_'):
                cell.number_format = '0'

    # 列宽
    widths = {'A': 6, 'B': 10, 'C': 25, 'D': 12, 'E': 12}
    for col, w in widths.items():
        ws_top.column_dimensions[col].width = w
    for j in range(6, len(top_cols) + 1):
        ws_top.column_dimensions[get_column_letter(j)].width = 11

    # 冻结
    ws_top.freeze_panes = 'D5'

    # 综合得分色阶
    if len(top_n_df) > 0:
        score_col = top_cols.index('综合得分') + 1
        col_letter = get_column_letter(score_col)
        ws_top.conditional_formatting.add(
            f'{col_letter}5:{col_letter}{4 + len(top_n_df)}',
            ColorScaleRule(start_type='num', start_value=60, start_color='F8696B',
                           mid_type='num', mid_value=75, mid_color='FFEB84',
                           end_type='num', end_value=95, end_color='63BE7B')
        )

    # ========== Sheet 2: 完整候选池 ==========
    ws_all = wb.create_sheet('完整候选池')
    all_cols = [c for c in top_cols if c != '排名'] + ['硬筛通过', '淘汰原因']
    all_cols = [c for c in all_cols if c in all_df.columns]

    ws_all.merge_cells(f'A1:{get_column_letter(len(all_cols))}1')
    ws_all['A1'] = f"完整候选池分析(共 {len(all_df)} 只)"
    ws_all['A1'].font = F_TITLE
    ws_all['A1'].fill = FILL_TITLE
    ws_all['A1'].alignment = ALIGN_C
    ws_all.row_dimensions[1].height = 28

    for j, col in enumerate(all_cols, 1):
        c = ws_all.cell(row=3, column=j, value=col)
        c.font = F_HEADER
        c.fill = FILL_HEADER
        c.alignment = ALIGN_C
        c.border = BORDER

    # 按综合得分降序输出
    sorted_all = all_df.sort_values('综合得分', ascending=False) if '综合得分' in all_df.columns else all_df
    for i, (_, row) in enumerate(sorted_all.iterrows(), start=4):
        for j, col in enumerate(all_cols, 1):
            val = row.get(col, '')
            if col == '熊市平均回撤' and pd.isna(val):
                bc = row.get('熊市数', 0)
                if pd.isna(bc):
                    bc = 0
                val = '未经历熊市' if int(bc) == 0 else ''
            elif pd.isna(val):
                val = ''
            cell = ws_all.cell(row=i, column=j, value=val)
            cell.font = F_BODY
            cell.alignment = ALIGN_C
            cell.border = BORDER

    for j in range(1, len(all_cols) + 1):
        ws_all.column_dimensions[get_column_letter(j)].width = 12
    ws_all.freeze_panes = 'D4'

    # ========== Sheet 3: 评分规则说明 ==========
    ws_rule = wb.create_sheet('评分规则')
    ws_rule.merge_cells('A1:D1')
    ws_rule['A1'] = '评分规则说明'
    ws_rule['A1'].font = F_TITLE
    ws_rule['A1'].fill = FILL_TITLE
    ws_rule['A1'].alignment = ALIGN_C
    ws_rule.row_dimensions[1].height = 28

    rules = [
        ('硬性筛选(6项,NaN 一律放行)', '', '', ''),
        ('1. 经理任职年限', '≥ 3 年', '放宽至3年纳入近年新锐', '经理表'),
        ('2. 基金成立时间', '≥ 3 年', '新基金数据不可靠', '排名表'),
        ('3. 基金规模', '2 - 100 亿', '过大调仓难,过小有清盘风险', '排名表'),
        ('4. 经理在管基金数', '≤ 5 只', '管太多说明在挂名', '经理表'),
        ('5. 近3年最大回撤', '≤ 45%(绝对)', '风控能力检验,改绝对避免候选池偏差', '净值计算'),
        ('6. 近1年收益兜底', '≥ -25%', '过滤长期好但近期暴雷的基金', '排名表'),
        ('', '', '', ''),
        ('软性评分(6维度,加权满分100,NaN→50)', '权重', '评分逻辑', '指标'),
        ('1. 业绩稳定性', '28%', '近3年收益的全市场分位,越高越好', '全市场分位'),
        ('2. 熊市相对表现', '20%', '历轮熊市平均回撤在候选池内分位 + 经验加成', '净值×熊市区间'),
        ('3. 经理任职年限', '15%', '分段线性: 3年→55, 5→72, 7→85, 10→95, ≥15→100 (边际递减)', '年限分档'),
        ('4. 投资框架', '15%', '卡玛比率(年化收益/最大回撤),≥0.8 满分', '净值计算'),
        ('5. 风格一致性', '12%', '行业配置余弦相似度,≥0.92 满分 / 退化为波动率', '行业配置历史'),
        ('6. 规模适中度', '10%', '钟形分段: [5,30]亿满分; 2→60, 50→85, 100→55, ≥200→30', '规模分档'),
        ('', '', '', ''),
        ('评级标准', '', '', ''),
        ('A 级', '≥ 85', '强烈推荐,核心持仓', '权益部分10-20%'),
        ('B 级', '70 - 85', '推荐,可作备选', '权益部分5-10%'),
        ('C 级', '60 - 70', '观望,继续跟踪', '不持有'),
        ('D 级', '< 60', '不推荐', '不持有'),
    ]
    headers = ['项目', '标准/权重', '说明', '数据来源']
    for j, h in enumerate(headers, 1):
        c = ws_rule.cell(row=3, column=j, value=h)
        c.font = F_HEADER
        c.fill = FILL_HEADER
        c.alignment = ALIGN_C
        c.border = BORDER

    for i, row_data in enumerate(rules, start=4):
        for j, val in enumerate(row_data, 1):
            c = ws_rule.cell(row=i, column=j, value=val)
            c.font = F_BODY
            c.alignment = Alignment(horizontal='left' if j == 1 else 'center', vertical='center', wrap_text=True)
            c.border = BORDER
            # 子标题加粗
            if row_data[0] in ('硬性筛选(6项,NaN 一律放行)', '软性评分(6维度,加权满分100,NaN→50)', '评级标准'):
                c.font = Font(name='微软雅黑', size=11, bold=True, color='1F4E78')
                c.fill = PatternFill('solid', start_color='D9E1F2')

    ws_rule.column_dimensions['A'].width = 28
    ws_rule.column_dimensions['B'].width = 22
    ws_rule.column_dimensions['C'].width = 35
    ws_rule.column_dimensions['D'].width = 22

    # ========== Sheet 4: 回测验证 (仅当传入 backtest 时) ==========
    if backtest and 'summary' in backtest:
        ws_bt = wb.create_sheet('回测验证')
        s = backtest['summary']

        ws_bt.merge_cells('A1:D1')
        ws_bt['A1'] = '评分体系回测验证 (PIT)'
        ws_bt['A1'].font = F_TITLE
        ws_bt['A1'].fill = FILL_TITLE
        ws_bt['A1'].alignment = ALIGN_C
        ws_bt.row_dimensions[1].height = 28

        ws_bt.merge_cells('A2:D2')
        ws_bt['A2'] = (
            f"回测起点 T = {s.get('as_of', '—')}, 持有至 {s.get('hold_end', '—')} "
            f"({s.get('hold_days', '—')} 天), "
            f"宇宙 {s.get('pool_universe_size', '—')} 只, "
            f"硬筛通过 {s.get('pool_passed_hard_filter', '—')} 只"
        )
        ws_bt['A2'].font = Font(name='微软雅黑', size=10, italic=True, color='595959')
        ws_bt['A2'].alignment = ALIGN_L

        # Summary 表
        bt_rows = [
            ('指标', 'Top N', '通过硬筛池子', '整宇宙'),
            ('平均持有期收益 (%)',
                s.get('top_n_avg_return_pct'),
                s.get('pool_passed_avg_return_pct'),
                s.get('universe_avg_return_pct')),
            ('中位数 (%)',
                s.get('top_n_median_return_pct'),
                s.get('pool_passed_median_return_pct'),
                s.get('universe_median_return_pct')),
            ('Top 超额 (pct)',
                None,
                f"vs 池子: {s.get('excess_top_vs_pool_avg', '—')}",
                f"vs 宇宙: {s.get('excess_top_vs_universe_avg', '—')}"),
            ('Top 胜率 vs 中位数 (%)',
                None,
                f"vs 池子: {s.get('win_rate_vs_pool_median_pct', '—')}",
                f"vs 宇宙: {s.get('win_rate_vs_universe_median_pct', '—')}"),
        ]

        for i, row_data in enumerate(bt_rows, start=4):
            for j, val in enumerate(row_data, 1):
                c = ws_bt.cell(row=i, column=j, value=val if val is not None else '')
                if i == 4:
                    c.font = F_HEADER
                    c.fill = FILL_HEADER
                else:
                    c.font = F_BODY
                c.alignment = ALIGN_C
                c.border = BORDER
                if i > 4 and j > 1 and isinstance(val, (int, float)):
                    c.number_format = '0.00'

        # Top N 持有期收益明细
        top_bt = backtest.get('top_n')
        if top_bt is not None and len(top_bt) > 0:
            start_row = 4 + len(bt_rows) + 2
            ws_bt.merge_cells(f'A{start_row}:D{start_row}')
            ws_bt.cell(row=start_row, column=1, value='Top N 持有期收益明细').font = Font(
                name='微软雅黑', size=11, bold=True, color='1F4E78'
            )
            ws_bt.cell(row=start_row, column=1).fill = PatternFill('solid', start_color='D9E1F2')
            ws_bt.cell(row=start_row, column=1).alignment = ALIGN_C

            headers_bt = ['排名', '基金代码', '基金简称', '持有期收益 (%)']
            for j, h in enumerate(headers_bt, 1):
                c = ws_bt.cell(row=start_row + 1, column=j, value=h)
                c.font = F_HEADER
                c.fill = FILL_HEADER
                c.alignment = ALIGN_C
                c.border = BORDER

            for i, (_, r) in enumerate(top_bt.iterrows(), start=1):
                ws_bt.cell(row=start_row + 1 + i, column=1, value=i)
                ws_bt.cell(row=start_row + 1 + i, column=2, value=r.get('基金代码', ''))
                ws_bt.cell(row=start_row + 1 + i, column=3, value=r.get('基金简称', ''))
                ret = r.get('持有期收益')
                ws_bt.cell(row=start_row + 1 + i, column=4,
                           value=float(ret) if pd.notna(ret) else '')
                for j in range(1, 5):
                    cell = ws_bt.cell(row=start_row + 1 + i, column=j)
                    cell.font = F_BODY
                    cell.alignment = ALIGN_C if j != 3 else ALIGN_L
                    cell.border = BORDER
                    if j == 4 and pd.notna(ret):
                        cell.number_format = '0.00'

        ws_bt.column_dimensions['A'].width = 24
        ws_bt.column_dimensions['B'].width = 20
        ws_bt.column_dimensions['C'].width = 30
        ws_bt.column_dimensions['D'].width = 22

    wb.save(output_path)
    logger.info(f'Excel 报告已保存: {output_path}')
    return output_path

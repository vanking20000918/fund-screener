"""
主入口
完整流程: 筛选 → 评分 → 生成报告 → 发送邮件
"""
import os
import sys
import logging
import traceback
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('run.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)


def main():
    """主流程"""
    start_time = datetime.now()
    logger.info(f'\n{"=" * 70}')
    logger.info(f'基金筛选脚本启动 - {start_time.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.info(f'{"=" * 70}\n')

    try:
        from src.screener import run_screening
        from src.report_generator import generate_html_report, generate_excel_report
        from src.mail_sender import send_report
        from src.config import OUTPUT_DIR

        # 1. 运行筛选
        top_n_df, all_df = run_screening()

        if len(top_n_df) == 0:
            logger.warning('未筛选出任何基金,仍发送空报告')

        # 1B. 跑滚动多窗口回测做评分体系验证 (失败降级, 不阻塞月度邮件)
        backtest_result = None
        try:
            from src.backtest import run_rolling_backtest
            logger.info('开始评分体系滚动回测验证 (5 个起点)...')
            backtest_result = run_rolling_backtest(
                hold_end_date=None,
                candidate_pool_size=100, top_n=20, max_universe=300,
            )
        except Exception as bt_e:
            logger.warning(f'回测失败, 报告将不含回测板块: {bt_e}')
            backtest_result = None

        # 2. 生成 HTML 报告
        html = generate_html_report(top_n_df, all_df, backtest=backtest_result)

        # 3. 生成 Excel 附件
        date_str = datetime.now().strftime('%Y%m%d')
        excel_path = os.path.join(OUTPUT_DIR, f'基金筛选报告_{date_str}.xlsx')
        generate_excel_report(top_n_df, all_df, excel_path, backtest=backtest_result)

        # 4. 发送邮件
        subject = f'📊 基金月度筛选报告 - {datetime.now().strftime("%Y年%m月")}'
        success = send_report(subject, html, excel_path)

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f'\n{"=" * 70}')
        logger.info(f'任务完成 · 用时 {elapsed:.1f} 秒 · 邮件发送: {"成功" if success else "失败"}')
        logger.info(f'{"=" * 70}\n')

        return 0 if success else 1

    except Exception as e:
        error_msg = f'{type(e).__name__}: {e}\n\n{traceback.format_exc()}'
        logger.error(f'\n❌ 脚本运行失败:\n{error_msg}')

        # 尝试发送失败通知
        try:
            from src.mail_sender import send_failure_notification
            send_failure_notification(error_msg)
        except Exception as inner_e:
            logger.error(f'失败通知也发送失败: {inner_e}')

        return 1


if __name__ == '__main__':
    sys.exit(main())

"""
邮件发送模块 - QQ 邮箱 SMTP
"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import formataddr
from email import encoders
from datetime import datetime
import os

from .config import EMAIL_CONFIG

logger = logging.getLogger(__name__)


def send_report(subject, html_content, attachment_path=None):
    """
    发送邮件
    subject: 主题
    html_content: HTML 正文
    attachment_path: 附件路径(可选)
    """
    cfg = EMAIL_CONFIG

    if not cfg['sender'] or not cfg['password']:
        logger.error('邮箱配置缺失,请设置 SENDER_EMAIL 和 EMAIL_PASSWORD 环境变量')
        return False

    msg = MIMEMultipart()
    msg['From'] = formataddr(('基金筛选机器人', cfg['sender']))
    msg['To'] = cfg['receiver']
    msg['Subject'] = subject

    # HTML 正文
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))

    # 附件
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, 'rb') as f:
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(f.read())
            encoders.encode_base64(attachment)
            filename = os.path.basename(attachment_path)
            # 中文文件名编码
            attachment.add_header(
                'Content-Disposition',
                'attachment',
                filename=('utf-8', '', filename)
            )
            msg.attach(attachment)
            logger.info(f'已附加附件: {filename}')

    try:
        with smtplib.SMTP_SSL(cfg['smtp_server'], cfg['smtp_port'], timeout=30) as smtp:
            smtp.login(cfg['sender'], cfg['password'])
            smtp.sendmail(cfg['sender'], [cfg['receiver']], msg.as_string())
        logger.info(f'✅ 邮件已发送至 {cfg["receiver"]}')
        return True
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f'❌ SMTP 认证失败: {e}')
        logger.error('   请检查: 1) 是否使用授权码而非登录密码; 2) QQ邮箱是否已开启SMTP服务')
        return False
    except Exception as e:
        logger.error(f'❌ 邮件发送失败: {e}')
        return False


def send_failure_notification(error_msg):
    """脚本失败时发送失败通知邮件"""
    cfg = EMAIL_CONFIG
    if not cfg['sender'] or not cfg['password']:
        return False

    html = f"""
    <html><body style="font-family: 'Microsoft YaHei'; padding: 20px;">
        <h2 style="color: #c00;">⚠️ 基金筛选脚本运行失败</h2>
        <p>时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <div style="background: #ffe4e1; padding: 15px; border-left: 4px solid #c00; font-family: monospace;">
            <pre>{error_msg}</pre>
        </div>
        <p style="color: #888; font-size: 12px;">
            请前往 GitHub Actions 查看完整日志,或在本地排查问题。
        </p>
    </body></html>
    """
    return send_report('⚠️ 基金筛选脚本运行失败', html)

# 主动基金月度筛选机器人

每月底自动筛选 A 股主动股票型 + 偏股混合型基金,推荐前 10 名,通过邮件发送到指定邮箱。

## 🎯 它做什么

1. 从 AKShare 拉取全市场主动权益基金数据
2. 应用 **6 项硬性筛选**(经理任职年限、规模、回撤等)
3. 计算 **7 维度软性评分**(业绩稳定性、夏普、熊市表现等)
4. 加权排序,输出 Top 10 推荐
5. 生成 HTML 邮件正文 + Excel 详细附件
6. 通过 QQ 邮箱 SMTP 发送到你的邮箱

## 📁 项目结构

```
fund_screener/
├── .github/workflows/
│   └── monthly_report.yml      # GitHub Actions 定时任务配置
├── src/
│   ├── __init__.py
│   ├── config.py               # 所有可调参数
│   ├── data_fetcher.py         # AKShare 数据获取(含缓存与重试)
│   ├── metrics.py              # 指标计算(回撤、夏普、熊市数等)
│   ├── screener.py             # 筛选与评分主逻辑
│   ├── report_generator.py     # HTML + Excel 报告生成
│   └── mail_sender.py          # QQ 邮箱 SMTP 发送
├── main.py                     # 主入口
├── requirements.txt            # Python 依赖
├── .gitignore
└── README.md                   # 本文件
```

## 🚀 部署步骤(GitHub Actions 方案)

### 第 1 步:准备 GitHub 仓库

1. 在 GitHub 创建一个**私有仓库**(避免日志或测试输出泄露隐私),例如名为 `fund-screener`
2. 把本项目所有文件上传到该仓库

```bash
# 在本地命令行中
cd fund_screener
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/你的用户名/fund-screener.git
git push -u origin main
```

### 第 2 步:开通 QQ 邮箱 SMTP 服务,获取授权码

1. 登录 [QQ 邮箱网页版](https://mail.qq.com)
2. 顶部菜单 → **设置** → **账户**
3. 往下翻找到 **"POP3/IMAP/SMTP/Exchange/CardDAV 服务"**
4. 开启 **"IMAP/SMTP 服务"** (按提示发送短信验证)
5. 系统会给你一个 **16 位授权码**,例如 `abcdefghijklmnop`
6. **务必复制保存**,这是 SMTP 登录密码,不是你的 QQ 密码

### 第 3 步:在 GitHub 配置 Secrets

打开仓库 → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

添加 **3 个 Secret**:

| 名称 | 值 | 说明 |
|---|---|---|
| `SENDER_EMAIL` | `你的QQ号@qq.com` | 用于发送邮件的 QQ 邮箱 |
| `EMAIL_PASSWORD` | `授权码`(第2步那个16位串) | **不是 QQ 密码**,是 SMTP 授权码 |
| `RECEIVER_EMAIL` | `1793031400@qq.com` | 接收报告的邮箱(默认值已写在 config.py,如不变可不设此项) |

### 第 4 步:启用 GitHub Actions

1. 打开仓库 → **Actions** 标签页
2. 如果提示 "Actions are disabled",点击启用
3. 在左侧应该能看到 "Monthly Fund Report" 工作流

### 第 5 步:测试运行

**手动触发一次,验证一切配置正确**:

1. 进入 **Actions** 标签页
2. 点击左侧 "Monthly Fund Report"
3. 右上角 **"Run workflow"** → 选择 `main` 分支 → **Run workflow**
4. 等待运行结束(首次约 15-25 分钟)
5. 查看 1793031400@qq.com 邮箱是否收到报告

### 第 6 步:确认定时调度

- 配置已设定为**每月 28-31 号的北京时间 18:00**(对应 UTC 10:00)自动运行
- 脚本会在 GitHub Actions 中先判断**今天是否本月最后一天**,只在月末实际跑
- 这样能保证每个月只执行 1 次,不会浪费 GitHub 免费额度

## 🔧 配置调整

打开 `src/config.py`,可修改:

```python
# 硬性筛选阈值
HARD_FILTER = {
    'min_manager_years': 5,        # 经理任职年限要求,默认5年
    'min_fund_age': 3,             # 基金成立年限,默认3年
    'min_scale': 2.0,              # 最小规模(亿)
    'max_scale': 100.0,            # 最大规模(亿)
    ...
}

# 评分权重(总和必须=1.0)
SCORE_WEIGHTS = {
    'stability': 0.25,    # 业绩稳定性
    'style': 0.15,        # 风格一致性
    'framework': 0.10,    # 投资框架
    ...
}

# 输出数量
TOP_N = 10              # 改成 20 就推荐前 20 名

# 候选池大小
PERF_CONFIG = {
    'candidate_pool_size': 150,    # 详细分析的基金数量,越大越慢
    ...
}
```

## 💻 本地测试运行

如果你想先在本地跑通一遍再上传:

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 设置环境变量(临时,关闭终端就消失)
export SENDER_EMAIL="你的QQ邮箱"
export EMAIL_PASSWORD="授权码"
export RECEIVER_EMAIL="1793031400@qq.com"

# 3. 运行
python main.py
```

Windows PowerShell:
```powershell
$env:SENDER_EMAIL = "你的QQ邮箱"
$env:EMAIL_PASSWORD = "授权码"
python main.py
```

## ⚠️ 已知限制与注意事项

### 1. 数据精度限制

由于 AKShare 是公开接口,有几项指标用了**估算值或代理指标**:

| 指标 | 实际做法 | 影响 |
|---|---|---|
| 综合费率 | 默认 1.75%(行业均值) | 真实费率请查基金合同 |
| 机构持有比例 | 默认 30% | 跳过该项硬筛 |
| 风格一致性 | 用波动率作代理 | 与持仓行业偏离度不完全等价 |
| 投资框架 | 用夏普比率作代理 | 主观维度难以完全量化 |

**结论**: 本工具是**初筛工具**,出来的 Top 10 应该结合人工核查再做决策。

### 2. AKShare 数据源稳定性

- 高频请求可能被限流,本项目已设置 0.3 秒/次的延迟
- 数据接口偶尔会改版,如出错请 `pip install -U akshare` 升级
- 首次运行约 15-25 分钟,后续有缓存加速

### 3. GitHub Actions 免费额度

- 免费账户每月 2000 分钟,本项目每次运行约 20 分钟,完全够用
- 私有仓库才占用配额,公有仓库不限

### 4. 月末日期处理

代码用了"今天的明天月份不同 = 今天是月末"的判断方式,可正确识别 28/29/30/31 号。

## 🐛 故障排查

### 邮件没收到

1. 检查 GitHub Actions 日志:**Actions** → 最近一次运行 → 看是否有红色错误
2. 检查 QQ 邮箱**垃圾邮件**文件夹
3. 检查 Secrets 是否正确填写,特别是授权码无空格

### 报错 `SMTPAuthenticationError`

- 99% 是把 QQ 登录密码当成了授权码,请重新生成
- 或者授权码失效,请重新申请

### 报错 `akshare 接口不可用`

- 升级 akshare: 在 `requirements.txt` 里把版本号改成 `akshare>=最新版`
- 推到 GitHub 触发重新安装

### Top 10 数量不足

- 当月通过硬筛的基金不够 10 只,这很正常
- 可在 `config.py` 适当放宽 `HARD_FILTER` 阈值

## 📝 后续可扩展方向

- 加入持仓相似度分析,避免推荐风格重复的基金
- 加入业绩归因(因子暴露)分析
- 增加因子(动量、价值、质量)轮动维度
- 加入跌幅自动提醒(配合你的定投策略)
- 把月度报告同步到 Notion / 飞书 / Telegram

## 📄 License

MIT License — 自由使用,但不对任何投资损失负责。

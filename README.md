# 主动基金月度筛选机器人

每月底自动筛选 A 股主动股票型 + 混合型 + QDII 基金,推荐前 20 名,通过邮件发送到指定邮箱。

## 它做什么

1. 从天天基金网拉取全市场基金数据(股票型 + 混合型 + QDII)
2. 按近 3 年收益率排序,取前 300 只进入详细分析
3. 应用 **6 项硬性筛选**(经理任职年限、规模、回撤等)
4. 计算 **6 维度软性评分** (评分卡 v2.1: 业绩稳定性 / 熊市表现 / 风格 — 均改为**候选池内分位 + 阈值密化**, 避免顶部全员满分)
5. 加权排序,输出 Top 20 推荐 + **人话版入选原因** (近3年业绩排候选池前X%; 历Y轮熊市平均回撤Z%; ...)
6. 跑**滚动 5 起点 PIT 回测** (T-60/48/36/24/18 月) 验证评分体系是否产生跨周期 alpha
7. 月度脚本自动基于候选池中位 NAV **检测新熊市段**, 在日志给出 BEAR_MARKETS 维护建议
8. 生成 HTML 邮件正文 + Excel 详细附件 (含回测验证 sheet) 通过 QQ 邮箱 SMTP 发送

## 项目结构

```
fund_screener/
├── .github/workflows/
│   └── monthly_report.yml          # GitHub Actions 定时任务配置
├── src/
│   ├── __init__.py
│   ├── config.py                   # 所有可调参数
│   ├── data_fetcher.py             # AKShare 数据获取(备用数据源)
│   ├── data_fetcher_eastmoney.py   # 天天基金网直接爬取(默认数据源,并发 8 线程)
│   ├── metrics.py                  # 指标计算(回撤、卡玛、熊市数等)
│   ├── screener.py                 # 筛选与评分主逻辑
│   ├── backtest.py                 # 滚动 5 起点 PIT 回测 (run_rolling_backtest)
│   ├── report_generator.py         # HTML + Excel 报告生成
│   └── mail_sender.py              # QQ 邮箱 SMTP 发送
├── main.py                         # 月度报告主入口
├── backtest.py                     # 回测脚本入口: python backtest.py (默认 5 起点滚动) / --date YYYY-MM-DD (单点)
├── requirements.txt                # Python 依赖
├── .gitignore
└── README.md                       # 本文件
```

## 部署步骤(GitHub Actions 方案)

### 第 1 步:准备 GitHub 仓库

1. 在 GitHub 创建一个**私有仓库**(避免日志或测试输出泄露隐私),例如名为 `fund-screener`
2. 把本项目所有文件上传到该仓库

```bash
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
4. 等待运行结束(并发抓取后约 5-10 分钟)
5. 查看邮箱是否收到报告

### 第 6 步:确认定时调度

- 配置已设定为**每月 28-31 号的北京时间 18:00**(对应 UTC 10:00)自动运行
- 脚本会在 GitHub Actions 中先判断**今天是否本月最后一天**,只在月末实际跑
- 这样能保证每个月只执行 1 次,不会浪费 GitHub 免费额度

## 配置调整

打开 `src/config.py`,可修改:

```python
# 数据源切换
DATA_SOURCE = 'eastmoney'  # 'eastmoney' (天天基金网) 或 'akshare' (AKShare库)

# 筛选范围
FUND_TYPES = ['股票型', '混合型', 'QDII']

# 硬性筛选阈值(NaN 一律放行,由软评分降权)
HARD_FILTER = {
    'min_manager_years': 3,        # 经理任职年限要求(放宽至3年纳入新锐)
    'min_fund_age': 3,             # 基金成立年限
    'min_scale': 2.0,              # 最小规模(亿)
    'max_scale': 100.0,            # 最大规模(亿)
    'max_funds_per_manager': 5,    # 经理在管基金数上限
    'max_drawdown_pct': 45.0,      # 近3年最大回撤绝对值
    'min_recent_1y_return': -25.0, # 近1年收益兜底,防长期好但近期暴雷
}

# 评分权重(总和=1.0, 评分卡 v2.1)
SCORE_WEIGHTS = {
    'stability': 0.28,    # 业绩稳定性: 近3年收益**候选池内分位** + 阈值密化 (≤5→100/≤10→92/.../≤90→28)
    'bear_perf': 0.20,    # 熊市相对表现: 历轮熊市回撤候选池内分位 + 阈值密化 + (熊市数-1)×2 加成
    'tenure':    0.15,    # 经理任职年限: 分段线性 3→55, 7→85, 15+→100
    'framework': 0.15,    # 投资框架: 卡玛比率(年化收益/最大回撤)
    'style':     0.12,    # 风格一致性: 行业相似度**候选池内分位** (数据不足回退波动率)
    'scale':     0.10,    # 规模适中度: 钟形 5-30亿满分, 100→55, 200+→30
}

# 熊市区间 (外置, 月度自动检测新段并日志建议)
BEAR_MARKETS = [
    ('2018-01-29', '2019-01-04'),
    ('2021-12-13', '2022-10-31'),
    ('2023-08-01', '2024-02-05'),
]

# 滚动回测起点 (月数 offset, 相对运行时刻)
BACKTEST_ROLLING_OFFSETS_MONTHS = [60, 48, 36, 24, 18]

# 输出数量
TOP_N = 20              # 推荐前 20 名

# 候选池大小
PERF_CONFIG = {
    'candidate_pool_size': 300,    # 详细分析的基金数量,越大越慢
    ...
}
```

## 本地测试运行

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

## 已知限制与注意事项

### 1. 数据源

默认使用**天天基金网**直接爬取,也可切换为 AKShare(设置 `DATA_SOURCE = 'akshare'`)。

天天基金网数据获取方式:
- 排名数据: 一次请求获取全部基金排名
- 净值数据: `pingzhongdata` 接口一次请求获取全部历史净值,**并发 8 线程**
- 详情数据: 经理(jjjl) + 概况(jbgk) 两页,**并发 8 线程**抓取
- 行业配置: 通过 akshare 拉取近 2 年, **仅给硬筛通过的基金抓**(性能优化)
- HTTP 超时 8s, 失败基金快速放弃, 避免单只拖累整批

### 2. 数据精度限制

有几项指标用了**估算值或代理指标**:

| 指标 | 实际做法 | 影响 |
|---|---|---|
| 风格一致性 | 优先用近 2 年行业配置余弦相似度; NaN 时回退波动率代理 | 行业数据缺失时与持仓偏离度不完全等价 |
| 投资框架 | 用**卡玛比率**(年化收益/最大回撤) | 主观维度难以完全量化,但卡玛比夏普更贴近"收益-风险" |
| NaN 数据 | 硬筛一律放行,软评分回中位 50 分 | 数据缺失基金不被误杀,但也不会被吹高 |

**结论**: 本工具是**初筛工具**,出来的 Top 20 应该结合人工核查再做决策。

### 3. GitHub Actions 免费额度

- 免费账户每月 2000 分钟,本项目每次运行约 10-20 分钟,完全够用
- 私有仓库才占用配额,公有仓库不限

### 4. 月末日期处理

代码用了"今天的明天月份不同 = 今天是月末"的判断方式,可正确识别 28/29/30/31 号。

## 故障排查

### 邮件没收到

1. 检查 GitHub Actions 日志:**Actions** → 最近一次运行 → 看是否有红色错误
2. 检查 QQ 邮箱**垃圾邮件**文件夹
3. 检查 Secrets 是否正确填写,特别是授权码无空格

### 报错 `SMTPAuthenticationError`

- 99% 是把 QQ 登录密码当成了授权码,请重新生成
- 或者授权码失效,请重新申请

### Top 20 数量不足

- 当月通过硬筛的基金不够 20 只,这很正常
- 可在 `config.py` 适当放宽 `HARD_FILTER` 阈值

## 后续可扩展方向

- 加入持仓相似度分析,避免推荐风格重复的基金
- 加入业绩归因(因子暴露)分析
- 增加因子(动量、价值、质量)轮动维度
- 加入跌幅自动提醒(配合定投策略)
- 把月度报告同步到 Notion / 飞书 / Telegram

## License

MIT License — 自由使用,但不对任何投资损失负责。

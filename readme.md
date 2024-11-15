# Stock Market Analysis Tool

一个基于 Tushare 的股票市场数据分析工具，支持获取和分析 A 股市场的各类数据。

## 功能特点

- 数据获取

  - 日线行情数据
  - 分钟级别数据
  - 股票基本信息
  - 涨跌停价格
  - 交易日历
  - 新闻资讯

- 数据分析
  - 行业板块分析
  - 涨停板分析
  - 连板概率统计
  - 市场分布分析

## 系统要求

- Python 3.8+
- SQLite 3
- Tushare Pro 账号和 API Token

## 安装

1. 克隆仓库

```bash
git clone https://github.com/logda/StockRadar-v3.git
cd StockRadar-v3
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 配置 Tushare Token

```
cp config.example.py config.py
# 填入您的 Tushare Token
```

## 使用说明

### 1. 初始化基础数据

```bash
# 获取股票基本信息
python fetcher/stock_basic_fetcher.py

# 获取交易日历
python fetcher/trade_cal_fetcher.py --start-date 20240101 --end-date 20241231
```

### 2. 获取行情数据

```bash
# 获取日线数据
python fetcher/daily_fetcher.py --date 20240321

# 获取分钟数据
python fetcher/stock_min_fetcher.py --ts-code 000001.SZ --start-datetime "2024-03-21 09:30:00" --end-datetime "2024-03-21 15:00:00" --freq 1min

# 获取涨跌停价格
python fetcher/stock_limit_fetcher.py --date 20240321
```

### 3. 数据分析

```bash
# 行业板块分析
python service/industry_analyzer.py --date 20240321

# 涨停板分析
python service/limit_up_analyzer.py --date 20240321 --max-days 5

# 昨日涨停今日表现
python service/limit_up_analyzer_v2.py --date 20240321
```

## 项目结构

```
stock-analysis-tool/
├── config.py           # 配置文件
├── db/                 # 数据库文件目录
├── fetcher/           # 数据获取模块
│   ├── daily_fetcher.py
│   ├── stock_min_fetcher.py
│   ├── stock_basic_fetcher.py
│   └── ...
├── service/           # 分析服务模块
│   ├── industry_analyzer.py
│   ├── limit_up_analyzer.py
│   └── ...
└── utils/             # 工具函数
    └── trade_cal_utils.py
```

## 数据库结构

项目使用 SQLite 数据库存储数据，主要包含以下表：

- daily_quotes: 日线行情数据
- minute_quotes: 分钟级别数据
- stock_basic: 股票基本信息
- stock_limits: 涨跌停价格
- trade_calendar: 交易日历
- news: 新闻资讯

## 注意事项

1. 请确保有足够的 Tushare 积分以获取所需数据
2. 数据获取频率需要遵守 Tushare 的限制
3. 首次运行需要初始化基础数据
4. 建议定期更新交易日历数据

## License

MIT

## 贡献指南

欢迎提交 Issue 和 Pull Request 来帮助改进项目。

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交改动 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

```

```

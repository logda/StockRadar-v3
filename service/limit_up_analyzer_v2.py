"""
昨日涨停股票今日表现分析工具

此模块分析昨日涨停股票在今日的表现情况，包括：
1. 上涨概率
2. 下跌概率
3. 继续涨停概率
4. 跌停概率

使用示例:
----------
    >>> python limit_up_analyzer_v2.py --date 20240321
    
    20240321 昨日涨停股票今日表现:
    昨日涨停股数: 146
    - 上涨数量: 89 (60.96%)
    - 下跌数量: 57 (39.04%)
    - 继续涨停: 52 (35.62%)
    - 跌停数量: 3 (2.05%)
"""

import sqlite3
import pandas as pd
import os
import sys
from typing import Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH
from utils.trade_cal_utils import get_previous_trade_day
from service.limit_up_analyzer import ensure_data_exists


def get_yesterday_limit_up_performance(trade_date: str) -> Dict:
    """分析昨日涨停股票今日表现"""
    # 获取昨日日期
    yesterday = get_previous_trade_day(trade_date)

    # 确保数据存在
    ensure_data_exists(trade_date)
    ensure_data_exists(yesterday)

    conn = sqlite3.connect(DATABASE_PATH)

    # 获取昨日涨停股票
    yesterday_query = """
    SELECT 
        d.ts_code,
        d.close as yesterday_close,
        l.up_limit as yesterday_limit
    FROM daily_quotes d
    JOIN stock_limits l ON d.ts_code = l.ts_code AND d.trade_date = l.trade_date
    WHERE d.trade_date = ?
    """

    # 获取今日这些股票的表现
    today_query = """
    SELECT 
        d.ts_code,
        d.close as today_close,
        l.up_limit as today_up_limit,
        l.down_limit as today_down_limit
    FROM daily_quotes d
    JOIN stock_limits l ON d.ts_code = l.ts_code AND d.trade_date = l.trade_date
    WHERE d.trade_date = ?
    """

    yesterday_df = pd.read_sql_query(yesterday_query, conn, params=(yesterday,))
    today_df = pd.read_sql_query(today_query, conn, params=(trade_date,))

    conn.close()

    # 标记昨日涨停股票
    yesterday_df["is_limit_up"] = (
        yesterday_df["yesterday_close"] >= yesterday_df["yesterday_limit"]
    )
    yesterday_limit_stocks = yesterday_df[yesterday_df["is_limit_up"]]

    # 合并今日表现数据
    merged_df = yesterday_limit_stocks.merge(today_df, on="ts_code", how="left")

    # 计算涨跌幅
    merged_df["change_ratio"] = (
        (merged_df["today_close"] - merged_df["yesterday_close"])
        / merged_df["yesterday_close"]
        * 100
    )

    # 统计各项指标
    total_stocks = len(merged_df)
    up_stocks = len(merged_df[merged_df["change_ratio"] > 0])
    down_stocks = len(merged_df[merged_df["change_ratio"] < 0])
    limit_up_stocks = len(
        merged_df[merged_df["today_close"] >= merged_df["today_up_limit"]]
    )
    limit_down_stocks = len(
        merged_df[merged_df["today_close"] <= merged_df["today_down_limit"]]
    )

    stats = {
        "total_count": total_stocks,
        "up_count": up_stocks,
        "up_ratio": round(up_stocks / total_stocks * 100, 2) if total_stocks > 0 else 0,
        "down_count": down_stocks,
        "down_ratio": (
            round(down_stocks / total_stocks * 100, 2) if total_stocks > 0 else 0
        ),
        "limit_up_count": limit_up_stocks,
        "limit_up_ratio": (
            round(limit_up_stocks / total_stocks * 100, 2) if total_stocks > 0 else 0
        ),
        "limit_down_count": limit_down_stocks,
        "limit_down_ratio": (
            round(limit_down_stocks / total_stocks * 100, 2) if total_stocks > 0 else 0
        ),
    }

    return stats


def main():
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="昨日涨停股票今日表现分析工具")
    parser.add_argument("--date", required=True, help="指定分析日期 (格式: YYYYMMDD)")
    args = parser.parse_args()

    try:
        # 验证日期格式
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print("日期格式错误，请使用YYYYMMDD格式")
        sys.exit(1)

    try:
        stats = get_yesterday_limit_up_performance(args.date)

        print(f"\n{args.date} 昨日涨停股票今日表现:")
        print(f"昨日涨停股数: {stats['total_count']}")
        print(f"- 上涨数量: {stats['up_count']} ({stats['up_ratio']}%)")
        print(f"- 下跌数量: {stats['down_count']} ({stats['down_ratio']}%)")
        print(f"- 继续涨停: {stats['limit_up_count']} ({stats['limit_up_ratio']}%)")
        print(f"- 跌停数量: {stats['limit_down_count']} ({stats['limit_down_ratio']}%)")

    except Exception as e:
        print(f"分析过程出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

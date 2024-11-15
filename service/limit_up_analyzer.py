"""
股市连续涨停概率分析工具

此模块提供了一个用于分析股市连续涨停（连板）情况的工具。通过分析历史交易数据，
计算各种涨停概率指标，帮助投资者了解市场的涨停热度和连续涨停趋势。

主要功能:
    1. 计算指定日期的整体市场涨停概率
    2. 分析连续涨停情况，包括连续2板到N板的概率
    3. 提供详细的数据统计，包括具体的股票数量和百分比

参数说明:
----------
date : str
    需要分析的交易日期，格式为YYYYMMDD
max_days : int, optional
    最大回溯天数，即最大连板数分析，默认为10天

统计指标:
----------
1. 基础统计
    - 当日涨停概率：当日涨停股数/总股票数
    - 涨停股票数量：达到涨停价的股票数量
    
2. 连板统计
    - 今天涨停股有多少是昨天也涨停
    - 连续N日涨停后继续涨停概率(N=2,3,...)
    
3. 数据明细
    - 每个统计指标都包含具体的股票数量和占比
    - 展示分子分母数据，方便验证和分析

使用示例:
----------
    >>> python limit_up_analyzer.py --date 20240321 --max-days 5
    
    20240321 连板概率统计:
    今日涨停概率: 8.45% (324/3834)
    昨日涨停今日继续涨停概率: 35.62% (52/146)
    连续2天涨停后继续涨停概率: 42.86% (12/28)
    连续3天涨停后继续涨停概率: 50.00% (4/8)

注意事项:
----------
1. 需要确保数据库中有足够的历史数据用于分析
2. 涨停判定基于stock_limits表的up_limit价格
3. 节假日期间可能因为停牌导致数据缺失
"""

import sqlite3
import pandas as pd
from typing import Dict, List
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH
from fetcher.stock_basic_fetcher import fetch_and_save_basic_info
from fetcher.daily_fetcher import fetch_and_save_data
from fetcher.stock_limit_fetcher import fetch_and_save_limits
from utils.trade_cal_utils import get_previous_n_trade_days


def check_stock_basic_exists():
    """检查stock_basic表是否存在数据"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_basic")
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def check_daily_quotes_exists(trade_date):
    """检查daily_quotes表中是否存在指定交易日的数据"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM daily_quotes WHERE trade_date = ? LIMIT 1", (trade_date,)
    )
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def check_stock_limits_exists(trade_date):
    """检查stock_limits表中是否存在指定交易日的数据"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM stock_limits WHERE trade_date = ? LIMIT 1", (trade_date,)
    )
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def ensure_data_exists(trade_date: str):
    """确保指定日期的所有必要数据都存在"""
    # 检查并初始化stock_basic数据
    if not check_stock_basic_exists():
        print("stock_basic表中没有数据，正在获取并保存股票基础信息...")
        try:
            fetch_and_save_basic_info()
            print("成功获取并保存股票基础信息")
        except Exception as e:
            raise Exception(f"获取股票基础信息失败: {str(e)}")
    # 检查并初始化daily_quotes数据
    if not check_daily_quotes_exists(trade_date):
        print(f"daily_quotes表中没有{trade_date}的数据，正在获取并保存数据...")
        try:
            fetch_and_save_data(trade_date)
            print(f"成功获取并保存{trade_date}的日线数据")
        except Exception as e:
            raise Exception(f"获取{trade_date}的日线数据失败: {str(e)}")

    # 检查并初始化stock_limits数据
    if not check_stock_limits_exists(trade_date):
        print(f"stock_limits表中没有{trade_date}的数据，正在获取并保存数据...")
        try:
            fetch_and_save_limits(trade_date)
            print(f"成功获取并保存{trade_date}的涨跌停数据")
        except Exception as e:
            raise Exception(f"获取{trade_date}的涨跌停数据失败: {str(e)}")


def get_continuous_limit_stats(trade_date: str, max_days: int = 10) -> Dict:
    """分析指定日期的连板概率统计"""
    conn = sqlite3.connect(DATABASE_PATH)

    date_list = [trade_date] + get_previous_n_trade_days(trade_date, max_days - 1)

    if len(date_list) < 2:
        raise ValueError("没有足够的历史数据进行分析")

    # 确保所有需要的日期数据都存在
    for date in date_list:
        ensure_data_exists(date)

    # 修改基础查询：使用stock_limits表的up_limit判断涨停
    base_query = """
    SELECT 
        d.ts_code, 
        d.trade_date,
        d.close,
        l.up_limit
    FROM daily_quotes d
    JOIN stock_limits l ON d.ts_code = l.ts_code 
        AND d.trade_date = l.trade_date
    WHERE d.trade_date IN ({})
    """.format(
        ",".join(["?"] * len(date_list))
    )

    df = pd.read_sql_query(base_query, conn, params=date_list)

    # 获取总股票数
    total_stocks_query = (
        "SELECT COUNT(DISTINCT ts_code) as count FROM daily_quotes WHERE trade_date = ?"
    )
    total_stocks = pd.read_sql_query(total_stocks_query, conn, params=(trade_date,))[
        "count"
    ].iloc[0]

    conn.close()

    # 标记涨停股票
    df["is_limit_up"] = df["close"] >= df["up_limit"]

    # 获取每个日期的涨停股票
    limit_up_df = df[df["is_limit_up"]][["ts_code", "trade_date"]]

    # 计算统计数据
    stats = {}

    # 计算今日涨停概率
    today_limit_up = len(limit_up_df[limit_up_df["trade_date"] == trade_date])
    stats["today_probability"] = round(today_limit_up / total_stocks * 100, 2)
    stats["today_limit_up_count"] = today_limit_up
    stats["total_stocks"] = total_stocks

    # 计算连续涨停概率
    for days in range(1, min(len(date_list), max_days + 1)):
        continuous_limit_stocks = get_continuous_limit_stocks(
            limit_up_df, date_list[: days + 1]
        )

        if days == 1:
            # 昨日涨停今日继续涨停概率
            yesterday_limit_stocks = len(
                limit_up_df[
                    limit_up_df["trade_date"] == date_list[1]
                ]  # 昨天涨停的股票数量
            )
            stats[f"yesterday_limit_up_count"] = yesterday_limit_stocks
            stats[f"continuous_1_day_count"] = len(
                continuous_limit_stocks
            )  # 昨天和今天都涨停的股票数量

            if yesterday_limit_stocks > 0:
                stats[f"continuous_1_day"] = round(
                    len(continuous_limit_stocks) / yesterday_limit_stocks * 100, 2
                )
            else:
                stats[f"continuous_1_day"] = 0
        else:
            # N天连续涨停概率
            previous_continuous = get_continuous_limit_stocks(
                limit_up_df, date_list[1 : days + 1]
            )  # 昨天至
            stats[f"continuous_{days}_days_base_count"] = len(previous_continuous)
            stats[f"continuous_{days}_days_count"] = len(continuous_limit_stocks)

            if len(previous_continuous) > 0:
                stats[f"continuous_{days}_days"] = round(
                    len(continuous_limit_stocks) / len(previous_continuous) * 100, 2
                )
            else:
                stats[f"continuous_{days}_days"] = 0

    return stats


def get_continuous_limit_stocks(df: pd.DataFrame, dates: List[str]) -> set:
    """获取在指定日期列表中连续涨停的股票代码"""
    continuous_stocks = set(df[df["trade_date"] == dates[0]]["ts_code"])

    for date in dates[1:]:
        date_stocks = set(df[df["trade_date"] == date]["ts_code"])
        continuous_stocks = continuous_stocks.intersection(date_stocks)

    return continuous_stocks


def main():
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="股市连板概率统计工具")
    parser.add_argument("--date", required=True, help="指定分析日期 (格式: YYYYMMDD)")
    parser.add_argument(
        "--max-days", type=int, default=10, help="最大统计天数 (默认: 10)"
    )
    args = parser.parse_args()

    try:
        # 验证日期格式
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print("日期格式错误，请使用YYYYMMDD格式")
        sys.exit(1)

    try:
        stats = get_continuous_limit_stats(args.date, args.max_days)

        print(f"\n{args.date} 连板概率统计:")
        print(
            f"今日涨停概率: {stats['today_probability']}% ({stats['today_limit_up_count']}/{stats['total_stocks']})"
        )
        print(
            f"昨日涨停今日继续涨停概率: {stats['continuous_1_day']}% ({stats['continuous_1_day_count']}/{stats['yesterday_limit_up_count']})"
        )

        for days in range(2, args.max_days + 1):
            if f"continuous_{days}_days" in stats:
                print(
                    f"连续{days}天涨停后继续涨停概率: {stats[f'continuous_{days}_days']}% "
                    f"({stats[f'continuous_{days}_days_count']}/{stats[f'continuous_{days}_days_base_count']})"
                )

    except Exception as e:
        print(f"分析过程出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

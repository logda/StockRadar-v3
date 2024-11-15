import sqlite3
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH
from fetcher.stock_basic_fetcher import fetch_and_save_basic_info
from fetcher.daily_fetcher import fetch_and_save_data


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


def analyze_industry_stats(trade_date):
    """分析指定交易日的行业统计数据"""
    conn = sqlite3.connect(DATABASE_PATH)

    # 修改查询以使用stock_limits表判断涨停
    query = """
    SELECT 
        b.industry,
        COUNT(*) as total_stocks,
        SUM(CASE WHEN d.pct_chg > 0 THEN 1 ELSE 0 END) as up_stocks,
        SUM(CASE WHEN d.close >= l.up_limit AND d.close > 0 THEN 1 ELSE 0 END) as limit_up_stocks,
        SUM(d.amount) as total_amount,
        AVG(d.pct_chg) as avg_change
    FROM stock_basic b
    JOIN daily_quotes d ON b.ts_code = d.ts_code
    LEFT JOIN stock_limits l ON d.ts_code = l.ts_code AND d.trade_date = l.trade_date
    WHERE d.trade_date = ?
    GROUP BY b.industry
    """

    df = pd.read_sql_query(query, conn, params=(trade_date,))
    conn.close()

    # 添加日期列
    df["trade_date"] = trade_date

    # 添加涨跌股比例列
    df["up_ratio"] = (df["up_stocks"] / df["total_stocks"] * 100).round(2)
    df["limit_up_ratio"] = (df["limit_up_stocks"] / df["total_stocks"] * 100).round(2)

    # 格式化交易额（转换为亿元）
    df["total_amount"] = (df["total_amount"] / 100000000).round(2)

    # 格式化涨跌幅
    df["avg_change"] = df["avg_change"].round(2)

    # 按涨停个股数量降序排序
    df = df.sort_values(by="total_amount", ascending=False)

    # 重命名列以便更好地显示
    df = df.rename(
        columns={
            "industry": "行业",
            "total_stocks": "总股票数",
            "up_stocks": "上涨个股数",
            "limit_up_stocks": "涨停个股数",
            "total_amount": "成交额(亿)",
            "avg_change": "平均涨幅(%)",
            "up_ratio": "上涨比例(%)",
            "limit_up_ratio": "涨停比例(%)",
            "trade_date": "交易日期",
        }
    )

    df = df[
        [
            "行业",
            "交易日期",
            "总股票数",
            "上涨个股数",
            "涨停个股数",
            "成交额(亿)",
            "平均涨幅(%)",
            "上涨比例(%)",
            "涨停比例(%)",
        ]
    ]

    return df


def main():
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="行业板块统计分析工具")
    parser.add_argument("--date", required=True, help="指定分析日期 (格式: YYYYMMDD)")
    args = parser.parse_args()

    try:
        # 验证日期格式
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print("日期格式错误，请使用YYYYMMDD格式")
        sys.exit(1)

    # 检查并初始化stock_basic数据
    if not check_stock_basic_exists():
        print("stock_basic表中没有数据，正在获取并保存股票基础信息...")
        try:
            fetch_and_save_basic_info()
            print("成功获取并保存股票基础信息")
        except Exception as e:
            print(f"获取股票基础信息失败: {str(e)}")
            sys.exit(1)

    # 检查并初始化daily_quotes数据
    if not check_daily_quotes_exists(args.date):
        print(f"daily_quotes表中没有{args.date}的数据，正在获取并保存数据...")
        try:
            fetch_and_save_data(args.date)
            print(f"成功获取并保存{args.date}的日线数据")
        except Exception as e:
            print(f"获取{args.date}的日线数据失败: {str(e)}")
            sys.exit(1)

    # 检查并初始化stock_limits数据
    if not check_stock_limits_exists(args.date):
        print(f"stock_limits表中没有{args.date}的数据，正在获取并保存数据...")
        try:
            from fetcher.stock_limit_fetcher import fetch_and_save_limits

            fetch_and_save_limits(args.date)
            print(f"成功获取并保存{args.date}的涨跌停数据")
        except Exception as e:
            print(f"获取{args.date}的涨跌停数据失败: {str(e)}")
            sys.exit(1)

    # 获取统计数据
    try:
        df = analyze_industry_stats(args.date)
        df.head(3).to_clipboard(index=False, header=False)
        # df.head(3).to_clipboard(index=False)

        print(f"\n{args.date} 行业板块统计:")

        # 设置pandas显示选项
        pd.set_option("display.max_rows", None)  # 显示所有行
        pd.set_option("display.max_columns", None)  # 显示所有列
        pd.set_option("display.width", None)  # 自动调整显示宽度
        pd.set_option("display.float_format", lambda x: "%.2f" % x)  # 设置浮点数格式

        print(df)

    except Exception as e:
        print(f"分析过程出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

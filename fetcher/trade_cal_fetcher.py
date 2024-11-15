import tushare as ts
import sqlite3
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH, TUSHARE_TOKEN
import argparse
from datetime import datetime


def init_db():
    """初始化数据库，创建交易日历表"""
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_calendar (
            exchange TEXT,
            cal_date TEXT,
            is_open INTEGER,
            pretrade_date TEXT,
            PRIMARY KEY (exchange, cal_date)
        )
    """
    )

    conn.commit()
    conn.close()


def fetch_and_save_calendar(start_date=None, end_date=None):
    """获取交易日历数据并保存到数据库"""
    # 初始化Tushare
    pro = ts.pro_api(TUSHARE_TOKEN)

    # 准备查询参数
    params = {
        "exchange": "",  # 默认获取所有交易所
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    print(f"正在获取交易日历数据... (start_date: {start_date}, end_date: {end_date})")

    # 获取数据
    df = pro.trade_cal(**params)

    if df.empty:
        raise Exception("No data retrieved for the specified date range")

    # 写入数据库
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql("trade_calendar", conn, if_exists="append", index=False)
    conn.close()

    return df


def validate_date(date_str):
    """验证日期格式是否正确"""
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y%m%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYYMMDD"
        )


def main():
    parser = argparse.ArgumentParser(description="交易日历数据获取工具")
    parser.add_argument(
        "--start-date", type=validate_date, help="开始日期 (格式: YYYYMMDD)"
    )
    parser.add_argument(
        "--end-date", type=validate_date, help="结束日期 (格式: YYYYMMDD)"
    )

    args = parser.parse_args()

    # 初始化数据库
    init_db()

    try:
        df = fetch_and_save_calendar(args.start_date, args.end_date)
        print(f"成功获取并保存交易日历数据")
        print("\n数据样例：")
        print(df.head())
    except Exception as e:
        print(f"获取数据失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

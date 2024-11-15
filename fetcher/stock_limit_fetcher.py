import tushare as ts
import sqlite3
import pandas as pd
import os
import sys
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH, TUSHARE_TOKEN


def init_db():
    """初始化数据库，创建涨跌停价格表"""
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_limits
        (ts_code TEXT,
         trade_date TEXT,
         pre_close REAL,
         up_limit REAL,
         down_limit REAL,
         PRIMARY KEY (ts_code, trade_date))
    """
    )

    conn.commit()
    conn.close()


def fetch_and_save_limits(trade_date):
    """获取指定日期的涨跌停价格数据并保存到数据库"""
    pro = ts.pro_api(TUSHARE_TOKEN)

    # 获取涨跌停数据
    df = pro.stk_limit(trade_date=trade_date)

    if df.empty:
        raise Exception("No data retrieved for the specified date")

    # 写入数据库
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql("stock_limits", conn, if_exists="append", index=False)
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
    parser = argparse.ArgumentParser(description="股票涨跌停价格数据获取工具")
    parser.add_argument(
        "--date", type=validate_date, help="指定获取数据的日期 (格式: YYYYMMDD)"
    )
    parser.add_argument(
        "--start-date", type=validate_date, help="开始日期 (格式: YYYYMMDD)"
    )
    parser.add_argument(
        "--end-date", type=validate_date, help="结束日期 (格式: YYYYMMDD)"
    )

    args = parser.parse_args()

    # 初始化数据库
    init_db()

    if args.date:
        # 获取单个日期的数据
        try:
            df = fetch_and_save_limits(args.date)
            print(f"成功获取并保存 {args.date} 的涨跌停数据")
            print("\n数据样例：")
            print(df.head())
        except Exception as e:
            print(f"获取数据失败: {str(e)}")

    elif args.start_date and args.end_date:
        # 获取日期范围内的数据
        start = datetime.strptime(args.start_date, "%Y%m%d")
        end = datetime.strptime(args.end_date, "%Y%m%d")

        current = start
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            try:
                df = fetch_and_save_limits(date_str)
                print(f"成功获取并保存 {date_str} 的涨跌停数据")
            except Exception as e:
                print(f"获取 {date_str} 的数据失败: {str(e)}")
            current += timedelta(days=1)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()

import tushare as ts
import sqlite3
import pandas as pd

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATABASE_PATH, TUSHARE_TOKEN
import argparse
from datetime import datetime, timedelta


def init_db():
    """初始化数据库，创建股票分钟数据表"""
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    c.execute(
        """CREATE TABLE IF NOT EXISTS minute_quotes
                 (ts_code TEXT,
                  trade_time TEXT,
                  freq TEXT,
                  open REAL,
                  high REAL,
                  low REAL,
                  close REAL,
                  amount REAL,
                  PRIMARY KEY (ts_code, trade_time, freq))"""
    )

    conn.commit()
    conn.close()


def fetch_and_save_minute_data(ts_code, start_date, end_date, freq="1min"):
    """获取指定时间范围的股票分钟数据并保存到数据库

    Args:
        ts_code: 股票代码
        start_date: 开始时间
        end_date: 结束时间
        freq: 数据频度，支持 1min/5min/15min/30min/60min
    """
    # 初始化Tushare
    pro = ts.pro_api(TUSHARE_TOKEN)

    # 获取数据
    df = ts.pro_bar(
        ts_code=ts_code, freq=freq, start_date=start_date, end_date=end_date
    )
    print(df.head)
    print(df.columns)

    if df is not None and not df.empty:
        # 添加频度列
        df["freq"] = freq

        # 写入数据库
        conn = sqlite3.connect(DATABASE_PATH)
        df.to_sql("minute_quotes", conn, if_exists="append", index=False)
        conn.close()

    return df


def validate_datetime(datetime_str):
    """验证日期时间格式是否正确"""
    try:
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime format: {datetime_str}. Use YYYY-MM-DD HH:MM:SS"
        )


def main():
    parser = argparse.ArgumentParser(description="股票分钟数据获取工具")
    parser.add_argument("--ts-code", required=True, help="股票代码 (格式: 600000.SH)")
    parser.add_argument(
        "--start-datetime",
        type=validate_datetime,
        required=True,
        help="开始日期时间 (格式: YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--end-datetime",
        type=validate_datetime,
        required=True,
        help="结束日期时间 (格式: YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--freq",
        choices=["1min", "5min", "15min", "30min", "60min"],
        default="1min",
        help="数据频度 (默认: 1min)",
    )

    args = parser.parse_args()

    # 初始化数据库
    init_db()

    try:
        df = fetch_and_save_minute_data(
            args.ts_code, args.start_datetime, args.end_datetime, args.freq
        )
        print(
            f"成功获取并保存 {args.ts_code} 从 {args.start_datetime} 到 {args.end_datetime} 的 {args.freq} 数据"
        )
        print("\n数据样例：")
        print(df.head())
    except Exception as e:
        print(f"获取数据失败: {str(e)}")


if __name__ == "__main__":
    main()

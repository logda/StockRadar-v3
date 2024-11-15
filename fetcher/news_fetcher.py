"""
python ./fetcher/news_fetcher.py --start-datetime "2024-11-13 9:00:00" --end-datetime "2024-11-14 9:00:00"
"""

import tushare as ts
import sqlite3
import pandas as pd

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATABASE_PATH, TUSHARE_TOKEN
import argparse
from datetime import datetime


def init_news_db():
    """初始化数据库，创建新闻数据表"""
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    c.execute(
        """CREATE TABLE IF NOT EXISTS news
                 (datetime TEXT,
                  content TEXT,
                  title TEXT,
                  channels TEXT,
                  PRIMARY KEY (datetime, title))"""
    )

    conn.commit()
    conn.close()


def fetch_and_save_news(start_date, end_date):
    """获取指定日期范围的新闻数据并保存到数据库"""
    pro = ts.pro_api(TUSHARE_TOKEN)
    df = pro.news(src="sina", start_date=start_date, end_date=end_date)

    if df.empty:
        return df

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 使用 INSERT OR IGNORE 忽略重复数据
    insert_sql = """
        INSERT OR IGNORE INTO news (datetime, content, title, channels)
        VALUES (?, ?, ?, ?)
    """

    data = df[["datetime", "content", "title", "channels"]].values.tolist()
    cursor.executemany(insert_sql, data)

    conn.commit()
    conn.close()

    return df


# def fetch_and_save_news(start_date, end_date):
#     """获取指定日期范围的新闻数据并保存到数据库"""
#     # 初始化Tushare
#     pro = ts.pro_api(TUSHARE_TOKEN)

#     # 获取新闻数据
#     df = pro.news(src="sina", start_date=start_date, end_date=end_date)

#     # 只查询当前时间范围内可能重复的数据
#     conn = sqlite3.connect(DATABASE_PATH)
#     query = """
#         SELECT datetime, title
#         FROM news
#         WHERE datetime BETWEEN ? AND ?
#     """
#     existing = pd.read_sql(query, conn, params=[start_date, end_date])

#     # 在内存中进行去重
#     df = df.merge(existing, on=["datetime", "title"], how="left", indicator=True)
#     df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

#     # 写入数据库
#     if not df.empty:
#         df.to_sql("news", conn, if_exists="append", index=False)
#     conn.close()

#     return df


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
    parser = argparse.ArgumentParser(description="新闻快讯数据获取工具")
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

    args = parser.parse_args()

    # 初始化数据库
    init_news_db()

    # 获取新闻数据
    try:
        df = fetch_and_save_news(args.start_datetime, args.end_datetime)
        print(
            f"成功获取并保存从 {args.start_datetime} 到 {args.end_datetime} 的新闻数据"
        )
        print("\n数据样例：")
        print(df.head())
    except Exception as e:
        print(f"获取新闻数据失败: {str(e)}")


if __name__ == "__main__":
    main()

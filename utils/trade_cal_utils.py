import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH
from fetcher.trade_cal_fetcher import fetch_and_save_calendar


def check_calendar_exists(start_date, end_date):
    """检查指定日期范围的交易日历数据是否存在"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 检查日期范围内是否有数据
    cursor.execute(
        """
        SELECT COUNT(*) 
        FROM trade_calendar 
        WHERE cal_date BETWEEN ? AND ?
    """,
        (start_date, end_date),
    )

    count = cursor.fetchone()[0]
    conn.close()
    # print(count)
    # 计算日期范围内的天数
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    days_between = (end - start).days + 1

    return count >= days_between  # 确保范围内所有日期都有数据


def ensure_calendar_data(start_date, end_date):
    """确保指定日期范围的交易日历数据存在"""
    if not check_calendar_exists(start_date, end_date):
        try:
            fetch_and_save_calendar(start_date, end_date)
        except Exception as e:
            raise Exception(f"获取交易日历数据失败: {str(e)}")


def is_trade_day(date_str):
    """判断指定日期是否为交易日

    Args:
        date_str: 日期字符串，格式为YYYYMMDD

    Returns:
        bool: 是否为交易日
    """
    # 确保数据存在
    ensure_calendar_data(date_str, date_str)

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 查询指定日期是否为交易日
    cursor.execute(
        """
        SELECT is_open 
        FROM trade_calendar 
        WHERE cal_date = ? 
        AND exchange = 'SSE'  -- 使用上交所交易日历作为标准
        """,
        (date_str,),
    )

    result = cursor.fetchone()
    conn.close()

    return result[0] == 1 if result else False


def get_previous_trade_day(date_str):
    """获取指定日期的上一个交易日
    Get the previous trading day before the specified date.

    Args:
        date_str (str): 日期字符串，格式为YYYYMMDD

    Returns:
        str: 上一个交易日的日期字符串，如果没有找到则返回None
    """
    # 计算可能的最早日期（往前推10个自然日）
    start_date = (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=365)).strftime(
        "%Y%m%d"
    )

    # 确保数据存在
    ensure_calendar_data(start_date, date_str)

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 查询前一个交易日
    cursor.execute(
        """
        SELECT MAX(cal_date) 
        FROM trade_calendar 
        WHERE exchange = 'SSE' 
        AND is_open = 1 
        AND cal_date < ?
        """,
        (date_str,),
    )

    result = cursor.fetchone()
    conn.close()

    return result[0] if result else None


def get_next_trade_day(date_str):
    """获取指定日期的下一个交易日

    Args:
        date_str: 日期字符串，格式为YYYYMMDD

    Returns:
        str: 下一个交易日的日期字符串
    """
    # 计算可能的最晚日期（往后推5个自然日）
    end_date = (datetime.strptime(date_str, "%Y%m%d") + timedelta(days=5)).strftime(
        "%Y%m%d"
    )

    # 确保数据存在
    ensure_calendar_data(date_str, end_date)

    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # 查询下一个交易日
    cursor.execute(
        """
        SELECT MIN(cal_date) 
        FROM trade_calendar 
        WHERE exchange = 'SSE' 
        AND is_open = 1 
        AND cal_date > ?
        """,
        (date_str,),
    )

    result = cursor.fetchone()
    conn.close()

    return result[0] if result else None


def get_previous_n_trade_days(date_str, n):
    """获取指定日期前N个交易日的日期列表
    Get a list of exactly N trading days before the specified date, skipping non-trading days.

    Args:
        date_str (str): 日期字符串，格式为YYYYMMDD
        n (int): 需要获取的交易日数量

    Returns:
        list: 交易日期列表，按日期降序排列
    """
    trade_days = []
    current_date = date_str

    # 循环获取前N个交易日
    for _ in range(n):
        prev_trade_day = get_previous_trade_day(current_date)
        if prev_trade_day is None:
            break
        trade_days.append(prev_trade_day)
        current_date = prev_trade_day

    return trade_days


def main():
    """测试交易日历工具的各项功能"""
    import argparse
    from datetime import datetime, timedelta

    # 设置命令行参数
    parser = argparse.ArgumentParser(description="交易日历工具测试程序")
    parser.add_argument(
        "--date",
        help="指定日期 (格式: YYYYMMDD)",
        default=datetime.now().strftime("%Y%m%d"),
    )
    parser.add_argument(
        "--days", type=int, default=5, help="回溯的交易日天数 (默认: 5)"
    )

    args = parser.parse_args()
    test_date = args.date

    print("\n=== 交易日历工具测试 ===")
    print(f"测试日期: {test_date}")

    # 测试1: 判断是否为交易日
    is_trade = is_trade_day(test_date)
    print(f"\n1. 交易日判断:")
    print(f"   {test_date} {'是' if is_trade else '不是'}交易日")

    # 测试2: 获取上一个交易日
    prev_day = get_previous_trade_day(test_date)
    print(f"\n2. 上一个交易日:")
    print(f"   {test_date} 的上一个交易日是: {prev_day}")

    # 测试3: 获取下一个交易日
    next_day = get_next_trade_day(test_date)
    print(f"\n3. 下一个交易日:")
    print(f"   {test_date} 的下一个交易日是: {next_day}")

    # 测试4: 获取前N个交易日
    n = args.days
    prev_n_days = get_previous_n_trade_days(test_date, n)
    print(f"\n4. 前{n}个交易日:")
    print(prev_n_days)

    print("\n=== 测试完成 ===")


if __name__ == "__main__":
    main()

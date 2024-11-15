import sqlite3
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH
from fetcher.stock_basic_fetcher import fetch_and_save_basic_info


def check_stock_basic_exists():
    """检查stock_basic表是否存在数据"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_basic")
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def analyze_market_distribution():
    """分析市场分布情况"""
    conn = sqlite3.connect(DATABASE_PATH)

    # 查询市场分布
    query = """
    SELECT 
        market,
        COUNT(*) as stock_count
    FROM stock_basic
    GROUP BY market
    ORDER BY stock_count DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # 重命名列
    df = df.rename(columns={"market": "市场类型", "stock_count": "股票数量"})

    return df


def main():
    # 检查并初始化stock_basic数据
    if not check_stock_basic_exists():
        print("stock_basic表中没有数据，正在获取并保存股票基础信息...")
        try:
            fetch_and_save_basic_info()
            print("成功获取并保存股票基础信息")
        except Exception as e:
            print(f"获取股票基础信息失败: {str(e)}")
            sys.exit(1)

    # 获取市场分布数据
    try:
        df = analyze_market_distribution()

        print("\n市场分布统计:")

        # 设置pandas显示选项
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)

        print(df)

    except Exception as e:
        print(f"分析过程出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

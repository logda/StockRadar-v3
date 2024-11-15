import tushare as ts
import sqlite3
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH, TUSHARE_TOKEN


def init_db():
    """初始化数据库，创建股票基础信息表"""
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_basic (
            ts_code TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            area TEXT,
            industry TEXT,
            cnspell TEXT,
            market TEXT,
            list_date TEXT,
            act_name TEXT,
            act_ent_type TEXT,
            enname TEXT,
            fullname TEXT,
            exchange TEXT,
            curr_type TEXT,
            list_status TEXT,
            delist_date TEXT,
            is_hs TEXT
        )
    """
    )

    conn.commit()
    conn.close()


def fetch_and_save_basic_info():
    """获取股票基础信息并保存到数据库"""
    # 初始化Tushare
    pro = ts.pro_api(TUSHARE_TOKEN)

    # 获取数据
    df = pro.stock_basic(
        fields=[
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "cnspell",
            "market",
            "list_date",
            "act_name",
            "act_ent_type",
            "enname",
            "fullname",
            "exchange",
            "curr_type",
            "list_status",
            "delist_date",
            "is_hs",
        ]
    )

    # 写入数据库
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql("stock_basic", conn, if_exists="replace", index=False)
    conn.close()

    return df


def main():
    # 初始化数据库
    init_db()

    try:
        # 获取并保存数据
        df = fetch_and_save_basic_info()
        print("成功获取并保存股票基础信息")
        print("\n数据样例：")
        print(df.head())
    except Exception as e:
        print(f"获取股票基础信息失败: {str(e)}")


if __name__ == "__main__":
    main()

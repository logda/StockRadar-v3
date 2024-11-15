import tushare as ts
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import TUSHARE_TOKEN


def check_token_status():
    """查询Tushare账户状态，包括积分和过期时间"""
    try:
        # 初始化API
        pro = ts.pro_api()

        # 查询用户信息
        df = pro.user(token=TUSHARE_TOKEN)
        print(df)

    except Exception as e:
        print(f"查询失败: {str(e)}")
        return None


if __name__ == "__main__":
    check_token_status()

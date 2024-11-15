import tushare as ts
from config import TUSHARE_TOKEN

pro = ts.pro_api(TUSHARE_TOKEN)

print(dir(pro))
print(dir(ts))

import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB

DEV_MYSQL = {
    'host': '114.80.222.242',
    'port': 13306,
    'user': 'dev_liuhuan',
    'password': 'weQ60jf4y0(qEd6W4xG^',
    'db': 'wind',
    'charset': 'utf8'
}

dev_pool = PooledDB(pymysql, 2, **DEV_MYSQL)
conn = dev_pool.connection()
table = 'aequfropleinforepperend'
OBJECT_ID = '{00004606-49F3-11E8-ADE7-8242D9248611}'
sql = f"select * from {table} where OBJECT_ID = '{OBJECT_ID}';"

df = pd.read_sql_query(sql, conn)
print(df)
conn.close()

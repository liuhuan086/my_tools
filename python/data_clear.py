import datetime
import os
import multiprocessing
import pymysql
import pandas as pd
from DBUtils.PooledDB import PooledDB

from python.date import convert_to_datetime
from concurrent.futures import ThreadPoolExecutor

FACTOR_PATH = r'/data/test'


class MysqlAPI(object):
    def __init__(self, mysql_conf):
        """
        :param mysql_conf: mysql config, eg:

        mysql_conf = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'test',
            'password': 'test',
            'db': 'test',
            'charset': 'utf8'
        }
        """
        self.pool = PooledDB(pymysql, 4, **mysql_conf)

    def insert_data_in_bulk(self, dataframe: pd.DataFrame, table_name):
        """
        Insert the data in the dataframe into the database in batches.
        :param dataframe: pd.DataFrame
        :param table_name: the table you want insert to.
        :return:
        """
        conn = self.pool.connection()
        cursor = conn.cursor()

        keys = dataframe.keys()
        values = dataframe.values.tolist()

        key_sql = ', '.join(keys)
        value_sql = ', '.join(['%s'] * dataframe.shape[1])
        insert_data_str = """insert into %s (%s) values (%s)""" % (table_name, key_sql, value_sql)

        try:
            s_num = 0
            e_num = 1000000
            a_num = 1000000
            l_num = len(dataframe)

            while True:
                if e_num > l_num:
                    value = values[s_num:l_num]
                    cursor.executemany(insert_data_str, value)
                    conn.commit()
                    print('%s, insert into %-30s, length is %s' % (datetime.datetime.now(), table_name, len(dataframe)))
                    break

                value = values[s_num:e_num]
                cursor.executemany(insert_data_str, value)
                conn.commit()
                print('%s, insert into %-30s,  length is %s' % (datetime.datetime.now(), table_name, len(dataframe)))

                s_num += a_num
                e_num += a_num

        except Exception as e:
            print(f'Error, {datetime.datetime.now()}, {table_name}, {e}.')
            # sys.exit(-1)

        cursor.close()
        conn.close()

    def _query_factor(self, table_name, start_time, end_time):
        """
        :param table_name: The table you want insert to.
        :param start_time: The start date.
        :param end_time: The end date.
        :return: dataframe
        """
        try:
            s_time = convert_to_datetime(start_time)
            e_time = convert_to_datetime(end_time)

            str_e_time = e_time.strftime("%Y-%m-%d %H:%M:%S")
            int_s_time = int(s_time.strftime("%Y%m%d%H%M%S"))

            file_path = os.path.join(FACTOR_PATH, table_name)
            file_name = os.listdir(file_path)[0]
            table_time = int(file_name.split('.')[0])
            str_table_time = str(datetime.datetime.strptime(str(table_time), "%Y%m%d%H%M%S"))

            table_path = os.path.join(file_path, file_name)

            if not os.path.exists(table_path):
                print('Error, table is not exists.')
                return pd.DataFrame()

            df_par = pd.read_parquet(table_path)

            if df_par.index.get_level_values(0)[0] > pd.Timestamp(s_time):
                print('Error, the start time is too early.')
                return pd.DataFrame()

            if int_s_time <= table_time:
                df_par = df_par.loc[(df_par.index.get_level_values(0) >= pd.Timestamp(s_time)) &
                                    (df_par.index.get_level_values(0) <= pd.Timestamp(e_time))]
                return df_par

            df_par = df_par[pd.Timestamp(s_time) <= df_par.index.get_level_values(0)]
            sql = f"select * from {table_name} where trade_time > '{str_table_time}' and trade_time <= '{str_e_time}';"
            conn = self.pool.connection()

            df_db = pd.read_sql(sql, conn).set_index(['trading_time', 'symbol'])
            conn.close()
            df_concat = pd.concat([df_par, df_db])

        except Exception as e:
            print(f'Error, details is {e}.')
            df_concat = pd.DataFrame()

        return df_concat

    def query_factors(self, table_names, start_time, end_time):
        """
        :param table_names: The tables you want to query.
        :param start_time: The start time you want to query.
        :param end_time: The end time you want to query.
        :return: List of dataframe.
        """
        if isinstance(table_names, str):
            return self._query_factor(table_names, start_time, end_time)

        elif isinstance(table_names, list):
            thread_pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())

            results = []
            for i in table_names:
                task = thread_pool.submit(self._query_factor, i, start_time, end_time)
                results.append(task.result())

            thread_pool.shutdown()
            return results
        else:
            print(f"Error, table_names must be str or list.")
            return pd.DataFrame()

    def get_tables(self, status=None):
        """
        :param status: The status of table.
        :return: The list of all tables in the database.
        """
        conn = self.pool.connection()
        curs = conn.cursor()
        if status is not None:
            sql = f"select table_name from table_info where status='{status}';"
        else:
            sql = "select table_name from table_info;"

        tables = []
        try:
            curs.execute(sql)
            result = curs.fetchall()
            tables = [table[0] for table in result if table[0] != 'table_info']
        except Exception as e:
            print(e)

        conn.close()
        return tables

    def generate_cache_data(self):
        """
        Query data from MySQL regularly and save it in parquet file format, the parquet file.
        The parquet file is named after the latest trade time.
        The save path of the file is named after the table name.
        eg:
            ./factor_one/20210901.par
            ./factor_two/20210901.par
            ./factor_six/20210101.par
            ......
        """
        conn = self.pool.connection()
        tables = self.get_tables()

        if not tables:
            return

        for table in tables:
            sql = f"""
            SELECT * FROM {table} ORDER BY trade_time ASC;"""
            df = pd.read_sql(sql, conn)
            last_time = df['trading_time'].iloc[-1]

            df = df.set_index(['trading_time', 'symbol'])
            path = os.path.join(FACTOR_PATH, table)
            int_last_time = int(datetime.datetime.strftime(last_time, '%Y%m%d%H%M%S'))

            if not os.path.exists(path):
                os.makedirs(path)

            if os.listdir(path):
                os.system(f'cd {path} && rm *.par')

            par_path = os.path.join(path, str(int_last_time)) + '.par'
            print(par_path)
            df.to_parquet(par_path)

        conn.close()

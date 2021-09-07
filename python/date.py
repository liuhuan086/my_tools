import datetime

import pandas as pd


def convert_to_datetime(query_date):
    """
    standardize input query_date to datetime.datetime
    :param query_date: query date
    :return: datetime.datetime
    """
    if isinstance(query_date, datetime.date) and not isinstance(query_date, datetime.datetime):
        return datetime.datetime.fromordinal(query_date.toordinal())
    elif isinstance(query_date, str):
        return pd.Timestamp(query_date).to_pydatetime()
    elif isinstance(query_date, datetime.datetime):
        return query_date
    else:
        if len(str(query_date)) == 8:
            return datetime.datetime.strptime(str(query_date), '%Y%m%d')
        else:
            return datetime.datetime.strptime(str(query_date), '%Y%m%d%H%M%S')

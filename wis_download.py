import sys
import time
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime as dt
from datetime import date
from calendar import monthrange

# wis_download.py id start_time end_time


# for instance: turn 2012-12-31 24:00 into 2013-01-01 00:00
def dt_parse(ser):
    # note for self: need to check dtype of input
    _date = str(ser.iloc[0])
    _time = str(ser.iloc[1])
    if _time != '24:00':
        return dt.datetime.strptime(_date+'/'+_time, '%Y/%m/%d/%H:%M')
        # dt.datetime(year, month, day, hour=int(_time))
    else:
        return dt.datetime.strptime(_date, '%Y/%m/%d') + dt.timedelta(days=1)


def main():
    args = sys.argv
    _id = args[1]
    bgn_time = dt.datetime.strptime(args[2], '%Y%m%d')
    end_time = dt.datetime.strptime(args[3], '%Y%m%d')
    df_tables = pd.DataFrame()
    current_time = bgn_time
    missing_rows = []
    # dt_index = current_time + pd.timedelta_range(start='1 hour', end='', freq='H')

    while current_time <= end_time:
        cur_year = current_time.year
        cur_month = current_time.month

        bgn = date(cur_year, cur_month, 1).strftime("%Y%m%d")
        end = date(cur_year, cur_month, monthrange(cur_year, cur_month)[1]).strftime("%Y%m%d")

        # generated index for the month
        dt_index = dt.datetime(cur_year, cur_month, 1) + pd.timedelta_range(start='1 hour', periods=(24*monthrange(cur_year, cur_month)[1]), freq='H')

        source = f"http://www1.river.go.jp/cgi-bin/DspDamData.exe?KIND=1&ID={_id}&BGNDATE={bgn}&ENDDATE={end}&KAWABOU=NO"
        soup = bs(requests.get(source).content, "lxml")
        link = "http://www1.river.go.jp" + soup.find('iframe').get('src')
        print(link)
        df = pd.read_html(link)[0]

        ser_dt = df.iloc[:, 0:2].apply(dt_parse, axis=1)
        delta_as_series = pd.Series(dt_index)

        df.insert(0, 'dt_index', ser_dt)
        df.set_index('dt_index', inplace=True)

        # check missing entries
        if not ser_dt.equals(delta_as_series):
            missing_rows.append(f'{cur_year}.{cur_month}')

        # reindex with real dt in case of missing
        df = df.reindex(dt_index)
        df_tables = pd.concat([df_tables, df], ignore_index=False)
        time.sleep(.1)

        # for next month
        current_time = dt.datetime(cur_year, cur_month, monthrange(cur_year, cur_month)[1]) + dt.timedelta(days=1)

    # information of missing
    if len(missing_rows) != 0:
        print(f"data missing in months\n{missing_rows}")

    os.makedirs('wis', exist_ok=True)
    df_tables.to_csv(f'wis/{_id}_{bgn_time}_{end_time}.csv')


if __name__ == "__main__":
    main()

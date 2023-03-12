import os
import sys
import time
import datetime as dt
import pandas as pd
from calendar import monthrange


# jma_download.py station_id start_year end_year


# for instance: turn 2012-12-31 24:00 into 2013-01-01 00:00
def dt_parse(_time, year, month, day):
    if _time != 24:
        return dt.datetime(year, month, day, hour=_time)
    else:
        return dt.datetime(year, month, day, hour=0) + dt.timedelta(days=1)


class Loader:
    def __init__(self, station_id, bgn_year, end_year):
        self.id = station_id
        self.bgn = bgn_year
        self.end = end_year
        self.sign = 'a' if int(station_id) < 10000 else 's'

    def save(self):
        df = pd.DataFrame()
        # ser_dt = pd.Series()
        check_dt = dt.datetime(self.bgn, 1, 1) + pd.timedelta_range(start='1 hour', periods=24, freq='H')
        missing_rows = []
        for year in range(self.bgn, self.end + 1):
            for month in range(1, 13):
                for day in range(1, monthrange(year, month)[1] + 1):
                    link = f"http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_{self.sign}1.php?prec_no=43&block_no={self.id}&year={year}&month={month:02}&day={day:02}&view="
                    df_con = pd.read_html(link)[0]  # df of the day, len: 24

                    dt_daily = pd.Series(df_con.iloc[:, 0]).copy().apply(dt_parse,
                                                                         args=(year, month, day))  # dt of the day

                    # indexing with datetime
                    df_con.insert(0, 'dt_index', dt_daily)
                    df_con.set_index('dt_index', inplace=True)

                    # ser_dt = pd.concat([ser_dt, dt_daily])  # full dt if necessary
                    # generate dt series to check if the daily data is missing entries
                    delta_as_series = pd.Series(check_dt)

                    if not dt_daily.equals(delta_as_series):
                        print('missing')
                        missing_rows.append(f'{year}{month:02}{day:02}')

                    # reindex with true datetime index in case of missing rows
                    df_con.reindex(check_dt)
                    # dt index for next
                    check_dt = check_dt + dt.timedelta(days=1)

                    df = pd.concat([df, df_con], ignore_index=False)
                    print(link)
                    time.sleep(1)

        # information of missing entries
        if len(missing_rows) == 0:
            print("no missing datetime in the table")
        else:
            print(f"data in following dates are missing\n{missing_rows}")

        os.makedirs('jma', exist_ok=True)
        df.to_csv(f'jma/{self.id}.csv')


def main():
    args = sys.argv
    _id = args[1]
    bgn_year = int(args[2])
    end_year = int(args[3])
    Loader(_id, bgn_year, end_year).save()


if __name__ == '__main__':
    main()

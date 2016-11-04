import os
from datetime import timedelta
import pandas as pd
import pandas_datareader.data as web


class PriceFetcher(object):
    def __init__(self, ticker, start_date, end_date, price_folder):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.price_folder = price_folder

    def get_price(self):
        price = self._get_yahoo_price()

        file_path = self._get_ticker_filename()
        if os.path.exists(file_path):
            old_price = pd.read_csv(file_path, index_col=0, parse_dates=True)
            s = str(self.start_date)
            old_price = old_price[old_price.index < pd.datetime(year=int(s[0:4]), month=int(s[4:6]), day=int(s[6:8]))]

        return pd.concat([old_price, price])

    def save_price(self, price_df):
        price_df.to_csv(self._get_ticker_filename())

    def get_request_date_range(self):
        file_path = self._get_ticker_filename()
        if os.path.exists(file_path):
            price_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            last_date_d = price_df.index.tolist()[-1].date()
            last_date = int(last_date_d.strftime('%Y%m%d'))

            if last_date > self.end_date:
                self.end_date = last_date
            else:
                self.start_date = int((last_date_d + timedelta(days=1)).strftime('%Y%m%d'))

    def _get_ticker_filename(self):
        return os.path.join(self.price_folder, self.ticker, '.csv')

    def _get_yahoo_price(self):
        return web.get_data_yahoo(self.ticker, self.start_date, self.end_date)

    def _get_google_price(self):
        return web.get_data_google(self.ticker, self.start_date, self.end_date)

    def _get_google_yahoo_diff(self, yahoo_price, google_price):
        column_names = ['Open', 'High', 'Low', 'Close']
        diff_price = yahoo_price[column_names] - google_price[column_names]
        gap_price = diff_price[(diff_price>0.02).any(axis=1)]
        return {
                    'yahoo_price': yahoo_price.ix[gap_price.index],
                    'google_price': google_price.ix[gap_price.index],
                    'gap_price': gap_price.ix[gap_price.index]
                }


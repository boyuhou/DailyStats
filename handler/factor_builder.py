import os
import pandas as pd
import numpy as np
from handler.util import Helper

MIN_LOOKBACK_DAYS = 30


class FactorBuilder(object):
    def __init__(self, ticker, factor_folder, price_folder):
        self.price_df = pd.read_csv(Helper.get_ticker_filename(price_folder, ticker), index_col=0, parse_dates=True)
        self.factor_path = Helper.get_ticker_filename(factor_folder, ticker)
        self.factor_df = self._init_factor()

    def build_factors(self):
        process_dates = self._get_todo_dates()
        self._append_prices(process_dates)
        for process_date in process_dates:
            self._build_single_day_factor(process_date)

    def save_factors(self):
        self.factor_df.to_csv(self.factor_path)

    def _init_factor(self):
        if os.path.exists(self.factor_path):
            return pd.read_csv(self.factor_path, index_col=0, parse_dates=True)
        else:
            empty_df = self.price_df[self.price_df['Close'] < -1]
            empty_df['AvgRange'] = np.nan
            empty_df['FrogBox'] = np.nan
            return empty_df

    def _get_todo_dates(self):
        if os.path.exists(self.factor_path):
            last_processed_date = self.factor_df.index.tolist()[-1]
            return self.price_df.index[self.price_df.index > last_processed_date].tolist()
        else:
            date_list = self.price_df.index.tolist()
            return date_list[MIN_LOOKBACK_DAYS-1:]

    def _append_prices(self, process_dates):
        self.factor_df = self.factor_df.append(self.price_df.ix[self.price_df.index.isin(process_dates)])

    def _build_single_day_factor(self, process_date):
        self._build_frog_factor(process_date)

    def _build_frog_factor(self, process_date):
        lookback_range = 30
        index_loc = self.price_df.index.get_loc(process_date)
        index_range = self.price_df.index[index_loc+1-lookback_range:index_loc+1]
        s = self.price_df[self.price_df.index.isin(index_range)].copy()
        s['LagPrice'] = s['Close'].shift(1)
        s['SplitFactor'] = np.round(s['LagPrice'] / s['Close'])
        s.loc[s['SplitFactor'] == 1, 'SplitFactor'] = np.nan
        s.loc[:, 'SplitFactor'] = s['SplitFactor'].fillna(method='backfill').fillna(value=1)
        self.factor_df.loc[process_date, 'AvgRange'] = ((s['High'] - s['Low']) / s['SplitFactor']).mean()
        self.factor_df.loc[process_date, 'FrogBox'] = ((s['High'] - s['Low']) / s['SplitFactor']).std()
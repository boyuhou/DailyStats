import os
import pandas as pd
import numpy as np
from sklearn import linear_model
from handler.util import Helper

MIN_LOOKBACK_DAYS = 30
MIN_LOOKBACK_INTRADAY_PERIOD = 270

class DailyFactorBuilder(object):
    def __init__(self, ticker, factor_folder, price_folder):
        self.price_df = pd.read_csv(Helper.get_ticker_filename(price_folder, ticker), index_col=0, parse_dates=True)
        self.factor_path = Helper.get_ticker_filename(factor_folder, ticker)
        self.factor_df = self._init_factor()

    def build_daily_factors(self):
        process_dates = self._get_todo_dates()
        self._append_prices(process_dates)
        for process_date in process_dates:
            self._build_single_day_factor(process_date)

    def save_daily_factors(self):
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


class IntradayFactorBuilder(object):
    FACTOR_LIST = ['RegLine10', 'RegLine30', 'RegLine90', 'RegLine270']
    DATETIME_FORMAT = '%Y%m%d %H%M%S'
    REG_MODEL = linear_model.LinearRegression()

    def __init__(self, ticker, factor_folder, price_folder):
        self.price_df = pd.read_csv(Helper.get_ticker_filename(price_folder, ticker), index_col=0, parse_dates=True)
        self.factor_path = Helper.get_ticker_filename(factor_folder, ticker)
        self.factor_df = self._init_factor()


    def _get_todo_index_time(self):
        if os.path.exists(self.factor_path):
            last_processed_date = self.factor_df.index.tolist()[-1]
            return self.price_df.index[self.price_df.index > last_processed_date]
        else:
            return self.price_df.index

    def _get_raw_data(self, todo_index):
        if todo_index[0] == self.price_df.index[0]:
            return self.price_df.copy()
        index_loc = self.price_df.index.get_loc(todo_index[0])
        return self.price_df.iloc[(index_loc-MIN_LOOKBACK_INTRADAY_PERIOD):]

    def build_intraday_factor(self):
        process_time_index = self._get_todo_index_time()
        raw_data = self._get_raw_data(process_time_index)
        raw_data['RegLine10'] = raw_data['Close'].rolling(window=10).apply(IntradayFactorBuilder.linear_regression_value)
        raw_data['RegLine30'] = raw_data['Close'].rolling(window=30).apply(IntradayFactorBuilder.linear_regression_value)
        raw_data['RegLine90'] = raw_data['Close'].rolling(window=90).apply(IntradayFactorBuilder.linear_regression_value)
        raw_data['RegLine270'] = raw_data['Close'].rolling(window=270).apply(IntradayFactorBuilder.linear_regression_value)
        processed_df = raw_data.loc[raw_data.index.isin(process_time_index), IntradayFactorBuilder.FACTOR_LIST]
        self.factor_df = pd.concat([self.factor_df, processed_df])

    def save_intraday_factor(self):
        self.factor_df[IntradayFactorBuilder.FACTOR_LIST].to_csv(
            self.factor_path,
            date_format=IntradayFactorBuilder.DATETIME_FORMAT
        )

    def _init_factor(self):
        if os.path.exists(self.factor_path):
            return pd.read_csv(self.factor_path, index_col=0, parse_dates=True)
        else:
            empty_df = self.price_df[self.price_df['Close'] < -1]
            for factor in IntradayFactorBuilder.FACTOR_LIST:
                empty_df[factor] = np.nan
            return empty_df

    @staticmethod
    def linear_regression_value(data_list):
        data_size = data_list.size
        x_axis = np.array([range(data_list.size)]).T
        IntradayFactorBuilder.REG_MODEL.fit(x_axis, np.array(data_list))
        return IntradayFactorBuilder.REG_MODEL.predict(np.array(data_size))[0]
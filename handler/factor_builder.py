import os
import pandas as pd
import numpy as np
from sklearn import linear_model
from handler.util import Helper

MIN_LOOKBACK_DAYS = 30
MIN_LOOKBACK_INTRADAY_PERIOD = 270

class DailyFactorBuilder(object):
    def __init__(self, ticker, factor_folder, price_folder):
        self.price_df = pd.read_csv(Helper.get_filename(price_folder, ticker), index_col=0, parse_dates=True)
        self.factor_path = Helper.get_filename(factor_folder, ticker)
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
    FACTOR_LIST = ['BB1Up',
                   'BB1Down',
                   'BB2Up',
                   'BB2Down',
                   'BB3Up',
                   'BB3Down',
                   'BBMean',
                   'RegLine10',
                   'RegLine30',
                   'RegLine90',
                   'RegLine270']
    DATETIME_FORMAT = '%Y%m%d %H%M%S'
    REG_MODEL = linear_model.LinearRegression()

    def __init__(self, ticker, factor_folder, price_folder):
        self.price_df = pd.read_csv(Helper.get_filename(price_folder, ticker), index_col=0, parse_dates=True)
        self.ticker = ticker
        self.factor_folder = factor_folder

    def _get_todo_index_time(self, factor):
        factor_path = Helper.get_filename(self.factor_folder, self.ticker, factor)
        if os.path.exists(factor_path):
            factor_df = self._init_factor(factor)
            last_processed_date = factor_df.index.tolist()[-1]
            return self.price_df.index[self.price_df.index > last_processed_date]
        else:
            return self.price_df.index

    def _get_raw_data(self, todo_index):
        if todo_index[0] == self.price_df.index[0]:
            return self.price_df.copy()
        index_loc = self.price_df.index.get_loc(todo_index[0])
        return self.price_df.iloc[(index_loc-MIN_LOOKBACK_INTRADAY_PERIOD):]

    def _build_intraday_factor(self, factor):
        process_time_index = self._get_todo_index_time(factor)
        if len(process_time_index):
            raw_data = self._get_raw_data(process_time_index)
            raw_data = self._build(raw_data, factor)
            processed_df = raw_data.loc[raw_data.index.isin(process_time_index), [factor]]
            factor_df = self._init_factor(factor)
            return pd.concat([factor_df, processed_df])

    def build_intraday_factors(self):
        for factor in IntradayFactorBuilder.FACTOR_LIST:
            print('Building factor: ' + factor)
            factor_df = self._build_intraday_factor(factor)
            if factor_df is None:
                continue
            factor_path = Helper.get_filename(self.factor_folder, self.ticker, factor)
            factor_df.to_csv(
                factor_path,
                date_format=IntradayFactorBuilder.DATETIME_FORMAT
            )

    def _build(self, raw_data, factor):
        if factor == 'RegLine10':
            raw_data['RegLine10'] = raw_data['Close'].rolling(window=10).apply(
                IntradayFactorBuilder.linear_regression_value)
        elif factor == 'RegLine30':
            raw_data['RegLine30'] = raw_data['Close'].rolling(window=30).apply(
                IntradayFactorBuilder.linear_regression_value)
        elif factor == 'RegLine90':
            raw_data['RegLine90'] = raw_data['Close'].rolling(window=90).apply(
                IntradayFactorBuilder.linear_regression_value)
        elif factor == 'RegLine270':
            raw_data['RegLine270'] = raw_data['Close'].rolling(window=270).apply(
                IntradayFactorBuilder.linear_regression_value)
        elif factor == 'BB1Up':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=1)
            raw_data['BB1Up'] = raw_data['BBUp']
        elif factor == 'BB1Down':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=1)
            raw_data['BB1Down'] = raw_data['BBDown']
        elif factor == 'BB2Up':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=2)
            raw_data['BB2Up'] = raw_data['BBUp']
        elif factor == 'BB2Down':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=2)
            raw_data['BB2Down'] = raw_data['BBDown']
        elif factor == 'BB3Up':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=3)
            raw_data['BB3Up'] = raw_data['BBUp']
        elif factor == 'BB3Down':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=3)
            raw_data['BB3Down'] = raw_data['BBDown']
        elif factor == 'BBMean':
            raw_data = IntradayFactorBuilder.bollinger_bands(raw_data, period=30, n_std=1)
        elif factor == 'DragonUp':
            pass
        elif factor == 'DragonDown':
            pass
        elif factor == 'DragonMean':
            pass
        else:
            pass
        return raw_data

    def _init_factor(self, factor):
        factor_path = Helper.get_filename(self.factor_folder, self.ticker, factor)
        if os.path.exists(factor_path):
            return pd.read_csv(factor_path, index_col=0, parse_dates=True)
        else:
            empty_df = self.price_df[self.price_df['Close'] < -1]
            empty_df[factor] = np.nan
            return empty_df[[factor]]

    @staticmethod
    def linear_regression_value(data_list):
        data_size = data_list.size
        x_axis = np.array([range(data_list.size)]).T
        IntradayFactorBuilder.REG_MODEL.fit(x_axis, np.array(data_list))
        return IntradayFactorBuilder.REG_MODEL.predict(np.array(data_size))[0]

    @staticmethod
    def bollinger_bands(df, period=30, n_std=2):
        df['BBMean'] = df['Close'].rolling(period).mean()
        df['BBStd'] = df['Close'].rolling(period).std()
        df['BBUp'] = df['BBMean'] + n_std * df['BBStd']
        df['BBDown'] = df['BBMean'] - n_std * df['BBStd']
        return df

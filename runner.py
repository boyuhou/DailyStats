import argparse
import datetime
import pandas as pd
import json
from handler.price_fetcher import PriceFetcher
from handler.factor_builder import DailyFactorBuilder

if __name__ == '__main__':
    int_today = int(datetime.datetime.today().strftime('%Y%m%d'))
    parser = argparse.ArgumentParser(description='Daily Report Generator')
    parser.add_argument('type', metavar='S', type=str, nargs='?', default='price', help='Action (price, indicator) to '
                                                                                        'run')
    parser.add_argument('--start_date', type=int, default=int_today, help='start date')
    parser.add_argument('--end_date', type=int, default=int_today, help='end date')
    args = parser.parse_args()

    with open('config.json') as config_file:
        configs = json.load(config_file)
        etf_list = pd.read_csv(configs['ETF'], index_col=0).index.tolist()
        stock_list = pd.read_csv(configs['STOCK'], index_col=0).index.tolist()
    if args.type.upper() == 'PRICE':
        print('Processing ETF...')
        for etf in etf_list:
            print('Processing ETF: ' + etf)
            fetcher = PriceFetcher(etf, args.start_date, args.end_date, configs['EtfPriceFolder'])
            price_df = fetcher.get_price()
            fetcher.save_price(price_df)
        print('Processing STOCK...')
        for stock in stock_list:
            print('Processing STOCK: ' + stock)
            fetcher = PriceFetcher(stock, args.start_date, args.end_date, configs['StockPriceFolder'])
            price_df = fetcher.get_price()
            fetcher.save_price(price_df)
    elif args.type.upper() == 'BUILD':
        etf_list = pd.read_csv(configs['ETF'], index_col=0).index.tolist()
        stock_list = pd.read_csv(configs['STOCK'], index_col=0).index.tolist()
        print('Processing ETF...')
        for etf in etf_list:
            print('Processing ETF: ' + etf)
            builder = DailyFactorBuilder(etf, configs['EtfFactorFolder'], configs['EtfPriceFolder'])
            builder.build_daily_factors()
            builder.save_daily_factors()
        print('Processing STOCK...')
        for stock in stock_list:
            print('Processing STOCK: ' + stock)
            builder = DailyFactorBuilder(stock, configs['StockFactorFolder'], configs['StockPriceFolder'])
            builder.build_daily_factors()
            builder.save_daily_factors()
    else:
        print('Nothing to do....')



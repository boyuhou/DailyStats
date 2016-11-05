import argparse
import datetime
import pandas as pd
import json
from handler.price_fetcher import PriceFetcher

if __name__ == '__main__':
    int_today = int(datetime.datetime.today().strftime('%Y%m%d'))
    parser = argparse.ArgumentParser(description='Daily Report Generator')
    parser.add_argument('type', metavar='S', type=str, nargs='?', default='price', help='Action (price, indicator) to '
                                                                                        'run')
    parser.add_argument('--start_date', type=int, default=int_today, help='start date')
    parser.add_argument('--end_date', type=int, default=int_today, help='end date')
    args = parser.parse_args()

    if args.type.upper() == 'PRICE':
        with open('config.json') as config_file:
            configs = json.load(config_file)
        etf_list = pd.read_csv(configs['ETF'], index_col=0).index.tolist()
        stock_list = pd.read_csv(configs['STOCK'], index_col=0).index.tolist()

        for etf in etf_list:
            fetcher = PriceFetcher(etf, args.start_date, args.end_date, configs['EtfPriceFolder'])
            price_df = fetcher.get_price()
            fetcher.save_price(price_df)
        for stock in stock_list:
            fetcher = PriceFetcher(stock, args.start_date, args.end_date, configs['StockPriceFolder'])
            price_df = fetcher.get_price()
            fetcher.save_price(price_df)





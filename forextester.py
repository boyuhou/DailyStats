import click
import logging
import os
import sys
from datetime import datetime, timedelta
from handler.price_fetcher import ForexTesterPriceHandler

today = datetime.now().today()
today_str = today.strftime('%Y%m%d')
datetime_format = '%Y%m%d %H%M%S'
date_format = '%Y%m%d'


def get_instruments_from_file(filename):
    instruments = []
    with open(filename, 'r') as f:
        for instrument in f:
            instruments.append(instrument.rstrip())
    if len(instruments) > 0:
        instruments = instruments[1:]
    return instruments


@click.command()
@click.option('--ticker', default=None, help='Ticker Symbol')
@click.option('--outdir', default=r'C:\temp\1minute\forextester', help='Output folder')
@click.option('--start_date', default=today_str, help='Start date default to 20140101')
@click.option('--debug', default=False, help='True or False to introduce debug mode')
@click.option('--universe', default=None, help='The file that contains the universe')
@click.option('--indir', default=r'C:\temp\1minute\price', help='input folder')
def main(ticker, outdir, start_date, debug, universe, indir):
    log = logging.getLogger()
    log_console = logging.StreamHandler(sys.stdout)
    log.setLevel(logging.DEBUG if debug else logging.INFO)
    log_console.setLevel(logging.DEBUG if debug else logging.INFO)
    log.addHandler(log_console)

    if ticker is not None:
        instruments = (ticker, )
    elif universe is not None:
        instruments = get_instruments_from_file(universe)
    else:
        raise NotImplementedError('No ticker or universe is specified. Not sure what to do.')

    for (i, instrument) in enumerate(instruments):
        try:
            log.info(str.format("Processing {0} ({1} out of {2})", instrument, i+1, len(instruments)))
            input_path = os.path.join(indir, instrument+'.csv')
            output_path = os.path.join(outdir, instrument+'.csv')
            if not os.path.exists(input_path):
                raise Exception('Path Not Found. Check if the price data is :{0}'.format(input_path))

            ForexTesterPriceHandler.to_forex_file(input_path, output_path, start_date)
        except Exception as e:
            log.error('Exception during converting, continuing', exc_info=e)

if __name__ == '__main__':
    main()
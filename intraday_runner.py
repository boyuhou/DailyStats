import click
import logging
import sys
from handler.factor_builder import IntradayFactorBuilder
from datetime import datetime

today = datetime.now().today()
today_str = today.strftime('%Y%m%d')
eastern_tz = 'US/Eastern'
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
@click.option('--outdir', default=r'C:\temp\1minute\factor', help='Output folder')
@click.option('--indir', default=r'C:\temp\1minute\price', help='Output folder')
@click.option('--debug', default=False, help='True or False to introduce debug mode')
@click.option('--universe', default=None, help='The file that contains the universe')
def main(ticker, outdir, indir, debug, universe):
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

    log.info('Input Folder: {0}'.format(indir))
    log.info('Output Folder: {0}'.format(outdir))

    for (i, instrument) in enumerate(instruments):
        try:
            log.info(str.format("Processing {0} ({1} out of {2})", instrument, i + 1, len(instruments)))
            builder = IntradayFactorBuilder(instrument, outdir, indir)
            builder.build_intraday_factor()
            builder.save_intraday_factor()
        except Exception as e:
            log.error('Exception during calculating, continuing', exc_info=e)

if __name__ == '__main__':
    main()
import argparse
import datetime


def init():
    print 'hello world'


if __name__ == '__main__':
    int_today = int(datetime.datetime.today().strftime('%Y%m%d'))
    parser = argparse.ArgumentParser(description='Daily Report Generator')
    parser.add_argument('type', metavar='S', type=str, nargs='?', default='price', help='Action (price, indicator) to '
                                                                                        'run')
    parser.add_argument('--start_date', type=int, default=20150101, help='start date')
    parser.add_argument('--end_date', type=int, default=int_today, help='end date')


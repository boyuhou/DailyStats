import os


class Helper(object):
    def __init__(self):
        pass

    @staticmethod
    def get_ticker_filename(folder_name, ticker):
        return os.path.join(folder_name, ticker + '.csv')
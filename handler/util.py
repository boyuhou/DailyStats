import os


class Helper(object):
    def __init__(self):
        pass

    @staticmethod
    def get_filename(folder_name, ticker, factor=None):
        if factor is None:
            return os.path.join(folder_name, ticker + '.csv')
        return os.path.join(folder_name, ticker + '_' + factor + '.csv')
from src.main.python.triloq.habitual.mb.common.DF import DF
class DataGen():
    def __init__(self):
        pass

    def read_data(self, config):
        d = DF()
        df = d.read_data("transactionfile", config)
        return df






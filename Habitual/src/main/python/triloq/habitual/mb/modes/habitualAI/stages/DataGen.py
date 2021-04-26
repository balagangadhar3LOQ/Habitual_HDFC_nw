from src.main.python.triloq.habitual.mb.common.DF import DF
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.log_writer import write_log


class DataGen():
    def __init__(self):
        pass

    def read_txn_data(self, config):
        write_log("Reading Transaction File")
        d = DF()
        df = d.read_data("transactionfile", config)
        return df






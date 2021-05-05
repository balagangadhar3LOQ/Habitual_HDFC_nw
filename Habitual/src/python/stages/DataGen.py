from src.python.stages.DF import DF
from src.python.stages.log_writer import write_log
import warnings
warnings.filterwarnings(action="ignore")

class DataGen():
    def __init__(self):
        pass

    def read_txn_data(self, config):
        write_log("Reading Transaction File")
        d = DF()
        df = d.read_data("transactionfile", config)
        print(df.txn_dt.min(), df.txn_dt.max())
        return df






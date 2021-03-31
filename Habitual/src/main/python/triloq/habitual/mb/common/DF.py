import sys
import pandas as pd


class DF:
    def __init__(self):
        pass

    def read_data(self, file_cat: str, config):
        txn = None
        usr = None
        event = None
        read_arr = []
        file_type = config['input_file_type']
        if file_type not in config['valid_fileInfo']:
            sys.exit()
        if file_cat.lower() == "transactionfile":
            txn = config['fileInfo']['transactionFile']['name']
            txn_meta = config['fileInfo']['transactionFile']['txn_mapping']
            read_arr = list(txn_meta.values())

        if file_type.lower() == "csv":
            df = pd.read_csv(txn, usecols=read_arr)
        elif file_type.lower() == "parquet":
            df = pd.read_parquet(txn, engine="fastparquet")

        return df

    def write_csv(self, config):

        pass

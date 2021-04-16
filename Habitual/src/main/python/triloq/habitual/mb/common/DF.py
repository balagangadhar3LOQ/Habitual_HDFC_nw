import sys
import pandas as pd
from pathlib import Path
import datetime


class DF:
    def __init__(self):
        pass

    def rename_df(self, df: pd.DataFrame, rename_dict: dict):
        nw_rename = dict([(value, key) for key, value in rename_dict.items()])
        df.rename(columns=nw_rename, inplace=True)
        return df

    def read_data(self, file_cat: str, config):
        valid_filetypes = ["csv", "parquet"]
        print("[DF.py]  Running Read DF")
        txn = None
        usr = None
        event = None
        read_arr = []
        start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
        file_type = config['input_file_type']
        if file_type not in valid_filetypes:
            print(
                "File Type Error check file type {0}, {1}".format(file_type, config["valid_object"]['valid_fileInfo']))
            sys.exit()

        if file_cat.lower() == "transactionfile":
            txn = config['fileInfo']['transactionFile']['name']
            txn_meta = config['fileInfo']['transactionFile']['txn_mapping']
            read_arr = list(txn_meta.values())
        if file_type.lower() == "csv":
            df = pd.read_csv(txn, usecols=read_arr)
        elif file_type.lower() == "parquet":
            data_dir = Path(txn)
            try:
                df = pd.concat(pd.read_parquet(pf, engine="fastparquet") for pf in data_dir.glob('*.parquet'))
            except Exception as e:
                print("[DF.py]  Exception in reading Parquet files please check")
        rename_columns_dict = config['fileInfo']['transactionFile']['txn_mapping']

        df = self.rename_df(df, rename_columns_dict)
        df['txn_dt'] = pd.to_datetime(df['txn_dt'], format="%m/%d/%Y %H:%M:%S")
        # start_dt = datetime.datetime.strftime(start_dt, '%Y-%m-%d')
        # end_dt = datetime.datetime.strftime(end_dt, '%Y-%m-%d')
        df['txn_dt'] = df['txn_dt'].dt.strftime('%Y-%m-%d')
        print(df['txn_dt'].dtypes, type(start_dt))
        print(df.txn_dt.head())
        df['category'] = df['category'].str[-3:]
        df = df[df['txn_dt'] >= start_dt & df['txn_dt'] <= end_dt]
        return df

    def cat_mapping(self, df:pd.DataFrame, config):
        cat_mapping_file = config['category_mapping_file']
        if cat_mapping_file[-5:].lower() != ".json":
            return "NOT A JSON FILE INPUT VALID MAPPING FILE"
        map_file = pd.read_json(cat_mapping_file)
        return df

    def get_active_cat(self, df: pd.DataFrame, config):
        active_cat = config['active_categories']
        df = df[df['category'].isin(active_cat)]
        return df

    def clean_df(self, df):
        pass

    def write_csv(self, config):
        pass

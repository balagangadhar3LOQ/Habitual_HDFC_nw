import sys
import pandas as pd
from pathlib import Path
import datetime
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.log_writer import write_log


class DF:
    def __init__(self):
        pass

    def rename_df(self, df: pd.DataFrame, rename_dict: dict):
        write_log("renaming the dataset")
        nw_rename = dict([(value, key) for key, value in rename_dict.items()])
        df.rename(columns=nw_rename, inplace=True)
        df = df[list(nw_rename.values())]
        return df

    def read_data(self, file_cat: str, config):
        valid_filetypes = ["csv", "parquet"]
        write_log("[DF.py]  Running Read DF")
        txn = None
        usr = None
        event = None
        read_arr = []
        start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
        file_type = config['input_file_type']
        if file_type not in valid_filetypes:
            print("File Type Error Check Log")
            write_log(
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
                write_log("[DF.py]  Exception in reading Parquet files please check")
        rename_columns_dict = config['fileInfo']['transactionFile']['txn_mapping']

        df = self.rename_df(df, rename_columns_dict)

        df['txn_dt'] = pd.to_datetime(df['txn_dt'], format="%m/%d/%Y %H:%M:%S")
        df['txn_dt'] = df['txn_dt'].dt.strftime('%Y-%m-%d')

        df = df[df['txn_dt'] >= start_dt]
        df = df[df['txn_dt'] <= end_dt]

        df = df.dropna(subset=['txn_category', 'user_id', 'txn_dt'], axis=0)
        return df

    def cat_mapping(self, df: pd.DataFrame, config):
        cat_mapping_file = config['category_mapping_file']
        cat_map_col = config['category_map_col']
        if cat_mapping_file[-5:].lower() != ".json":
            return "NOT A JSON FILE INPUT VALID MAPPING FILE"
        map_file = pd.read_json(cat_mapping_file)
        df = df.merge(map_file[['event_id', cat_map_col]], on='event_id', how='left')
        df.drop(['event_id'], inplace=True)
        df.rename({cat_map_col: 'event_id'}, inplace=True)
        return df

    def get_active_cat(self, df: pd.DataFrame, config):
        active_cat = config['active_categories']
        df = df[df['event_id'].isin(active_cat)]
        return df

    def clean_df(self, df):
        pass

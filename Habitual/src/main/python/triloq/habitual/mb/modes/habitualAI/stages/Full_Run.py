import time

import pandas as pd

from src.main.python.triloq.habitual.mb.modes.habitualAI.stages import DataGen, Segment1, modelDataPrep
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.log_writer import write_log


class FullRun:
    def __init__(self):
        pass

    def write_data(self, df: pd.DataFrame, path, format):
        if format == 'csv':
            df.to_csv(path, index=False)
        elif format == "parquet":
            df.to_parquet(path, engine="fastparquet", compression='gzip', index=False)

    def execute(self, config):
        d = DataGen.DataGen()
        print("checking")
        df = d.read_txn_data(config)

        phase1 = Segment1.Phase1()
        df = phase1.execute_step1(df)
        phase1_path = config['file_output_path']['phase1_output']
        phase1_format = config['file_output_path']['phase1_output_format']
        self.write_data(df, phase1_path, phase1_format)
        write_log("Saved Segment data")

        time.sleep(20)
        tempdf_final = modelDataPrep.model_data_prep(df, config)
        for i in tempdf_final:
            stage3_path = config['file_output_path']['stage3_output']
            stage3_format = config['file_output_path']['stage3_output_format']
            write_log("<----------df---------->\n", df.head(2))
            # self.write_data(tempdf, stage3_path, stage3_format)
            write_log("Saved Model Data Preparation data")
        return True

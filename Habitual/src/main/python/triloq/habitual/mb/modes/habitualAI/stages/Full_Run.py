import pandas as pd

from src.main.python.triloq.habitual.mb.modes.habitualAI.stages import DataGen, Phase1, stage3


class FullRun:
    def __init__(self):
        pass

    def write_data(self, df: pd.DataFrame, path, format):
        if format == 'csv':
            df.to_csv(path, index=False)
        elif format == "parquet":
            df.to_parquet(path, index=False)

    def execute(self, config):
        d = DataGen.DataGen()
        print("checking")
        df = d.read_data(config)

        phase1 = Phase1()
        df = phase1.execute_step1(df)
        phase1_path = config['file_output_path']['phase1_output']
        phase1_format = config['file_output_path']['phase1_output_format']
        self.write_parquet(df, phase1_path, phase1_format)

        df = stage3.frame(df)
        stage3_path = config['file_output_path']['stage3_output']
        stage3_format = config['file_output_path']['stage3_output_format']
        self.write_parquet(df, stage3_path, stage3_format)
        return df

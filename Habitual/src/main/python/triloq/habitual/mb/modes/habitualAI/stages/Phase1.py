import pandas as pd
import numpy as np


class Phase1:
    def __init__(self):
        pass

    def ri_cat(self, df: pd.DataFrame):
        df['ri'] = np.random.choice([0, 1], df.shape[0])
        return df

    def si_cat(self, df: pd.DataFrame):
        df['si'] = np.random.choice([0, 1], df.shape[0])
        return df

    def ri_si(self, df):
        df = df[df['ri'] == 1 & df['si'] == 1]
        return df

    def nonri(self, df):
        df = df[df['ri'] == 0]
        return df

    def si_nonri(self, df: pd.DataFrame):
        df = df[df['ri'] == 1 & df['si'] == 0]
        return df

    def execute_step1(self, df: pd.DataFrame):
        # get preprocessed data from config file output location
        print("Running RI SI Categorization")
        df = self.ri_cat(df)
        df = self.si_cat(df)
        return df

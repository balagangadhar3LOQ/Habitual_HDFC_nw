import pandas as pd


class Phase1:
    def __init__(self):
        pass

    def ri_cat(self, df: pd.DataFrame):
        return self

    def si_cat(self, df: pd.DataFrame):
        # df['si'] = logic
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

    def execute_step1(self, df: pd.DataFrame, config: dict):
        #get preprocessed data from config file output location

        df = df.apply(self.ri_cat)
        df = df.apply(self.si_cat)
        df = self.ri_cat(df)
        df = self.si_cat(df)
        return df




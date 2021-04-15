from src.main.python.triloq.habitual.mb.modes.habitualAI.stages import DataGen,Phase1,stage3


class FullRun:
    def __init__(self):
        pass

    def execute(self, config):
        d = DataGen.DataGen()
        print("checking")
        df = d.read_data(config)

        phase1 = Phase1()
        df = phase1.execute_step1(df)

        df = stage3.frame(df)



        return df



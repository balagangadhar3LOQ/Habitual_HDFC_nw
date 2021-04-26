import json
from os import path

import pandas as pd

from src.main.python.triloq.habitual.mb.modes.habitualAI.stages import Full_Run
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.Segment1 import Phase1
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.log_writer import write_log


def validate_config(config):
    valid_objects = config['valid_object']
    valid_file_type = valid_objects['input_file_type']
    valid_execution_type = valid_objects['execution_type']
    valid_mode = valid_objects['model_mode']

    file_type = config['input_file_type']
    execution_type = config['execution_type']
    model_mode = config["model_mode"]

    if file_type not in valid_file_type:
        return False
    if execution_type not in valid_execution_type:
        return False
    if model_mode not in valid_mode:
        return False

    try:
        write_log("checking availability of required files")
        for i in config['fileInfo'].keys():
            if not path.exists(config['fileInfo'][i]['name']):
                write_log("Not Existing file {0}".format(config['fileInfo'][i]['name']))
                raise Exception("File not found")
            else:
                pass
    except Exception as e:
        print(e)
        return False
    return True


if __name__ == '__main__':
    with open("Habitual/src/main/resource/config.json") as r:
        config = json.load(r)

    if not validate_config(config):
        import sys
        sys.exit()

    if config['execution_type'] == "full":
        write_log("Processing the execution type {0}".format(config['execution_type']))
        f = Full_Run.FullRun()
        f.execute(config)
    elif config['execution_type'].lower() == "phase1":
        df = pd.read_parquet("output/phase1/dataset") #receive data from the output path
        f = Phase1()
        f. execute_step1(df)





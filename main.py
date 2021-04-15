import json
from os import path
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages import Full_Run
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.Phase1 import Phase1

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
        print("checking availability of required files")
        for i in config['fileInfo'].keys():
            if not path.exists(config['fileInfo'][i]['name']):
                print("Not Existing file {0}".format(config['fileInfo'][i]['name']))
                # raise Exception("File not found")
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
        print("Processing the execution type {0}".format(config['execution_type']))
        f = Full_Run.FullRun()

        f.execute(config)
    elif config['execution_type'].lower() == "phase1":
        f = Phase1()
        f. execute_step1(df)





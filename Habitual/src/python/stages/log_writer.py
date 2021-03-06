import json


def write_log(text):
    valid_log = ['log', 'txt']
    with open("config.json") as r:
        config = json.load(r)
    file_path = config["file_output_path"]["log_file"]
    if file_path[-3:] not in valid_log:
        raise Exception("Invalid Log File extension")
    f = open(file_path, "a")
    f.write("{}\n".format(text))
    return

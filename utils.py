import json
import logging
import sys


signaled_to_stop = False


def load_json(fname):
    opened_file = open(fname)
    result = json.load(opened_file)
    opened_file.close()
    return result


def load_config(fname):
    try:
        core_config = load_json(fname)
    except FileNotFoundError:
        logging.error('Cannot load a config file!')
        sys.exit(1)

    return core_config

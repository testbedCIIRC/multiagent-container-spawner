import os
import json

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app_config.json')

with open(config_path, 'r') as config_file:
    app_config_dic = json.load(config_file)

# Made by Juliano E. S. Padua
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
import yaml

script_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(script_dir, "config.yaml")

# load configuration
with open(config_dir, 'r') as config_file:
    config = yaml.safe_load(config_file)

# initialize paths from config
data_raw_path = os.path.join(script_dir, config['paths']['data_raw'])
data_processed_path = os.path.join(script_dir, config['paths']['data_processed'])
images_path = os.path.join(script_dir, config['paths']['images'])
report_path = os.path.join(script_dir, config['paths']['report'])
addons_path = os.path.join(script_dir, config['paths']['addons'])

# your code starts here
current_datetime = datetime.datetime.now()
print(f"Project 'TCC' initialized on 2025-04-03 14:58:40")

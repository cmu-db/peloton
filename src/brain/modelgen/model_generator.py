#===----------------------------------------------------------------------===#
#
#                         Peloton
#
# model_generator.py
#
# Identification: src/brain/modelgen/model_generator.py
#
# Copyright (c) 2015-2018, Carnegie Mellon University Database Group
#
#===----------------------------------------------------------------------===#

import json
from LSTM_Model import LSTM_Model
import sys

AVAILABLE_MODELS = {
    "LSTM": LSTM_Model
}

REL_SETTINGS_PATH = sys.argv[1]
WRITE_GRAPH_PATH = sys.argv[2]

def generate_models():
    with open(REL_SETTINGS_PATH) as settings_fp:
        settings = json.load(settings_fp)
        model_configs = settings["models"]
        for model_name, config in model_configs.items():
            if model_name in AVAILABLE_MODELS:
                model = AVAILABLE_MODELS[model_name](**config)
                # Call initializer op to add it to graph
                model.tf_init()
                model.write_graph(WRITE_GRAPH_PATH)
                print("Model written successfully!")
            else:
                print("{} unavailable currently!")


if __name__ == '__main__':
    generate_models()
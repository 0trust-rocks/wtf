import json
import os
import importlib

from utils.logs import logger

postprocessors = {}

def get_all_postprocessors():
    processors = {}
    handlers_dir = os.path.dirname(__file__)
    for filename in os.listdir(handlers_dir):
        if filename.endswith('.py') and filename != 'postprocessors.py':
            filename = filename[:-3]
            module_name = f"postprocess.{filename}"
            try:
                module = importlib.import_module(module_name)
                handler_class = None
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if callable(attr) and attr_name == 'extract':
                        processors[filename] = attr
                        break
            except Exception as e:
                print(f"Error loading custom handler from {filename}: {e}")
    
    return processors

postprocessors = get_all_postprocessors()
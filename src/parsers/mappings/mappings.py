"""

TODO:

- move code to __init__.py
- ^ do same here what was done in ../postprocess/

"""

import json
import os
import importlib

from utils.logs import get_logger

logger = get_logger(__name__)

custom_handlers = {}

mappings = {}
priority_fields = []

def load_mappings():
    mappings_path = os.path.join(os.path.dirname(__file__), 'mappings.json')
    mappings = json.load(open(mappings_path, 'r'))
    
    priorityMappings = []
    for mapping in mappings:
        isPriority = mappings[mapping]["priority"] if "priority" in mappings[mapping] else False
        if isPriority:
            priorityMappings.append(mapping)

    return mappings, priorityMappings

mappings, priorityMappings = load_mappings()

def get_mapping(key, detectedFields={}):
    if key in mappings:
        return key
    
    lower = key.lower()
    
    for k, v in mappings.items():
        if lower in v["values"]:
            return k

    # Check if any detected fields match this key (i.e email fields detected by Regex)
    if len(detectedFields) > 0:
        if key in detectedFields:
            detected_type = detectedFields[key]
            for k, v in mappings.items():
                if detected_type in v["values"]:
                    return k

    return None

def get_value(key: str, original_key: str, value: str, original: dict):
    if key in custom_handlers:
        handler = custom_handlers[key]
        try:
            return handler(value, original_key, original)
        except Exception as e:
            logger.error(f"Error applying custom handler for key: {key} with value: {value}\nError: {e}")
        return []
    else:
        return [{key: value}]


def get_all_custom_handlers():
    custom_handlers = {}
    handlers_dir = os.path.join(os.path.dirname(__file__), 'custom_handlers')
    for filename in os.listdir(handlers_dir):
        if filename.endswith('.py') and filename != '__init__.py':
            filename = filename[:-3]
            module_name = f"parsers.mappings.custom_handlers.{filename}"
            try:
                module = importlib.import_module(module_name)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if callable(attr) and attr_name == 'extract':
                        custom_handlers[filename] = attr
                        break
            except Exception as e:
                print(f"Error loading custom handler from {filename}: {e}")
    
    return custom_handlers

custom_handlers = get_all_custom_handlers()
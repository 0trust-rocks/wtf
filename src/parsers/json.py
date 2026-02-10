import json

from utils.logs import logger
from .base_parser import BaseParser

class JSONParser(BaseParser):
    _EXTENSIONS = ['.json']

    def _flatten_dict(self, obj, parent_key='', sep='.'):
        items = []
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)

    def _walk_json(self, obj):
        if isinstance(obj, dict):
            yield self._flatten_dict(obj)
        elif isinstance(obj, list):
            for item in obj:
                yield from self._walk_json(item)
        else:
            yield obj

    def get_itr(self):
        with open(self.file_path, 'r') as f:
            data = json.load(f)
            yield from self._walk_json(data)

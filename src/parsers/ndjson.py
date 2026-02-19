import json
from utils.logs import get_logger
from .base_parser import BaseParser

logger = get_logger(__name__)

class NDJSONParser(BaseParser):
    _EXTENSIONS = ['.ndjson', '.jsonl']

    def get_itr(self):
        with open(self.file_path, 'r') as f:
            for line in f:
                data = json.loads(line.strip())
                yield from self._yield_and_remove_sub_objects(data)

    def _yield_and_remove_sub_objects(self, data):
        if isinstance(data, (str, int, float, bool, type(None))):
            yield data
        elif isinstance(data, dict):
            for key, value in list(data.items()):
                if isinstance(value, (dict, list)):
                    yield from self._yield_and_remove_sub_objects(value)
                    del data[key] 
            yield data
        elif isinstance(data, list):
            for item in list(data):
                if isinstance(item, (dict, list)):
                    yield from self._yield_and_remove_sub_objects(item)
                else:
                    yield item
            yield data

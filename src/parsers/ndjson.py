import json
import os
import gzip
from utils.logs import get_logger
from .base_parser import BaseParser

logger = get_logger(__name__)

class NDJSONParser(BaseParser):
    _EXTENSIONS = ['.ndjson', '.jsonl', '.gz']

    def delist(self, record: dict):
        for key, value in record.items():
            if isinstance(value, list) and len(value) == 1:
                record[key] = value[0]  # Replace the list with the single item
        return record

    def get_itr(self):
        file_extension = os.path.splitext(self.file_path)[1].lower()

        if file_extension == ".gz":
            with gzip.open(self.file_path, 'rt') as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        for key in ['dateOfBirth', 'birthday', 'county', 'city_search', 'address_search', '_version_']:
                            data.pop(key, None)
                        yield self.delist(data)
                    except Exception as e:
                        logger.warning("Unable to parse line %s", line)
        else:
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
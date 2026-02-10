import os
import re
import json
from uuid import uuid4

import parsers.mappings.mappings
from postprocess.postprocessors import postprocessors

from ir.record import Record
from utils.logs import logger

uuid4Re = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$', re.IGNORECASE)

class BaseParser:
    _EXTENSIONS = []

    def __init__(self, file_path, output_path=None):
        self.file_path = file_path
        # If output path is a folder
        if output_path and os.path.isdir(output_path):
            self.output_path = os.path.join(output_path, os.path.basename(file_path) + ".jsonl")
        else:
            self.output_path = output_path if output_path else file_path + ".jsonl"

    def associate_key(self, key):
        return parsers.mappings.mappings.get_mapping(key)

    def parse_value(self, key, value, original):
        if key == "id":
            if isinstance(value, str) and uuid4Re.match(value):
                return [{key: value}]
            else:
                return [{"id": str(uuid4())}]

        return parsers.mappings.mappings.get_value(key, value, original)

    def get_itr(self):
        raise NotImplementedError("Subclasses must implement the get_itr method")

    def parse(self):
        record_count = 0
        with open(self.output_path, 'w') as output_file:
            for record in self.get_itr():
                try:
                    std_record = Record()
                    for key, value in record.items():
                        mapped_key = self.associate_key(key)
                        values = self.parse_value(mapped_key, value, record) if mapped_key else None

                        if not values:
                            # logger.warning(f"Unmapped key: {key} with value: {value}")
                            continue

                        for newValue in values:
                            for k, v in newValue.items():
                                if v is not None and v != "":
                                    std_record.add_or_set_value(k, v)

                    record_dict = std_record.to_dict()

                    if len(record_dict) > 2:
                        if "line" not in record_dict:
                            record_dict["line"] = json.dumps(record, indent=2)

                        # if self.file_path.endswith(".csv"):
                        #     print(json.dumps(record_dict, indent=2))

                        # Apply postprocessors if any exist
                        for name, postprocessor in postprocessors.items():
                            record_dict = postprocessor(record_dict)

                        output_file.write(json.dumps(record_dict) + "\n")
                        record_count += 1

                except Exception as e:
                    logger.error(f"Error parsing record: {record}\nError: {e}")

        if record_count == 0:
            logger.info(f"No records found in file: {self.file_path}")
            # Delete the empty output file
            os.remove(self.output_path)
        else:
            logger.info(f"Finished parsing file: {self.file_path}. Total records: {record_count}. Output written to: {self.output_path}")
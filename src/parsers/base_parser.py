import os
import re
import json
from uuid import uuid4

import parsers.mappings.mappings
from postprocess.postprocessors import postprocessors

from ir.record import Record
from utils.logs import logger
from utils.regex import UUID4_REGEX, EMAIL_REGEX, URL_REGEX, SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX, IP_REGEX


class BaseParser:
    _EXTENSIONS = []

    detectedFields = {}

    def __init__(self, file_path, output_path=None):
        self.file_path = file_path
        # If output path is a folder
        if output_path and os.path.isdir(output_path):
            self.output_path = os.path.join(output_path, os.path.basename(file_path) + ".jsonl")
        else:
            self.output_path = output_path if output_path else file_path + ".jsonl"

    def associate_key(self, key):
        return parsers.mappings.mappings.get_mapping(key, self.detectedFields)

    def parse_value(self, key, value, original):
        if key == "id":
            if isinstance(value, str) and UUID4_REGEX.match(value):
                return [{key: value}]
            else:
                return [{"id": str(uuid4())}]

        return parsers.mappings.mappings.get_value(key, value, original)

    def detect_fields(self):
        for i, record in enumerate(self.get_itr()):
            if i >= 100:
                break

            for key, value in record.items():
                if key in self.detectedFields:
                    continue
                if isinstance(value, str):
                    if EMAIL_REGEX.match(value):
                        logger.debug(f"Detected potential email field: {key} with value: {value}")
                        self.detectedFields[key] = "emails"
                    elif URL_REGEX.match(value):
                        logger.debug(f"Detected potential URL field: {key} with value: {value}")
                        self.detectedFields[key] = "urls"
                    elif SHA1_REGEX.match(value) or SHA256_REGEX.match(value) or SHA512_REGEX.match(value) or BCRYPT_REGEX.match(value):
                        logger.debug(f"Detected potential password field: {key} with value: {value}")
                        self.detectedFields[key] = "passwords"
                    elif IP_REGEX.match(value):
                        logger.debug(f"Detected potential IP field: {key} with value: {value}")
                        self.detectedFields[key] = "ips"
                


    def get_itr(self):
        raise NotImplementedError("Subclasses must implement the get_itr method")

    def parse(self):
        record_count = 0
        self.detect_fields()

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
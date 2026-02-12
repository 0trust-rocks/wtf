from uuid import uuid4
from collections import defaultdict
from postprocess import postprocessors # __init__.py change
from ir.record import Record
from utils.logs import get_logger
from utils.regex import UUID4_REGEX, EMAIL_REGEX, URL_REGEX, SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX, IPV4_REGEX

import os, traceback, orjson, parsers.mappings.mappings

logger = get_logger(__name__)


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

    def parse_value(self, key, original_key, value, original):
        if key == "id":
            if isinstance(value, str) and UUID4_REGEX.match(value):
                return [{key: value}]
            else:
                return [{"id": str(uuid4())}]
            
        return parsers.mappings.mappings.get_value(key, original_key, value, original)

    def detect_fields(self):
        # Temporary storage to count occurrences: {field_name: {type: count}}
        counts = defaultdict(lambda: defaultdict(int))

        for i, record in enumerate(self.get_itr()):
            # Increased limit slightly to ensure we can hit 20 matches
            if i >= 500: 
                break

            for key, value in record.items():
                # Skip if already finalized in a previous run
                if key in self.detectedFields or not isinstance(value, str):
                    continue

                # Identify potential type
                detected_type = None
                if EMAIL_REGEX.match(value):
                    detected_type = "emails"
                elif URL_REGEX.match(value):
                    detected_type = "urls"
                elif IPV4_REGEX.match(value):
                    detected_type = "ips"
                elif any(r.match(value) for r in [SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX]):
                    detected_type = "passwords"

                # If a type was matched, increment and check threshold
                if detected_type:
                    counts[key][detected_type] += 1
                    if counts[key][detected_type] >= 50:
                        logger.debug(f"Threshold met for {key}: confirmed as {detected_type}")
                        self.detectedFields[key] = detected_type

    def get_itr(self):
        raise NotImplementedError("Subclasses must implement the get_itr method")

    def parse(self):
        record_count = 0
        self.detect_fields()
        
        key_mapping_cache = {}

        with open(self.output_path, 'wb') as output_file:
            for record in self.get_itr():
                try:
                    std_record = Record()
                    priority_fields_count = 0
                    for key, value in record.items():
                        # Use cached mapping or compute it once
                        if key not in key_mapping_cache:
                            key_mapping_cache[key] = self.associate_key(key)
                        
                        mapped_key = key_mapping_cache[key]
                        if mapped_key is not None:
                            if mapped_key in parsers.mappings.mappings.priorityMappings:
                                record_count += 1
                            
                            values = self.parse_value(mapped_key, key, value, record) if value is not None else None

                            if not values: continue

                            for newValue in values:
                                for k, v in newValue.items():
                                    if v is not None and v != "":
                                        std_record.add_or_set_value(k, v)

                    record_dict = std_record.to_dict()

                    if len(record_dict) > 2:
                        if "line" not in record_dict:
                            # Remove empty values from the original record to reduce size
                            for k in list(record.keys()):
                                if record[k] is None or record[k] == "":
                                    del record[k]
                            record_dict["line"] = orjson.dumps(record).decode('utf-8')

                        # Apply postprocessors if any exist
                        for name, postprocessor in postprocessors.items():
                            if postprocessor: record_dict = postprocessor(record_dict)
                            logger.error(f"{postprocessor} missing extract() or module is broken or file is corrupted")

                        output_file.write(orjson.dumps(record_dict) + b"\n")
                        record_count += 1

                except Exception as e:
                    logger.error(f"Error parsing record: {record}.\nError: {e}")
                    traceback.print_exc()

        if record_count == 0:
            logger.info(f"No records found in file: {self.file_path}")
            # Delete the empty output file
            os.remove(self.output_path)
        else:
            logger.info(f"Finished parsing file: {self.file_path}. Total records: {record_count}. Output written to: {self.output_path}")
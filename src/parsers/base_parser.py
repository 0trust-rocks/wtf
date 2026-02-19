from uuid import uuid4
from collections import defaultdict
from postprocess import postprocessors
from ir.record import Record
from utils.logs import get_logger
from utils.regex import UUID4_REGEX, EMAIL_REGEX, URL_REGEX, SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX, IPV4_REGEX

import os, traceback, orjson, parsers.mappings.mappings

logger = get_logger(__name__)


class BaseParser:
    _EXTENSIONS = []

    detectedFields = {}

    def __init__(self, file_path, args):
        self.file_path = file_path
        self.num_threads = args.threads
        self.dry_run = args.dry_run
        self.headers = args.headers
        self.source = args.source
        self.recency_year = args.recency_year
        self.no_output = args.no_output

        if self.recency_year is not None:
            logger.info("Using user defined recency year %s when not detected from data", self.recency_year)

        if args.output and os.path.isdir(args.output):
            self.output_path = os.path.join(args.output, os.path.basename(file_path) + ".jsonl")
        else:
            self.output_path = args.output if args.output else file_path + ".jsonl"

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
        counts = defaultdict(lambda: defaultdict(int))

        for i, record in enumerate(self.get_itr()):
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

    def stop_parse(self):
        pass

    def get_itr(self):
        raise NotImplementedError("Subclasses must implement the get_itr method")
    
    def parse(self):
        record_count = 0
        self.detect_fields()
        
        key_mapping_cache = {}

        with open(self.output_path, 'wb') as output_file:
            it = self.get_itr()
            record = None
            try:
                for record in it:
                    record_count += 1
                    if self.dry_run:
                        if record_count >= 100:
                            self.stop_parse()
                            logger.info(f"Dry run limit reached. Stopping...")
                            break
                            

                    std_record = Record()
                    priority_fields_count = 0
                    
                    keys_to_remove = []

                    for key, value in record.items():
                        if key not in key_mapping_cache:
                            key_mapping_cache[key] = self.associate_key(key.strip('"'))
                        
                        mapped_key = key_mapping_cache[key]
                        if mapped_key is not None:
                            values = self.parse_value(mapped_key, key, value, record) if (value is not None and value != "" and value != "-" and value != '""' and value != "NULL") else None

                            # If we have valid values, this key was successfully mapped
                            keys_to_remove.append(key)

                            if not values:
                                continue
                            
                            if mapped_key in parsers.mappings.mappings.priorityMappings:
                                priority_fields_count += 1

                            for newValue in values:
                                for k, v in newValue.items():
                                    if v is not None and v != "":
                                        std_record.add_or_set_value(k, v)

                    if priority_fields_count >= 2:
                        record_dict = std_record.to_dict()

                        if len(record_dict) > 2:
                            if "line" not in record_dict:
                                line_data = record.copy()
                                for k in keys_to_remove:
                                    if k in line_data:
                                        del line_data[k]

                                for k in list(line_data.keys()):
                                    if line_data[k] is None or (isinstance(line_data[k], str) and (len(line_data[k]) > 500 or line_data[k] == "NULL" or line_data[k] == "-" or line_data[k] == "")):
                                        del line_data[k]
                                
                                if line_data:
                                    record_dict["line"] = ", ".join(f"{k}: {v}" for k, v in line_data.items())
                                
                                if self.source:
                                    record_dict["source"] = self.source

                            for name, postprocessor in postprocessors.items():
                                if postprocessor:
                                    try:
                                        record_dict = postprocessor(record_dict)
                                    except Exception as e:
                                        logger.error(f"Postprocessor '{name}' failed: {e}")

                            if self.recency_year is not None and "recencyYear" not in record_dict:
                                record_dict["recencyYear"] = self.recency_year

                            output_file.write(orjson.dumps(record_dict) + b"\n")
            except Exception as e:
                logger.error(f"Error parsing record: {record}.\nError: {e}")
                traceback.print_exc()
            finally:
                it.close()

        if record_count == 0 or self.no_output:
            logger.info(f"No records found in file or no output specified: {self.file_path}")
            if os.path.exists(self.output_path):
                os.remove(self.output_path)
        else:
            logger.info(f"Finished parsing file: {self.file_path}. Total records: {record_count}. Output written to: {self.output_path}")
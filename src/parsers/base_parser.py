from uuid import uuid4
from collections import defaultdict
from postprocess import postprocessors
from ir.record import Record
from utils.logs import get_logger
from utils.regex import UUID4_REGEX, EMAIL_REGEX, URL_REGEX, SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX, IPV4_REGEX
from utils.multithreading import QUEUE_BUFFER_SIZE, FILE_READER_SENTINEL, LINE_PARSER_SENTINEL

import os
import orjson
import multiprocessing

logger = get_logger(__name__)


class BaseParser:
    _EXTENSIONS = []
    detectedFields = {}
    write_q = None
    writer_p = None

    parse_input_q = None
    parser_ps = []

    def __init__(self, file_path, args):
        self.args = args
        self.file_path = file_path
        self.num_threads = args.threads
        self.dry_run = args.dry_run
        self.headers = args.headers
        self.source = args.source
        self.recency_year = args.recency_year
        self.no_output = args.no_output

        if self.args.recency_year is not None:
            logger.info("Using user defined recency year %s when not detected from data", self.args.recency_year)

        if args.output and os.path.isdir(args.output):
            self.output_path = os.path.join(args.output, os.path.basename(file_path) + ".jsonl")
        else:
            self.output_path = args.output if args.output else file_path + ".jsonl"
        

    def detect_fields(self):
        counts = defaultdict(lambda: defaultdict(int))

        old_threadCount = self.num_threads
        self.num_threads = 1

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

        self.num_threads = old_threadCount

    def stop_parse(self):
        pass

    def get_itr(self):
        raise NotImplementedError("Subclasses must implement the get_itr method")
    
    def start_extraction(self):
        self.detect_fields()

        self.write_q = multiprocessing.Queue(maxsize=QUEUE_BUFFER_SIZE)
        self.parse_input_q = multiprocessing.Queue(maxsize=QUEUE_BUFFER_SIZE)

        self.writer_p = multiprocessing.Process(
            target=self._file_writer_worker, 
            args=(self.output_path, self.write_q)
        )
        self.writer_p.daemon = True
        self.writer_p.start()


        self.parser_ps = []
        for _ in range(self.num_threads):
            p = multiprocessing.Process(
                target=self._parser_thread,
                args=(self.parse_input_q, self.write_q, self.detectedFields, self.args)
            )
            p.daemon = True
            p.start()
            self.parser_ps.append(p)

        for record in self.get_itr():
            self.parse_input_q.put(record)

        for _ in range(self.num_threads):
            self.parse_input_q.put(LINE_PARSER_SENTINEL)

        for parser_p in self.parser_ps:
            parser_p.join()

        self.write_q.put(FILE_READER_SENTINEL)
        self.writer_p.join()


    @staticmethod
    def _file_writer_worker(output_path: str, write_q: multiprocessing.Queue):
        logger.info(f"Starting writer process for {output_path}")
        try:
            with open(output_path, 'wb') as f:
                while True:
                    line = write_q.get()
                    
                    if line == FILE_READER_SENTINEL:
                        break
                    
                    if line:
                        f.write(line + b"\n")
        except Exception as e:
            logger.error(f"Writer Process Error: {e}")
        finally:
            logger.info("Writer process finished.")

    @staticmethod
    def _parser_thread(parse_input_q: multiprocessing.Queue, write_q: multiprocessing.Queue, detectedFields: dict, args):
        import parsers.mappings.mappings

        key_mapping_cache = {}

        while True:
            record = parse_input_q.get()

            if record == LINE_PARSER_SENTINEL:
                break

            std_record = Record()

            priority_fields_count = 0
            keys_to_remove = []

            for key, value in record.items():
                if key not in key_mapping_cache:
                    key_mapping_cache[key] = parsers.mappings.mappings.get_mapping(key.strip('"'), detectedFields)
                
                mapped_key = key_mapping_cache[key]
                if mapped_key is not None:
                    if mapped_key == "id":
                        if isinstance(value, str) and UUID4_REGEX.match(value):
                            values = [{key: value}]
                        else:
                            values = [{"id": str(uuid4())}]
                    else:
                        values = parsers.mappings.mappings.get_value(mapped_key, key, value, record) if (value is not None and value != "" and value != "-" and value != '""' and value != "NULL") else None

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
                        
                        if args.source:
                            record_dict["source"] = args.source

                    for name, postprocessor in postprocessors.items():
                        if postprocessor:
                            try:
                                record_dict = postprocessor(record_dict)
                            except Exception as e:
                                logger.error(f"Postprocessor '{name}' failed: {e}")

                    if args.recency_year is not None and "recencyYear" not in record_dict:
                        record_dict["recencyYear"] = args.recency_year
                    write_q.put(orjson.dumps(record_dict))
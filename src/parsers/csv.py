import csv
import codecs
from typing import List
import chardet
import traceback
import multiprocessing
import utils.multithreading

from utils.logs import get_logger
from parsers.base_parser import BaseParser
from parsers.unknown import extract_with_unknown_parser

logger = get_logger(__name__)
POSSIBLE_DELIMITERS = [",", "\t", " | ", "|", ":", " "]
csv.field_size_limit(10 * 1024 * 1024)


class CSVParser(BaseParser):
    _EXTENSIONS = [".csv", ".tsv", ".psv", ".txt"]
    lastLine = ""
    input_q = None
    output_q = None
    reader_p = None
    worker_ps = []

    def detect_encoding_and_bom(self):
        with open(self.file_path, 'rb') as f:
            raw_data = f.read(4)

        bom_signatures = [
            (codecs.BOM_UTF32_BE, 'utf-32'),
            (codecs.BOM_UTF32_LE, 'utf-32'),
            (codecs.BOM_UTF16_BE, 'utf-16'),
            (codecs.BOM_UTF16_LE, 'utf-16'),
            (codecs.BOM_UTF8, 'utf-8-sig'),
        ]

        for bom, encoding in bom_signatures:
            if raw_data.startswith(bom):
                return (encoding, True)

        try:
            with open(self.file_path, 'rb') as f:
                raw_data = f.read(4)
            
            bom_signatures = [
                (codecs.BOM_UTF32_BE, 'utf-32'),
                (codecs.BOM_UTF32_LE, 'utf-32'),
                (codecs.BOM_UTF16_BE, 'utf-16'),
                (codecs.BOM_UTF16_LE, 'utf-16'),
                (codecs.BOM_UTF8,  'utf-8-sig'),
            ]
            
            for bom, encoding in bom_signatures:
                if raw_data.startswith(bom):
                    return encoding, True
            
            with open(self.file_path, 'rb') as f:
                # We only read 1MB to keep memory low
                result = chardet.detect(f.read(1024 * 1024))
            if result and result.get('encoding'):
                if result['encoding'] == 'ascii':
                    return ('utf-8', False)

                return (result['encoding'], False)
        except:
            pass

        for encoding in ['utf-8', 'cp1252', 'iso-8859-1']:
            try:
                with open(self.file_path, 'r', encoding=encoding) as f:
                    f.read(524288)
                return (encoding, False)
            except (UnicodeDecodeError, LookupError):
                continue

        return ('utf-8', False)

    def detect_delimiter(self, encoding):
        possibleDelims = dict.fromkeys(POSSIBLE_DELIMITERS, 0)
        with open(self.file_path, 'r', encoding=encoding) as f:
            for _ in range(1000):
                line = f.readline()
                if not line:
                    break
                line = line.rstrip()
                for delim in POSSIBLE_DELIMITERS:
                    if delim in line:
                        possibleDelims[delim] += 1

        maxKey = None
        maxValue = 0
        for k, v in possibleDelims.items():
            if v > maxValue:
                maxKey = k
                maxValue = v
        return maxKey
    
    def detect_fieldnames(self, encoding, delimiter):
        with open(self.file_path, 'r', encoding=encoding) as f:
            first_line = f.readline().strip()
            fieldnames = first_line.split(delimiter)
            fieldnames = [field.strip() for field in fieldnames]

        return fieldnames


    @staticmethod
    def _line_parser_worker(input_q: multiprocessing.Queue, output_q: multiprocessing.Queue, fieldnames: List[str], delimiter: str):
        logger.info("Starting line parser worker")
        lastLine = ""
        def _queue_generator():
            nonlocal lastLine
            while True:
                line = input_q.get()
                
                if line == utils.multithreading.LINE_PARSER_SENTINEL:
                    logger.info("Got sentinel, ending parser thread")
                    break
                
                if line and line.strip():
                    lastLine = line
                    yield line

        cleaned_delimiter = delimiter.strip(" ")

        reader = csv.DictReader(_queue_generator(), delimiter=cleaned_delimiter, fieldnames=fieldnames)

        for row in reader:
            try:
                if row and all(k is not None for k in row):
                    output_q.put({k: v.strip() for k, v in row.items() if k is not None and v is not None})
                else:
                    logger.warning("Malformed CSV row, using UnknownParser fallback: %s", lastLine.strip())
                    output_q.put(extract_with_unknown_parser(lastLine))
            except Exception:
                logger.warning("CSV parsing error, using UnknownParser fallback: %s %s", lastLine.strip(), traceback.format_exc())
                output_q.put(extract_with_unknown_parser(lastLine))

        logger.info("Processor thread complete")
        output_q.put(utils.multithreading.FILE_READER_SENTINEL)

    @staticmethod
    def _file_reader_worker(file_path: str, encoding: str, delimiter: str, 
                            input_q: multiprocessing.Queue, num_workers: int):
        try:
            with open(file_path, 'r', encoding=encoding, errors="ignore") as f:
                next(f, None) 
                
                for line in f:
                    clean_line = line.strip()
                    if clean_line:
                        input_q.put(clean_line)
        except Exception as e:
            logger.error(f"Reader Process Error: {e}")
        finally:
            # Tell EVERY worker thread to stop
            for _ in range(num_workers):
                input_q.put(utils.multithreading.LINE_PARSER_SENTINEL)

    def stop_parse(self):
        logger.debug("Stopping CSV parser threads...")
        if self.reader_p and self.reader_p.is_alive():
            self.reader_p.terminate()
            self.reader_p.join(0.1)

        for w in self.worker_ps:
            if w.is_alive():
                w.terminate()
                w.join(0.1)

    def get_itr(self):
        encoding, _ = self.detect_encoding_and_bom()
        delimiter = self.detect_delimiter(encoding)
        
        # Ensure fieldnames we have field names
        detected_fields = self.detect_fieldnames(encoding, delimiter)
        fieldnames = self.headers.split(',') if self.headers else detected_fields

        if not delimiter:
            logger.error("Could not detect delimiter.")
            return

        self.input_q = multiprocessing.Queue(maxsize=utils.multithreading.QUEUE_BUFFER_SIZE)
        self.output_q = multiprocessing.Queue(maxsize=utils.multithreading.QUEUE_BUFFER_SIZE)

        self.reader_p = multiprocessing.Process(
            target=self._file_reader_worker,
            args=(self.file_path, encoding, delimiter, self.input_q, self.num_threads)
        )
        self.reader_p.daemon = True
        self.reader_p.start()

        self.worker_ps = []
        for _ in range(self.num_threads):
            p = multiprocessing.Process(
                target=self._line_parser_worker,
                args=(self.input_q, self.output_q, fieldnames, delimiter)
            )
            p.daemon = True
            p.start()
            self.worker_ps.append(p)

        # 3. Yield for base_parser until we are done processing
        finished_workers = 0
        try:
            while finished_workers < self.num_threads:
                item = self.output_q.get()
                
                # We use the sentinel to count how many workers have finished
                if item == utils.multithreading.FILE_READER_SENTINEL:
                    finished_workers += 1
                else:
                    yield item
        finally:
            self.stop_parse()
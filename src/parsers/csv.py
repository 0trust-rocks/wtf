import csv
import codecs
import chardet
import multiprocessing
import queue
import logging
from typing import Generator, Any, Dict, Optional, Tuple

from utils.logs import get_logger
from parsers.base_parser import BaseParser

logger = get_logger(__name__)

# Constants for better maintainability
POSSIBLE_DELIMITERS = [",", "\t", " | ", "|", ":"]
QUEUE_BUFFER_SIZE = 1000
SENTINEL = "STOP"

class SVParser(BaseParser):
    _EXTENSIONS = [".csv", ".tsv", ".psv", ".txt"]
    workers = []
    reader_p = None

    def detect_encoding_and_bom(self) -> Tuple[str, bool]:
        """Detects file encoding and presence of BOM."""
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
                
                # Pylance fix: Check if 'encoding' is not None before returning
                detected_enc = result.get('encoding')
                if detected_enc and result.get('confidence', 0) > 0.8:
                    return str(detected_enc), False
                    
        except Exception as e:
            logger.warning(f"Encoding detection failed: {e}")
        
        # Absolute fallback to ensure we never return (None, False)
        return 'utf-8', False

    def detect_delimiter(self, encoding: str) -> Optional[str]:
        """Sniffs the delimiter by analyzing the first 1000 lines."""
        delim_counts = {d: 0 for d in POSSIBLE_DELIMITERS}
        try:
            with open(self.file_path, 'r', encoding=encoding, errors='replace') as f:
                for i, line in enumerate(f):
                    if i > 1000: break
                    for d in POSSIBLE_DELIMITERS:
                        if d in line:
                            delim_counts[d] += 1
            
            # Filter and find max
            valid_delims = {k: v for k, v in delim_counts.items() if v > 0}
            if not valid_delims:
                return None
            return max(valid_delims, key=valid_delims.__getitem__)
        except Exception as e:
            logger.error(f"Delimiter detection failed: {e}")
            return None

    @staticmethod
    def _file_reader_worker(file_path: str, encoding: str, delimiter: str, 
                            input_q: multiprocessing.Queue, num_workers: int):
        try:
            clean_delim = delimiter.strip()
            
            def line_generator():
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    for line in f:
                        # Handle multi-char delimiters for the standard csv module
                        if len(delimiter) > 1:
                            yield line.replace(delimiter, clean_delim)
                        else:
                            yield line
            
            # DictReader is a lazy iterator - it doesn't load the file into memory
            reader = csv.DictReader(line_generator(), delimiter=clean_delim)
            for row in reader:
                if row:
                    input_q.put(row) # This blocks if queue is full
                    
        except Exception as e:
            logger.error(f"Reader Worker Exception: {e}")
        finally:
            # Send shutdown signal to all processing workers
            for _ in range(num_workers):
                input_q.put(SENTINEL)

    @staticmethod
    def _parse_worker(input_q: multiprocessing.Queue, output_q: multiprocessing.Queue):
        """Processes rows from the input queue."""
        while True:
            row = input_q.get()
            if row == SENTINEL:
                break
            try:
                # Place for data validation or transformation
                output_q.put(row)
            except Exception as e:
                logger.error(f"Parse Worker Exception: {e}")
        
        # Signal that this specific worker is done
        output_q.put(SENTINEL)

    def stop_parse(self):
        """Orchestrate a clean shutdown of multiprocessing components."""
        logger.debug("Stopping CSV parser workers...")
        if self.reader_p and self.reader_p.is_alive():
            self.reader_p.terminate()
            self.reader_p.join(0.1)
        
        for w in self.workers:
            if w.is_alive():
                w.terminate()
                w.join(0.1)
        
        self.workers = []
        self.reader_p = None

    def get_itr(self) -> Generator[Dict[str, Any], None, None]:
        """Main orchestrator for the multiprocessing pipeline."""
        encoding, _ = self.detect_encoding_and_bom()
        delimiter = self.detect_delimiter(encoding)

        if not encoding:
            raise ValueError(f"Could not determine encoding for {self.file_path}")
        
        if not delimiter:
            raise ValueError(f"Could not determine delimiter for {self.file_path}")

        # Queues are the backbone. maxsize ensures we don't exceed memory limits.
        input_q = multiprocessing.Queue(maxsize=QUEUE_BUFFER_SIZE)
        output_q = multiprocessing.Queue(maxsize=QUEUE_BUFFER_SIZE * 2)
        
        # 1. Start Parser Workers
        workers = []
        for _ in range(self.num_threads):
            p = multiprocessing.Process(target=self._parse_worker, args=(input_q, output_q))
            p.daemon = True 
            p.start()
            self.workers.append(p) # Store in self

        # 2. Start Reader Process
        self.reader_p = multiprocessing.Process(
            target=self._file_reader_worker,
            args=(self.file_path, encoding, delimiter, input_q, self.num_threads)
        )
        self.reader_p.daemon = True
        self.reader_p.start()

        try:
            while True:
                item = output_q.get()
                if item == SENTINEL:
                    # Logic for counting finished workers can go here if needed
                    continue 
                yield item
        except GeneratorExit:
            # This is triggered if the loop in base_parser breaks (dry_run)
            self.stop_parse()
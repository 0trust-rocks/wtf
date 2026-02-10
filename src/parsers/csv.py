import csv
import codecs
import chardet
from pathlib import Path

from utils.logs import logger
from parsers.base_parser import BaseParser

class CSVParser(BaseParser):
    _EXTENSIONS = ['.csv']

    def detect_encoding_and_bom(self, file_path):
        with open(file_path, 'rb') as f:
            raw_data = f.read(4)
        
        # Check for BOMs in order of likelihood (longest first)
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
        
        # Try to detect encoding using chardet
        try:
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read(1024 * 1024))
            if result and result.get('encoding'):
                return (result['encoding'], False)
        except:
            pass
        
        # Try common encodings
        common_encodings = ['utf-8', 'cp1252', 'iso-8859-1', 'ascii']
        for encoding in common_encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1024)
                return (encoding, False)
            except (UnicodeDecodeError, LookupError):
                continue
        
        # Default fallback
        return ('utf-8', False)

    def get_itr(self):
        encoding, has_bom = self.detect_encoding_and_bom(self.file_path)
        logger.debug(f"Detected encoding: {encoding} for file: {self.file_path}")
        
        with open(self.file_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
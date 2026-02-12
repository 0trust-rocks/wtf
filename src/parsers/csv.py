import csv
import codecs
import chardet

from utils.logs import get_logger
from parsers.base_parser import BaseParser

logger = get_logger(__name__)
POSSIBLE_DELIMITERS = [",", "\t", " | ", "|"]

class CSVParser(BaseParser):
    _EXTENSIONS = ['.csv']

    def detect_encoding_and_bom(self):
        with open(self.file_path, 'rb') as f:
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
        
        try:
            with open(self.file_path, 'rb') as f:
                result = chardet.detect(f.read(1024 * 1024))
            if result and result.get('encoding'):
                return (result['encoding'], False)
        except:
            pass
        
        # Try common encodings
        common_encodings = ['utf-8', 'cp1252', 'iso-8859-1', 'ascii']
        for encoding in common_encodings:
            try:
                with open(self.file_path, 'r', encoding=encoding) as f:
                    f.read(1024)
                return (encoding, False)
            except (UnicodeDecodeError, LookupError):
                continue
        
        # Default fallback
        return ('utf-8', False)
    
    def detect_delimiter(self, encoding):
        possibleDelims = dict.fromkeys(POSSIBLE_DELIMITERS, 0)

        with open(self.file_path, 'r', encoding=encoding) as f:
            lines = []
            for x in range(1000):
                line = f.readline()
                if line is None:
                    break
                lines.append(line.rstrip())

            for line in lines:
                for delim in POSSIBLE_DELIMITERS:
                    if delim in line:
                        possibleDelims[delim] += 1
        
        maxKey = None
        maxValue = 0
        for k, v in possibleDelims.items():
            if v > maxValue and v > 10:
                maxKey = k
                maxValue = v
        
        return maxKey
    
    def get_csv_iter(self, encoding, delimiter: str):
        clean_delim = delimiter.strip()
        with open(self.file_path, 'r', encoding=encoding) as f:
            for line in f:
                if delimiter in line:
                    if len(delimiter) > 1:
                        yield line.replace(delimiter, clean_delim)
                    else:
                        yield line

    def get_itr(self):
        encoding, has_bom = self.detect_encoding_and_bom()
        delimiter = self.detect_delimiter(encoding)
        
        logger.debug("Detected %s CSV with delimiter %s", encoding, delimiter)

        if delimiter is None:
            logger.error("Unable to detect delimiter for file: %s", self.file_path)
            exit(-1)
        
        cleaned_delimiter = delimiter.strip(" ")
        
        with open(self.file_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(self.get_csv_iter(encoding, delimiter), delimiter=cleaned_delimiter)
            for row in reader:
                yield row
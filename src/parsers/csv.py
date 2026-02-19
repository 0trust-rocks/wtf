import csv
import codecs
import chardet
import urllib.parse
import traceback

from utils.logs import get_logger
from parsers.base_parser import BaseParser
from ir.record import Record
from utils.regex import EMAIL_REGEX, URL_ENCODED_EMAIL_REGEX, URL_REGEX, SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX, IPV4_REGEX

logger = get_logger(__name__)
POSSIBLE_DELIMITERS = [",", "\t", " | ", "|", ":", " "]
csv.field_size_limit(10 * 1024 * 1024)


class CSVParser(BaseParser):
    _EXTENSIONS = [".csv", ".tsv", ".psv", ".txt"]
    lastLine = ""

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
            if v > maxValue and v > 10:
                maxKey = k
                maxValue = v
        return maxKey

    def get_csv_iter(self, encoding, delimiter: str):
        clean_delim = delimiter.strip() if delimiter not in [" ", "\t"] else delimiter
        with open(self.file_path, 'r', encoding=encoding, errors="ignore") as f:
            for line in f:
                self.lastLine = line  # Always keep track of last line
                if len(line) < 131072:
                    if len(delimiter) > 1:
                        yield line.replace(delimiter, clean_delim).strip()
                    else:
                        yield line.strip()
                else:
                    logger.warning("Line too long %s", line)

    def extract_with_unknown_parser(self, line):
        """Fallback parser for a single line using UnknownParser logic."""
        ir = Record()
        for email in EMAIL_REGEX.findall(line):
            ir.add_or_set_value("emails", email)
        for email in URL_ENCODED_EMAIL_REGEX.findall(line):
            decoded_email = urllib.parse.unquote(email)
            ir.add_or_set_value("emails", decoded_email)
        for url in URL_REGEX.findall(line):
            ir.add_or_set_value("urls", url)
        for sha1 in SHA1_REGEX.findall(line):
            ir.add_or_set_value("passwords", sha1)
        for sha256 in SHA256_REGEX.findall(line):
            ir.add_or_set_value("passwords", sha256)
        for sha512 in SHA512_REGEX.findall(line):
            ir.add_or_set_value("passwords", sha512)
        for bcrypt in BCRYPT_REGEX.findall(line):
            ir.add_or_set_value("passwords", bcrypt)
        for ip in IPV4_REGEX.findall(line):
            ir.add_or_set_value("ips", ip)
        ir.add_or_set_value("line", line.strip())
        return ir.to_dict()

    def get_itr(self):
        encoding, has_bom = self.detect_encoding_and_bom()
        delimiter = self.detect_delimiter(encoding)

        logger.debug("Detected %s CSV with delimiter %s", encoding, delimiter)
        if delimiter is None:
            logger.error("Unable to detect delimiter for file: %s", self.file_path)
            exit(-1)

        cleaned_delimiter = delimiter.strip() if delimiter not in ["\t", " "] else delimiter
        fieldnames = self.headers.split(',') if self.headers else None
        logger.info("Using CSV field names: %s", fieldnames)

        reader = csv.DictReader(self.get_csv_iter(encoding, delimiter), delimiter=cleaned_delimiter, fieldnames=fieldnames)

        for row in reader:
            try:
                if row and all(k is not None for k in row):
                    yield {k: v.strip() for k, v in row.items() if k is not None and v is not None}
                else:
                    logger.warning("Malformed CSV row, using UnknownParser fallback: %s", self.lastLine.strip())
                    yield self.extract_with_unknown_parser(self.lastLine)
            except Exception:
                logger.warning("CSV parsing error, using UnknownParser fallback: %s %s", self.lastLine.strip(), traceback.format_exc())

                yield self.extract_with_unknown_parser(self.lastLine)

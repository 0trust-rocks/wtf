# Generic parser for unknown text formats. It will make the best attempt to extract data using regex.
import urllib.parse

from utils.fingerprint_unknown import fingerprint_type
from utils.logs import get_logger

logger = get_logger(__name__)
from utils.regex import EMAIL_REGEX, URL_ENCODED_EMAIL_REGEX, URL_REGEX, SHA1_REGEX, SHA256_REGEX, SHA512_REGEX, BCRYPT_REGEX, IPV4_REGEX
from parsers.base_parser import BaseParser
from ir.record import Record


def extract_with_unknown_parser(line):
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

class UnknownParser(BaseParser):
    _EXTENSIONS = [".txt"]
    _IS_WILDCARD = True

    def get_itr(self):
        fingerprint = fingerprint_type(self.file_path)
        logger.info(f"Fingerprint for file {self.file_path}: {fingerprint}")
        with open(self.file_path, 'r') as f:
            for line in f:
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
                yield ir.to_dict()

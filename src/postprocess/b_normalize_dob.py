import re
from dateutil import parser
from utils.logs import get_logger

logger = get_logger(__name__)

ZERO_PATTERN = re.compile(r'(^|[-/])0+(?=[-/]|$)')
DIGIT_PATTERN = re.compile(r'\d+')

def extract(record):
    dob_val = record.get("dob")
    if not dob_val:
        return record

    dob_str = str(dob_val).strip().split(' ')[0]
    normalized = ZERO_PATTERN.sub(r'\1 1 ', dob_str).replace(' ', '')

    try:
        dt = parser.parse(normalized, fuzzy=True)
        original_parts = DIGIT_PATTERN.findall(dob_str)
        raw_values = [int(p) for p in original_parts]

        if dt.year > 0 and dt.year in raw_values:
            record["dobYear"] = dt.year
            
        if dt.month > 0 and dt.month in raw_values:
            record["dobMonth"] = dt.month
            
        if dt.day > 0 and dt.day in raw_values:
            record["dobDay"] = dt.day

    except (ValueError, OverflowError, TypeError) as e:
        pass
    
    if "dob" in record:
        del record["dob"]

    return record
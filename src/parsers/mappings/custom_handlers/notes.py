import ipaddress
from utils.regex import IPV4_REGEX, IPV6_REGEX
from utils.logs import get_logger

logger = get_logger(__name__)
from typing import List


def extract(note: str, original_key: str, original_dict: dict):
    results = []
    if (isinstance(note, list)):
        results.append({"notes": note})
    else:
        results.append({"notes": f"{original_key}: {note}"})

    return results

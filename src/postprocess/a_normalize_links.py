from utils.logs import get_logger
from utils.regex import URL_REGEX
import re, time

logger = get_logger(__name__)

def extract(record):
    if "links" in record and isinstance(record["links"], list):
        record["links"] = [
            link for link in record["links"] 
            if re.match(URL_REGEX, str(link))
        ]
        
        if not record["links"]:
            del record["links"]

    return record
from utils.logs import get_logger

logger = get_logger(__name__)

def extract(record):
    if "source" in record:
        val = record["source"].strip().lower().split(' ')[0].split('-')[0].split("/")[0]
        
        record["source"] = val

    return record
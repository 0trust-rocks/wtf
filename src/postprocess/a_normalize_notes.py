from utils.logs import get_logger

logger = get_logger(__name__)

def extract(record):
    if "notes" in record:
        if isinstance(record["notes"], list):
                if "line" in record:
                    record["line"] = record['line'] + ' '.join(record["notes"])
                else:
                    record["line"] = ' '.join(record["notes"])
        elif isinstance(record["notes"], str):
            if "line" in record:
                record["line"] = record["line"] + record["notes"]
            else:
                 record["line"] = record["notes"]

        del record["notes"]

    return record
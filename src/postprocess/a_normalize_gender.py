from utils.logs import get_logger

logger = get_logger(__name__)

GENDER_MAP = {
    "male": "m", "m": "m", "man": "m", "boy": "m", "true": "m",
    "female": "f", "f": "f", "woman": "f", "girl": "f", "false": "f",
    "other": "o", "o": "o", "non-binary": "o", "nb": "o", "diverse": "o"
}

def extract(record):
    if "gender" in record:
        val = record["gender"]
        
        cleaned = val.strip().lower()
        record["gender"] = GENDER_MAP.get(cleaned, "o")

    return record
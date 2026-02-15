from utils.logs import get_logger
from dateutil import parser

logger = get_logger(__name__)

def extract(dob_string: str, original_key: str, original_dict: dict):
    results = []
    
    if not dob_string:
        return results

    try:
        # fuzzy=True allows it to ignore extra text like "Born on: " 
        dt = parser.parse(str(dob_string), fuzzy=True)
        
        results.append({
            "dobYear": str(dt.year),
            "dobMonth": f"{dt.month:02d}",
            "dobDay": f"{dt.day:02d}"
        })
        
    except (ValueError, OverflowError, TypeError) as e:
        logger.warning("Could not parse DOB: %s. Error: %s", dob_string, e)

    return results
from utils.logs import get_logger
from dateutil import parser
import re

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
        dob_parts = re.split(r'[\/\-\s,]+', dob_string) 

        if len(dob_parts) == 3:
            m, d, y = dob_parts
            try:
                if int(m) != 0:
                    results.append({"dobMonth": int(m)})
            except ValueError:
                pass

            try:
                if int(d) != 0:
                    results.append({"dobDay": int(d)})
            except ValueError:
                pass

            try:
                if int(y) != 0:
                    results.append({"dobYear": int(y)})
            except ValueError:
                pass
    return results
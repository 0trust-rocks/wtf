# fullName isn't actually a field on base record, this generates first/middle/last
from utils.logs import get_logger

logger = get_logger(__name__)

def extract(fullName: str, original_key: str, original_dict: dict):
    results = []
    nameSplit = fullName.strip().split(' ')

    if len(nameSplit) == 0:
        logger.warning("Invalid full name %s", fullName)
    elif len(nameSplit) == 1:
        results.append({
            "firstName": nameSplit[0]
        })
    elif len(nameSplit) == 2:
        results.append({
            "firstName": nameSplit[0],
            "lastName": nameSplit[-1]
        })
    else:
        middleName = ' '.join(nameSplit[1:-1])

        results.append({
            "firstName": nameSplit[0],
            "middleName": middleName,
            "lastName": nameSplit[-1]
        })

    return results
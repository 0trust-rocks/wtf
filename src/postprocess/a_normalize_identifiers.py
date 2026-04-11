from utils.logs import get_logger

logger = get_logger(__name__)

def extract(record):
    if "usernames" in record:
        if isinstance(record["usernames"], list):
            record["usernames"] = [username.lower().rstrip() for username in record["usernames"]]
        elif isinstance(record["usernames"], str):
            record["usernames"] = record["usernames"].lower().rstrip()
        else:
            logger.warning("Invalid usernames field: %s", record["usernames"])
    
    if "emails" in record:
        if isinstance(record["emails"], list):
            record["emails"] = [username.lower().rstrip() for username in record["emails"]]
        elif isinstance(record["emails"], str):
            record["emails"] = record["emails"].lower().rstrip()
        else:
            logger.warning("Invalid emails field: %s", record["emails"])

    if "passwords" in record:
        if isinstance(record["passwords"], list):
            record["passwords"] = [password.lower().rstrip() for password in record["passwords"]]
        elif isinstance(record["passwords"], str):
            record["passwords"] = record["passwords"].lower().rstrip()
        else:
            logger.warning("Invalid passwords field: %s", record["passwords"])
    
    if "source" in record:
        if isinstance(record["source"], str):
            record["source"] = record["source"].split('.')[0].split('_')[0].split(" ")[0].lower()


    return record
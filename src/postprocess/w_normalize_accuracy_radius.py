from utils.logs import get_logger

logger = get_logger(__name__)

def extract(record):
    if "latLong" in record and "accuracy_radius" not in record:
        lat_lon_str = str(record["latLong"])
        parts = lat_lon_str.split(',')
        
        if len(parts) == 2:
            try:
                # Ensure both parts are valid floats
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                
                # Calculate precision based on the latitude string
                decimal_part = parts[0].split('.')[-1].rstrip("0") if '.' in parts[0] else ""
                precision = len(decimal_part)

                # Accuracy mapping (approximate km at the equator)
                accuracy_map = {
                    0: 111,
                    1: 11,
                    2: 1.1,
                    3: 0.11,
                    4: 0.011,
                    5: 0.0011
                }

                # Default to the highest precision in the map if digits exceed 5
                record["accuracy_radius"] = accuracy_map.get(precision, 0.0011)
                
            except (ValueError, TypeError):
                # Discard latLong if parts aren't valid floats
                del record["latLong"]
        else:
            # Discard latLong if formatting (comma split) is wrong
            del record["latLong"]
            
    return record
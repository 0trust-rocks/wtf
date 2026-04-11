import requests
import math
import traceback
import random
from utils.logs import get_logger

# Configuration
HOSTS = ["10.0.5.1:4000", "10.0.5.2:4000", "10.0.5.3:4000", "10.0.5.4:4000", ]
PATH = "/v1/search"
TIMEOUT = 5
logger = get_logger(__name__)

SESSIONS = {host: requests.Session() for host in HOSTS}

def calculate_radius(lat, lon, bbox):
    """Calculates distance in meters from center to a bbox corner."""
    try:
        corner_lon, corner_lat = bbox[0], bbox[1]
        
        R = 6371000 
        phi1, phi2 = math.radians(lat), math.radians(corner_lat)
        d_phi = math.radians(corner_lat - lat)
        d_lambda = math.radians(corner_lon - lon)
        
        a = math.sin(d_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return round((R * c) / 1000, 5)
    except Exception as e:
        logger.error("Error calculating accuracy_radius: %s", traceback.format_exc())
        return None

def extract(record):
    location_found = False

    if record.get("address"):
        # Randomly select a host to distribute the load
        selected_host = random.choice(HOSTS)
        selected_session = SESSIONS[selected_host]
        url = f"http://{selected_host}{PATH}"

        try:
            res = selected_session.get(url, params={"text": record["address"]}, timeout=TIMEOUT)
            res.raise_for_status()

            
            data = res.json()
            if "features" in data:
                if isinstance(data["features"], list) and len(data["features"]) > 0:
                    best_match = data["features"][0]


                    geom = best_match.get("geometry", {})
                    coordinates = geom.get("coordinates")

                    if coordinates is not None:
                        record["latLong"] = f"{coordinates[1]},{coordinates[0]}"

                        location_found = True
                        
                        if "bbox" in best_match:
                            radius = calculate_radius(coordinates[1], coordinates[0], best_match["bbox"])
                            if radius:
                                record["accuracy_radius"] = radius
                        elif "bbox" in data:
                            radius = calculate_radius(coordinates[1], coordinates[0], data["bbox"])
                            if radius:
                                record["accuracy_radius"] = radius
                        
                        
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Host {selected_host} failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in extract: {e}")

    # Fallback and Cleanup logic
    if not location_found:
        if "_latLong" in record:
            record["latLong"] = record.get("_latLong")
        
        if "_accuracy_radius" in record:
            record["accuracy_radius"] = record.get("_accuracy_radius")

    record.pop("_latLong", None)
    record.pop("_accuracy_radius", None)
    
    return record
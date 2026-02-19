import os
from utils.logs import get_logger
import geoip2.database

logger = get_logger(__name__)

mappings_path = os.path.join(os.path.dirname(__file__), 'resources', 'GeoLite2-City.mmdb')

try:
    geo_reader = geoip2.database.Reader(mappings_path)
except Exception as e:
    logger.error(f"Failed to load GeoLite2 database: {e}")
    geo_reader = None


def extract(record):
    if geo_reader is None:
        logger.warning("GeoLite2 database not loaded, skipping location enrichment.")
        return record

    if record.get("city") or record.get("state") or record.get("country"):
        return record

    ip_list = record.get("ips", [])
    if not ip_list:
        return record

    for ip in ip_list:
        try:
            response = geo_reader.city(ip)

            if not record.get("city") and response.city and response.city.name:
                record["city"] = response.city.name

            if not record.get("state") and response.subdivisions and len(response.subdivisions) > 0:
                record["state"] = response.subdivisions.most_specific.name

            if not record.get("country") and response.country and response.country.name:
                record["country"] = response.country.name

            if not record.get("continent") and response.continent and response.continent.name:
                record["continent"] = response.continent.name

            if not record.get("latLong") and response.location:
                lat = response.location.latitude
                lon = response.location.longitude
                if lat is not None and lon is not None:
                    record["latLong"] = f"{lat},{lon}"

            if not record.get("accuracy_radius") and response.location and response.location.accuracy_radius:
                record["accuracy_radius"] = response.location.accuracy_radius

            break

        except Exception as e:
            logger.warning(f"Failed to lookup IP {ip}: {e}")
            continue

    return record

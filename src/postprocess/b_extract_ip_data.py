import os
import re
import geoip2.database
from utils.logs import get_logger
from utils.regex import IPV4_REGEX, IPV6_REGEX  # Ensure these are defined in your utils.regex

logger = get_logger(__name__)

ASN_PATTERN = re.compile(r'(AS\d+)', re.IGNORECASE)

geoip_path = os.path.join(os.path.dirname(__file__), 'resources', 'GeoLite2-City.mmdb')
asn_path = os.path.join(os.path.dirname(__file__), 'resources', 'GeoLite2-ASN.mmdb')

# Initialize Readers
try:
    geo_reader = geoip2.database.Reader(geoip_path)
except Exception as e:
    logger.error(f"Failed to load GeoLite2-City database: {e}")
    geo_reader = None

try:
    asn_reader = geoip2.database.Reader(asn_path)
except Exception as e:
    logger.error(f"Failed to load GeoLite2-ASN database: {e}")
    asn_reader = None

def _extract_ips_from_text(text, ip_list):
    if not text or not isinstance(text, str):
        return
    found_ipv4 = IPV4_REGEX.findall(text)
    found_ipv6 = IPV6_REGEX.findall(text)
    
    for ip in (found_ipv4 + found_ipv6):
        if ip not in ip_list:
            ip_list.append(ip)

def extract(record):
    existing_asn = record.get("asn")
    
    # Handle ASN logic
    if existing_asn:
        if isinstance(existing_asn, str):
            match = ASN_PATTERN.search(existing_asn.upper())  # Ensure case-insensitivity
            if match:
                record["asn"] = int(match.group(1)[2:])  # Remove "AS" prefix and convert to int
            else:
                del record['asn']
        elif isinstance(existing_asn, int):
            record["asn"] = existing_asn
        else:
            del record['asn']

    # Determine if we need geo or ASN information
    needs_geo = not all(record.get(k) for k in ["city", "state", "country"])
    needs_asn = not record.get("asn")

    # If geo and ASN are both present, return the record early
    if not (needs_geo or needs_asn):
        return record

    # Extract IPs from "line" or "notes" fields if necessary
    ip_list = record.get("ips", [])
    if not ip_list:
        if "line" in record:
            _extract_ips_from_text(record["line"], ip_list)

        if "notes" in record:
            notes_data = record["notes"]
            notes_to_process = [notes_data] if isinstance(notes_data, str) else notes_data
            
            if isinstance(notes_to_process, list):
                for note in notes_to_process:
                    _extract_ips_from_text(note, ip_list)

        if ip_list:
            record['ips'] = ip_list

    # Look up geo data for each IP address
    for ip in ip_list:
        try:
            if geo_reader and needs_geo:
                response = geo_reader.city(ip)
                if not record.get("city") and response.city.name:
                    record["city"] = response.city.name
                if not record.get("state") and response.subdivisions:
                    record["state"] = response.subdivisions.most_specific.name
                if not record.get("country") and response.country.name:
                    record["country"] = response.country.name
                if not record.get("continent") and response.continent.name:
                    record["continent"] = response.continent.name
                # We'll use this in z_get_lat_long later.
                if not record.get("latLong") and response.location.latitude:
                    record["_latLong"] = f"{response.location.latitude},{response.location.longitude}"
                if not record.get("accuracy_radius") and response.location.accuracy_radius:
                    record["_accuracy_radius"] = response.location.accuracy_radius
            break

        except Exception as e:
            continue

    # Look up ASN data for each IP address
    for ip in ip_list:
        try:
            if asn_reader and needs_asn:
                asn_response = asn_reader.asn(ip)
                if asn_response.autonomous_system_number:
                    record["asn"] = asn_response.autonomous_system_number
                if not record.get("isp") and asn_response.autonomous_system_organization:
                    record["isp"] = asn_response.autonomous_system_organization
            break

        except Exception as e:
            continue

    return record
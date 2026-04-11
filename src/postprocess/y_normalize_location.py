from utils.logs import get_logger
import pycountry


logger = get_logger(__name__)

logger.info("Loading Postal address parser, please be patient...")
from postal.parser import parse_address
from postal.normalize import normalize_string
logger.info("Postal address parser loaded successfully.")

US_STATES_SET = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware',
    'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky',
    'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana',
    'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'ohio',
    'oklahoma', 'oregon', 'pennsylvania', 'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas',
    'utah', 'vermont', 'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming', 'washington dc', 'district of columbia',
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky",
    "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh",
    "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
    "puerto rico", "pr", "guam", "gu", "american samoa", "as", "northern mariana islands", "mp", "virgin islands", "vi"
}

def _get_first_string(value):
    if isinstance(value, list) and value:
        value = value[0]
    return str(value).strip() if value is not None else ""

def get_country_code(country_name):
    """
    Given a country name, returns the corresponding ISO 3166-1 alpha-2 country code.
    If no country match is found, returns None.
    """
    try:
        country = pycountry.countries.get(name=country_name)
        if country:
            return country.alpha_2  # Returns the 2-letter code
    except KeyError:
        return None  # Country not found

    return None

def extract(record):
    fields_map = {
        'house_number': 'houseNumber',
        'road': 'road',
        'unit': 'unit',
        'pobox': 'poBox',
        'city': 'city',
        'state': 'state',
        'country': 'country',
        'postcode': 'postcode'
    }
    
    if not any(val in record for val in fields_map.values()):
        return record
    
    # Build context-rich string
    parts = []
    for key in ['address', 'city', 'state', 'country', 'zipCode']:
        val = _get_first_string(record.get(key))
        if val and val.lower() not in ','.join(parts).lower():
            parts.append(val)
            
            # Special case: If state is US and no country exists, append USA
            if key == 'state' and val.lower() in US_STATES_SET:
                if not record.get('country'):
                    parts.append("usa")

    full_string = ", ".join(parts)
    normalized_address = normalize_string(full_string)

    if not normalized_address:
        return record
        
    record['address'] = normalized_address

    # PARSE ONCE: Everything coming out of here is already normalized
    parsed_components = parse_address(normalized_address)

    for value, label in parsed_components:
        if label in fields_map:
            target_key = fields_map[label]
            record[target_key] = value.strip()

    # After parsing, handle country normalization to 2-letter country code
    if 'country' in record:
        country_name = record['country'].lower()
        country_code = get_country_code(country_name)
        if country_code:
            record['country'] = country_code
    
    return record
import re
import phonenumbers
import phonenumbers.geocoder

from utils.logs import get_logger

logger = get_logger(__name__)

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
    return str(value).strip() if value is not None else None

def get_region_context(record):
    country = _get_first_string(record.get("country"))
    if country:
        country_lower = country.lower()
        if country_lower in ["usa", "united states", "united states of america", "us"]:
            return "US"

    state = _get_first_string(record.get("state"))
    if state and state.lower() in US_STATES_SET:
        return "US"
                
    return None

def update_country_in_record(record, phone_number, lang="en"):
    try:
        # Parse the phone number into a PhoneNumber object
        parsed_number = phonenumbers.parse(phone_number)

        country_name = phonenumbers.geocoder.country_name_for_number(parsed_number, lang)
        full_description = phonenumbers.geocoder.description_for_number(parsed_number, lang)
        
        
        if country_name:
            record["country"] = country_name
            
            if full_description and full_description != country_name:                
                # Simple heuristic: If it contains a comma, it's likely "City, State"
                if "," in full_description:
                    parts = [p.strip() for p in full_description.split(",")]
                    if "city" not in record:
                        record["city"] = parts[0]
                    if "state" not in record:
                        record["state"] = parts[1]
                else:
                    # If no comma, it's often just the State/Province
                    record["state"] = full_description

        if parsed_number.country_code:
            region_code = phonenumbers.region_code_for_country_code(parsed_number.country_code)

            if "country" not in record:
                record["country"] = region_code

    except phonenumbers.phonenumberutil.NumberParseException as e:
        logger.error(f"Error parsing phone number: {e}")
    
    return record

def extract(record):
    phone_list = record.get("phoneNumbers")
    if not phone_list:
        return record

    if not isinstance(phone_list, list):
        phone_list = [phone_list]
    
    region_code = get_region_context(record)
    normalized_phones = []

    for raw_phone in phone_list:
        phone_str = _get_first_string(raw_phone)
        if not phone_str:
            continue
            
        parsed_number = None
        
        try:
            parsed_number = phonenumbers.parse(phone_str, region_code)
        except phonenumbers.NumberParseException:
            pass

        if (not parsed_number or not phonenumbers.is_valid_number(parsed_number)) and not phone_str.startswith('+'):
            try:
                retry_str = f"+{phone_str}"
                retry_parsed = phonenumbers.parse(retry_str, None)
                if phonenumbers.is_valid_number(retry_parsed):
                    parsed_number = retry_parsed
            except phonenumbers.NumberParseException:
                pass

        if parsed_number and phonenumbers.is_valid_number(parsed_number):
            normalized = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
            normalized_phones.append(normalized)
            
            # Update country in the record after normalizing phone number if not already set
            record = update_country_in_record(record, normalized)
        else:
            digits = re.sub(r'\D', '', phone_str)
            if digits:
                if phone_str.startswith('+'):
                    normalized_phones.append(f"+{digits}")
                elif len(digits) == 10:
                    normalized_phones.append(f"+1{digits}")
                else:
                    if "line" in record:
                        record["line"] += f' _0tDiscardedPhoneNumber: {phone_str}'

    if normalized_phones:
        record["phoneNumbers"] = list(dict.fromkeys(normalized_phones))
    else:
        del record["phoneNumbers"]

    return record
from utils.logs import get_logger
import pycountry

logger = get_logger(__name__)

# 1. Specific domain mapping for providers that don't use country-specific TLDs
REGIONAL_MAP = {
    "qq.com": "cn", "163.com": "cn", "126.com": "cn", "sina.com": "cn", 
    "aliyun.com": "cn", "139.com": "cn", "21cn.com": "cn",
    "naver.com": "kr", "daum.net": "kr", "hanmail.net": "kr", 
    "nate.com": "kr", "kakao.com": "kr",
    "yandex.com": "ru", "my.com": "ru",
    "gmx.net": "de",
    "btinternet.com": "gb", "sky.com": "gb", "virginmedia.com": "gb"
}

ISO_CORRECTIONS = {
    "UK": "GB",
    "SU": "RU",  # Soviet Union legacy
    "TP": "TL",  # East Timor legacy
}

VANITY_TLDS = {"IO", "AI", "TV", "ME", "FM", "AM", "LY", "CO", "CC", "WS", "TO", "BZ"}

US_TLDS = {"GOV", "MIL", "EDU"}

def extract(record):
    if "country" in record and record["country"]:
        return record

    if "emails" in record and record["emails"]:
        # Ensure emails is a list
        emails = record["emails"]
        if not isinstance(emails, list):
            emails = [emails]

        for email in emails:
            if not isinstance(email, str) or "@" not in email:
                continue

            try:
                domain = email.split("@")[-1].lower()
                
                if domain in REGIONAL_MAP:
                    record["country"] = REGIONAL_MAP[domain]
                    return record

                parts = domain.split(".")
                tld = parts[-1].upper()

                if tld in US_TLDS:
                    record["country"] = "us"
                    return record

                if len(tld) == 2:
                    if tld in VANITY_TLDS:
                        continue

                    search_code = ISO_CORRECTIONS.get(tld, tld)
                    
                    country = pycountry.countries.get(alpha_2=search_code)
                    if country:
                        record["country"] = country.alpha_2.lower()
                        return record
                        
            except Exception as e:
                logger.debug(f"Failed to parse email {email}: {e}")
                continue

    return record
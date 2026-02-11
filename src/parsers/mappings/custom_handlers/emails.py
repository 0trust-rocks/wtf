import re

from utils.logs import get_logger

logger = get_logger(__name__)
from typing import List

emailRe = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

publicEmailDomains = [
    "@gmail.",
    "@yahoo.",
    "@outlook.",
    "@hotmail.",
    "@aol.",
    "@icloud.",
    "@mail.",
    "@protonmail.",
    "@zoho.",
    "@gmx.",
    "@yandex.",
    "@fastmail.",
    "@hushmail.",
    "@tutanota.",
    "@mailfence.",
    "@runbox.",
    "@posteo.",
    "@countermail.",
    "@startmail.",
    "@mailbox.",
    "@lavabit.",
    "@disroot.",
    "@mailbox.",
    "@cock.li",
    "@safe-mail.net",
    "@msn."
]

def extract(emails, original_key: str, original_dict: dict):
    results = []

    if isinstance(emails, str):
        emails = [emails]

    for email in emails:
        if isinstance(email, str) and emailRe.match(email):
            results.append({'emails': email})

            if not any(domain in email for domain in publicEmailDomains):
                results.append({'domain': email.split('@')[-1]})

    return results
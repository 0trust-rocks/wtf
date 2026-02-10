import re

UUID4_REGEX = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$', re.IGNORECASE)
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
URL_ENCODED_EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+%40[a-zA-Z0-9.-]+%2E[a-zA-Z]{2,}')

URL_REGEX = re.compile(r'https?://[^\s]+')
SHA1_REGEX = re.compile(r'\b[a-fA-F0-9]{40}\b')
SHA256_REGEX = re.compile(r'\b[a-fA-F0-9]{64}\b')
SHA512_REGEX = re.compile(r'\b[a-fA-F0-9]{128}\b')
BCRYPT_REGEX = re.compile(r'\$2[aby]?\$\d{2}\$[./A-Za-z0-9]{53}')
IP_REGEX = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')

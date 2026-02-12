"""

TODO:
- search H0 in file
- integration to wtf.py and load_parsers as this processes filepaths (and will process archive files)
- add bom_signatures for utf codec differences (will encounter cyrillic alphabet and other alphabets)

- this may need to go in the parent dir i got confused and mad and will
  work on this later

"""

from urllib.parse import urlsplit
from utils.regex import DOMAIN_REGEX
from utils.logs import get_logger

import os, re

logger = get_logger(__name__)

def _extract_domain(url: str):
    # fuck pep8
    if not url: return None
    if url.startswith("android.app"): return None 

    match = DOMAIN_REGEX.search(url)
    if match: return match.group(0).lower()

    # fallback
    try:                              
        netloc = urlsplit(url).netloc
        return netloc if netloc else None
    except: return None

def _format_android_url(raw_url: str):
    try:
        package_name = raw_url.split("@")[-1]
        package_name = package_name.replace("-", "").replace("_", "").replace(".", "")
        package_name = ".".join(package_name.split("/")[::-1])
        package_name = ".".join(package_name.split(".")[::-1])
        return f"{package_name}android.app"
    except Exception: 
        return raw_url

def get_itr(file_path: str): # H0: add original_key & original_dict. idk wtf they do yet but base_parser.py for reference
    try:
        contents = open(file_path, "r", encoding="utf-8", errors="ignore").read()
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        return

    if "===============" in contents: entries = contents.split("===============")
    else: entries = contents.split("\n\n")

    for entry in entries:
        if not entry.strip(): continue

        lines = entry.strip().split("\n")
        url = user = password = ""

        for line in lines:
            clean_line = line.lower().strip()
                
            if clean_line.startswith(("url:", "host:", "hostname:")):
                parts = line.split(":", 1)
                if len(parts) > 1: url = parts[1].strip()
                
            elif clean_line.startswith(("user:", "login:", "username:", "user login:", "username login:")):
                parts = line.split(":", 1)
                if len(parts) > 1: user = parts[1].strip() if user.lower() != "unknown" else ""
                
            elif clean_line.startswith(("pass:", "password:", "user password:", "pass password:")):
                parts = line.split(":", 1)
                if len(parts) > 1: password = parts[1].strip()
            
        if url:
            if url.startswith("android"):
                url = self._format_android_url(url)
            else:
                try:
                    if not url.startswith(("http", "ftp", "file")):
                        # fix scheme
                        url_components = urlsplit(f"http://{url}")            
                        if not url_components.netloc and url_components.path:
                            url = f"http://{url_components.path}"
                        else:
                            url = f"{url_components.scheme}://{url_components.netloc}"
                    else:
                        url_components = urlsplit(url)
                        url = f"{url_components.scheme}://{url_components.netloc}"
                except Exception:     pass

        domain = self._extract_domain(url)

        if url or user or password:
            results = {}
            results["links"] =     [url] if url else []
            results["usernames"] = [user] if user else [],
            results["passwords"] = [password] if password else [],
            results["domain"] = domain

            return results

def extract():
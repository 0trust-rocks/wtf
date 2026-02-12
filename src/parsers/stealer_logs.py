from urllib.parse import urlsplit
from utils.regex  import DOMAIN_REGEX
from utils.logs   import get_logger
from .base_parser import BaseParser

import os, re

logger = get_logger(__name__)

class StealerLogParser(BaseParser):
    _EXTENSIONS = [".txt"]

    def _extract_domain(self, url):
        if not url: return None
        
        match = DOMAIN_REGEX.search(url)
        if match:
            return match.group(0).lower()
        return None

    def get_itr(self):
        try:
            with open(self.file_path, "r", encoding="utf-8", errors="ignore") as f:
                contents = f.read()
        except Exception as e:
            logger.error(f"Failed to read file {self.file_path}: {e}")
            return

        mad_underscores = "==============="
        if mad_underscores in contents: entries = contents.split(mad_underscores)
        else: entries = contents.split("\n\n")

        for entry in entries:
            if not entry.strip():
                continue

            lines = entry.strip().split("\n")
            url, user, password = "", "", ""

            for line in lines:
                if line.startswith(("URL:", "url:", "Url:", "Host:", "HOSTNAME:")):
                    parts = line.split(":", 1)
                    if len(parts) > 1: url = parts[1].strip()
                
                elif line.startswith(("USER:", "login:", "Login", "Username", "USER LOGIN:")):
                    parts = line.split(":", 1)
                    if len(parts) > 1: user = parts[1].strip() if user.lower() != "unknown" else "" # leave as "", not None, needs to stay str bc record.py specifies it
                
                elif line.startswith(("PASS:", "password:", "Password", "USER PASSWORD:", "USER PASSWORD")):
                    parts = line.split(":", 1)
                    if len(parts) > 1: password = parts[1].strip()

            if url:
                if url.startswith("android"):
                    try:
                        package_name = url.split("@")[-1]
                        package_name = package_name.replace("-", "").replace("_", "").replace(".", "")
                        package_name = ".".join(package_name.split("/")[::-1])
                        package_name = ".".join(package_name.split(".")[::-1])
                        url = f"{package_name}android.app"
                    except:
                        pass
                else:
                    try:
                        if not url.startswith(("http", "ftp", "file://")):
                            url_components = urlsplit(f"http://{url}")
                            if not url_components.netloc and url_components.path:
                                url = f"http://{url_components.path}"
                            else:
                                url = f"{url_components.scheme}://{url_components.netloc}"
                        else:
                             url_components = urlsplit(url)
                             url = f"{url_components.scheme}://{url_components.netloc}"
                    except Exception:
                        pass

            domain = self._extract_domain(url)

            if url or user or password:
                results = {}
                results["links"] =     [url] if url else []
                results["usernames"] = [user] if user else [],
                results["passwords"] = [password] if password else [],
                results["domain"] = domain

                yield results
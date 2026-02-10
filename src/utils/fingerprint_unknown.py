import os
import json

from utils.logs import logger

def fingerprint_type(file_path):
        if os.path.getsize(file_path) > 512 * 1024 * 1024:
            return "large_text"

        # Load to memory
        with open(file_path, 'r') as f:
            content = f.read()

        # Check for common structured formats
        if content.lstrip().startswith('{') or content.lstrip().startswith('['):
            # Attempt to parse as JSON
            try:
                json.loads(content)
                return "json"
            except:
                pass
        # Check for XML
        if content.lstrip().startswith('<'):
            logger.debug("File starts with <, likely XML")
            return "xml"
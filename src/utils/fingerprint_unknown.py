import os
import json
import mimetypes

from utils.logs import logger

def fingerprint_type(file_path):
        mime = mimetypes.guess_type(file_path)[0]

        if mime:
            logger.debug(f"Guessed MIME type: {mime} for file: {file_path}")
            if mime.startswith("text/csv") or mime.startswith("text/tab-separated-values"):
                return "csv"
            if mime.startswith("text/"):
                return "text"
            elif mime == "application/json":
                return "json"
            elif mime in ["application/xml", "text/xml"]:
                return "xml"
            elif mime in ["application/x-ndjson", "application/jsonl"]:
                return "ndjson"

        # if os.path.getsize(file_path) > 512 * 1024 * 1024:
        #     return "large_text"
        


        # # Load to memory
        # with open(file_path, 'r') as f:
        #     content = f.read()

        # # Check for common structured formats
        # if content.lstrip().startswith('{') or content.lstrip().startswith('['):
        #     # Attempt to parse as JSON
        #     try:
        #         json.loads(content)
        #         return "json"
        #     except:
        #         pass
        # # Check for XML
        # if content.lstrip().startswith('<'):
        #     logger.debug("File starts with <, likely XML")
        #     return "xml"
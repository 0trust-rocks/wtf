import mimetypes
import magic

from utils.logs import get_logger

logger = get_logger(__name__)

def fingerprint_type(file_path):
    try:
        blob = open(file_path, 'rb').read(2048)
        mime = magic.from_buffer(blob, mime=True)
        logger.debug(f"Magic detected MIME type: {mime} for file: {file_path}")
    except Exception as e:
        logger.warning(f"Magic failed, falling back to mimetypes: {e}")
        mime = mimetypes.guess_type(file_path)[0]

    if not mime:
        return "unknown"

    # 2. Map the MIME types to your internal labels
    # Magic correctly identifies CSVs even if they are named .txt
    if mime == "text/csv" or mime == "text/tab-separated-values":
        return "csv"
    
    if mime == "application/json":
        return "json"
    
    if mime in ["application/xml", "text/xml"]:
        return "xml"
    
    if mime in ["application/x-ndjson", "application/jsonl"]:
        return "ndjson"

    if mime.startswith("text/"):
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                snippet = f.read(1024).lstrip()
                if snippet.startswith(('{', '[')):
                    return "json"
                if snippet.startswith('<'):
                    return "xml"
        except Exception:
            pass
            
        return "text"

    return "unknown"
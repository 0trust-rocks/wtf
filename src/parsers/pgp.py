from datetime import datetime
from parsers.base_parser import BaseParser

from ir.record import Record
from utils.logs import get_logger
from utils.regex import PGPPGP_UID_REGEX # can get more than just email, name, groups with keys but get this working first

import re, logging, pgpy

logger = get_logger(__name__)

class PGPParser(BaseParser):
    _EXTENSIONS = ['.asc', '.pub', '.pgp', '.gpg', '.key']

    def get_itr(self):
        # iterates through a file containing PGP Public Key Blocks

        logger.info(f"Starting PGP conversion for {self.file_path}")

        try:
            with open(self.file_path, 'r', encoding='utf-8', errors='replace') as f: content = f.read()
        except Exception as e:
            logger.error(f"Failed to read {self.file_path}: {e}")
            return

        key_blocks = re.findall(r"-----BEGIN PGP PUBLIC KEY BLOCK-----.*?-----END PGP PUBLIC KEY BLOCK-----", content, re.DOTALL)

        if not key_blocks:
            logger.warning(f"No key blocks found in {self.file_path}")
            return

        # parse individually to handle the binary packet structure
        for block in key_blocks:
            try:
                key, _ = pgpy.PGPKey.from_blob(block)
                ir = Record()
                ir.add_or_set_value("source", "PGP Public Key")
                ir.add_or_set_value("notes", f"Fingerprint: {key.fingerprint}")
                ir.add_or_set_value("notes", f"Key Algorithm: {key.pubkey_algorithm.name}")
                if key.created:
                    ir.recencyYear =  key.created.year
                    ir.recencyMonth = key.created.month
                    ir.recencyDay =   key.created.day

                # key.userids returns a list of "PGPUID" objects which resolve to str
                for uid in key.userids: self._parse_user_id(str(uid), ir)
                yield ir.to_dict()

            except Exception as e:
                logger.error(f"Error parsing specific PGP block in {self.file_path}: {e}")
                continue

    def _parse_user_id(self, uid_string, record: Record):
        match = self.PGP_UID_REGEX.match(uid_string)
        if not match:
            # fallback: just treat the whole thing as a note or name if regex fails
            record.add_or_set_value("notes", f"Raw UID: {uid_string}")
            return

        raw_name = match.group(1)
        comment =  match.group(3)
        email =    match.group(5)

        if email:   record.add_or_set_value("emails", email)
        if comment: record.add_or_set_value("notes", f"ID Comment: {comment}")

        if raw_name:
            raw_name = raw_name.strip()
            if raw_name:
                parts = raw_name.split()
                if len(parts) == 1:
                    if not record.firstName: # only set if not already set by a primary UID
                        record.firstName = parts[0]
                    else:
                        record.add_or_set_value("usernames", parts[0]) # secondary names as aliases (pgp allows multiple names, emails, etc)
                
                elif len(parts) == 2:
                    # first Last
                    if not record.firstName:
                        record.firstName = parts[0]
                        record.lastName = parts[1]
                    else:
                        record.add_or_set_value("notes", f"Alias: {raw_name}")

                elif len(parts) > 2:
                    # first middle last
                    if not record.firstName:
                        record.firstName = parts[0]
                        record.lastName = parts[-1]
                        record.middleName = " ".join(parts[1:-1])
                    else:
                        record.add_or_set_value("notes", f"Alias: {raw_name}") # remember this is raw, i know "alias" will catch your eyes and wonder why i didnt put under "username" like the last/like i should've
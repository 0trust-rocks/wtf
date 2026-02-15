import re
import logging
import pgpy
from datetime import datetime

from ir.record import Record
from utils.logs import get_logger
from utils.regex import PGP_UID_REGEX
from parsers.base_parser import BaseParser

logger = get_logger(__name__)

class PGPParser(BaseParser):
    _EXTENSIONS = ['.asc', '.pub', '.pgp', '.gpg', '.key']

    def get_itr(self):
        """
        extracts primary identity, subkeys, signatures, and cryptographic metadata
        """
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

        for block in key_blocks:
            try:
                # pgpy handles the binary packet unpacking from the ASCII armor
                key, _ = pgpy.PGPKey.from_blob(block)
                ir = Record()
                ir.add_or_set_value("source", self.file_path[:20]) # truncate to 20char just incase we somehow try to ingest a filename with like 400 chars and it just freaks out or smth
                ir.add_or_set_value("notes", f"Fingerprint: {key.fingerprint}")
                ir.add_or_set_value("notes", f"Key ID: {key.magicid}")
                ir.add_or_set_value("notes", f"Key Algorithm: {key.pubkey_algorithm.name}")
                ir.add_or_set_value("notes", f"Key Size: {key.key_size} bits")
                
                # creation/revokation
                if key.created:
                    ir.recencyYear = key.created.year
                    ir.recencyMonth = key.created.month
                    ir.recencyDay = key.created.day
                    ir.add_or_set_value("notes", f"Created At: {key.created.isoformat()}")
                if key.expires:    ir.add_or_set_value("notes", f"Expires At: {key.expires.isoformat()}")
                if key.is_revoked: ir.add_or_set_value("notes", "Status: REVOKED")

                # key.userids returns a list of "PGPUID" objects which resolve to str
                for uid in key.userids: 
                    self._parse_user_id(str(uid), ir)

                # subkeys
                for subkey_id in key.subkeys:
                    sk = key.subkeys[subkey_id]
                    sk_meta = f"Subkey [{sk.magicid}] {sk.pubkey_algorithm.name} ({sk.key_size} bits)"
                    ir.add_or_set_value("notes", sk_meta)
                    if sk.created:
                        ir.add_or_set_value("notes", f"Subkey {sk.magicid} Created: {sk.created.isoformat()}")

                # sigs/trust metadata
                # rxtract Key IDs of people who have signed this key
                unique_signers = {sig.keyid for sig in key.signatures if sig.keyid}
                if unique_signers:
                    ir.add_or_set_value("notes", f"Certified by KeyIDs: {', '.join(unique_signers)}")

                yield ir.to_dict()

            except Exception as e:
                logger.error(f"Error parsing specific PGP block in {self.file_path}: {e}")
                continue

    def _parse_user_id(self, uid_string, record: Record):
        match = self.PGP_UID_REGEX.match(uid_string)
        if not match:
            record.add_or_set_value("notes", f"Raw UID: {uid_string}")
            return

        raw_name = match.group(1)
        comment =  match.group(3)
        email =    match.group(5)

        if email:   record.add_or_set_value("emails", email)
        if comment: record.add_or_set_value("notes", f"ID Comment: {comment}")

        if raw_name:
            raw_name = raw_name.strip()
            if not raw_name: return
            # if we domt have a name yet, populate the primary fields
            if not record.firstName:
                parts = raw_name.split()
                if len(parts) == 1:
                    record.firstName = parts[0]
                elif len(parts) == 2:
                    record.firstName = parts[0]
                    record.lastName = parts[1]
                elif len(parts) > 2:
                    record.firstName = parts[0]
                    record.lastName = parts[-1]
                    record.middleName = " ".join(parts[1:-1])
            else:
                # if primary name is set, treat additional uids as aliases/usernames
                record.add_or_set_value("usernames", raw_name)
                # record.add_or_set_value("notes", f"Raw PGP Name Unknown: {raw_name}")
                # change if u want ^
"""
IMAP Manager — handles all IMAP operations.
All actions are performed on the real mail server (source of truth).
"""
import imaplib
import email
import email.utils
import logging
import re
from dataclasses import dataclass
from datetime import datetime

from src.rules.engine import EmailContext

logger = logging.getLogger(__name__)

PROCESSED_FLAG = "X-MailIA-Processed"


class FolderNotSelectable(Exception):
    """SELECT refused the mailbox — it does not exist, or cannot be opened."""


class InvalidFolderName(ValueError):
    """A blank target would destroy mail: GreenMail answers OK to `COPY <uid> ""`
    and discards the message, so no status check downstream can catch it."""


class InvalidUid(ValueError):
    """A UID that is not a plain number would be an RFC 3501 sequence set.

    "1:*" and "1,2,3" are valid IMAP *sets*: passed to an endpoint documented as acting
    on one message, they act on the whole folder. Anything meaning "one message" must
    therefore refuse everything but digits.
    """


def _check_uid(uid, quoi: str = "uid") -> str:
    u = str(uid).strip()
    # RFC 3501 : les UID commencent a 1 ; "0" n'est pas un message
    if not u.isdigit() or u.lstrip("0") == "" or u != u.lstrip("0"):
        raise InvalidUid(
            f"{quoi} '{uid}' is not a single message id — ranges and sets such as "
            "'1:*' or '1,2,3' are refused here"
        )
    return u


def _check_uid_list(uids, quoi: str = "uids") -> list[str]:
    """Every element of a bulk list must itself be a single UID.

    Guarding only the unitary entry points would leave the hole open: one range slipped
    into a list reaches the very same IMAP command.
    """
    if not isinstance(uids, (list, tuple)) or not uids:
        raise InvalidUid(f"{quoi}: a non-empty list of message ids is required")
    return [_check_uid(u, quoi) for u in uids]


class ImapInjection(ValueError):
    """A value that would break out of the IMAP command it is embedded in."""


class MessageGone(Exception):
    """Le message designe n'est plus la : rien n'a ete deplace ni supprime.

    Distinct d'un echec serveur. En concurrence, deux clients demandent le meme
    deplacement, un seul agit — et le second doit l'apprendre. Sans ca les deux
    recevaient « deplace », le perdant croyait avoir travaille, et cette illusion
    masquait les vraies duplications."""


class NoTrashFolder(Exception):
    """No Trash folder, so deleting would purge instead of moving.

    Refused rather than performed: destruction must be asked for, not suffered.
    Creating a Trash folder unprompted was the alternative — rejected, see the
    report: on a server whose bin has an unexpected name it would split the user's
    deleted mail across two folders, silently."""


class InvalidFlag(ValueError):
    """A flag name outside the finite set of IMAP system flags.

    Unlike a search criteria, which is free by design, flags are a closed set:
    we allow what is valid instead of refusing what is dangerous."""


def _imap_astring(value: str, quoi: str = "value") -> str:
    """Quote a SEARCH *value* the way _imap_quote does for folder names.

    `f'FROM "{value}"'` without escaping lets a value close the quote and start a new
    command: a CRLF ends the line on the wire and what follows runs in the authenticated
    session. Demonstrated with `rien@x.invalid"\r\nA042 CREATE temoin`.

    CR/LF/NUL cannot appear in an IMAP quoted string at all, so they are refused rather
    than escaped; the quote and the backslash are escaped.
    """
    v = "" if value is None else str(value)
    if re.search(r"[\x00-\x1f\x7f]", v):
        raise ImapInjection(f"{quoi} contains a control character (IMAP command injection)")
    if len(v) > 512:
        raise ImapInjection(f"{quoi} is too long ({len(v)} characters, 512 max)")
    escaped = v.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _imap_criteria(expr: str, quoi: str = "criteria") -> str:
    """Guard a caller-supplied SEARCH *expression*.

    An expression cannot be escaped — the caller writes its structure by design. All that
    can be done is to refuse what turns one command into two, and to bound the length: a
    very long criteria string (`"OR " * 400`) drops the GreenMail session outright.
    """
    v = "" if expr is None else str(expr)
    if not v.strip():
        raise ImapInjection(f"{quoi} is empty")
    if re.search(r"[\x00-\x1f\x7f]", v):
        raise ImapInjection(f"{quoi} contains a control character (IMAP command injection)")
    if len(v) > 1024:
        raise ImapInjection(f"{quoi} is too long ({len(v)} characters, 1024 max)")
    if v.count('"') % 2:
        raise ImapInjection(f"{quoi} has an unbalanced quote")
    return v


def _check_target(folder: str, quoi: str = "target folder") -> str:
    """A folder name must name a folder, not the root of the hierarchy.

    `COPY <uid> ""` is answered OK by some servers, which discard the message; and
    `RENAME "." "X"` renames the whole tree — a name made only of separators designates
    the root. `..` does it too, so comparing against the separator is not enough, and
    the separator itself is server-dependent and sometimes misreported.
    """
    if not isinstance(folder, str) or not folder.strip():
        raise InvalidFolderName(f"{quoi} is empty — refusing, this would destroy the message")
    if re.search(r"[\x00-\x1f\x7f]", folder):
        raise InvalidFolderName(f"{quoi} contains a control character (IMAP command injection)")
    if any(c in folder for c in "*%"):
        raise InvalidFolderName(f"{quoi} '{folder}' contains an IMAP wildcard (* or %)")
    if not folder.strip().strip("./ \t\r\n").strip():
        raise InvalidFolderName(
            f"{quoi} '{folder}' designates the hierarchy root, not a folder — refusing"
        )
    for segment in re.split(r"[./]", folder.strip()):
        if not segment.strip():
            raise InvalidFolderName(f"{quoi} '{folder}' has an empty path segment")
    return folder


def _imap_quote(folder: str) -> str:
    """Quote folder name for IMAP commands (RFC 3501).

    Auto-encodes UTF-8 folder names to IMAP modified UTF-7 if they contain
    non-ASCII characters. Already-encoded names (pure ASCII) pass through.
    """
    if folder.startswith('"') and folder.endswith('"'):
        return folder
    # If folder contains non-ASCII chars, it's UTF-8 and needs IMAP UTF-7 encoding
    if any(ord(c) > 127 for c in folder):
        folder = _encode_imap_utf7(folder)
    escaped = folder.replace('\\', '\\\\').replace('"', '\\"')
    return f'"{escaped}"'


@dataclass
class IMAPConfig:
    host: str
    port: int
    ssl: bool
    user: str
    password: str


class IMAPManager:
    """Manages IMAP connection and operations for a single mail account."""

    def __init__(self, config: IMAPConfig):
        self.config = config
        self._conn: imaplib.IMAP4_SSL | imaplib.IMAP4 | None = None
        # (dossier, readonly) actuellement selectionne. La sûrete de `reutiliser_selection`
        # tient a ce qu'AUCUN IMAPManager n'est partage : `get_imap()` rend une instance
        # neuve a chaque appel et les 26 instanciations cote API sont a portee locale.
        # ATTENTION : introduire un pool de connexions rend ce marqueur concurrent, et un
        # FETCH partirait sur le dossier d'une autre requete — sans que rien ne le montre,
        # les UID etant propres a chaque boite. Qui ajoute un pool doit d'abord proteger
        # ce marqueur, ou retirer `reutiliser_selection`.
        #
        # Un pool casse AUSSI la protection de fait sur les deplacements concurrents,
        # et la marge est plus mince qu'il n'y parait. Mesures sur Dovecot :
        #
        #   fenetre de duplication : deux UID MOVE a moins de ~0,5 ms l'un de l'autre
        #     dupliquent le message ; au-dela de 1 ms, jamais. C'est la duree de la
        #     commande, pas celle de l'instantane de session — une session qui garde sa
        #     vue 10 s ne duplique pas, le serveur refuse un UID deja expurge.
        #   ce qui protege aujourd'hui : la VARIANCE du preambule par requete (connexion
        #     IMAP + LOGIN + SELECT), ecart-type ~5 ms. Meme avec des requetes lancees
        #     exactement en meme temps, les MOVE arrivent espaces de ~2 ms.
        #   marge reelle : facteur 2 a 3, et elle plafonne vers 1 ms quand la concurrence
        #     augmente (24 clients : 0,93 ms mesure) — le preambule ralentit sous charge,
        #     ce qui reetale les requetes. On frole le bord sans le franchir.
        #
        # Un pool supprime ce preambule, donc sa variance, donc la seule protection.
        # Le verrou applicatif, ecarte aujourd'hui faute de cas atteignable, deviendrait
        # alors necessaire.
        self._selection: tuple[str, bool] | None = None
        self._capacites: set[str] = set()

    def connect(self):
        if self.config.ssl:
            self._conn = imaplib.IMAP4_SSL(self.config.host, self.config.port, timeout=30)
        else:
            self._conn = imaplib.IMAP4(self.config.host, self.config.port, timeout=30)
        try:
            self._conn.login(self.config.user, self.config.password)
        except imaplib.IMAP4.error:
            # Fallback to AUTHENTICATE PLAIN for non-ASCII passwords
            import base64
            auth_string = f"\x00{self.config.user}\x00{self.config.password}"
            self._conn.authenticate("PLAIN", lambda _: auth_string.encode("utf-8"))
        self._selection = None
        # imaplib garde les capacites annoncees AVANT authentification : sur Dovecot,
        # MOVE et UIDPLUS n'y figurent pas alors que le serveur les supporte. On redemande.
        try:
            typ, data = self._conn.capability()
            self._capacites = set(data[0].decode().upper().split()) if typ == "OK" and data[0] else set()
        except Exception:
            self._capacites = set()
        logger.info(f"Connected to {self.config.host} as {self.config.user}")

    def disconnect(self):
        if self._conn:
            try:
                self._conn.logout()
            except Exception:
                pass
            self._conn = None
        self._selection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def list_folders(self) -> list[dict]:
        """List all IMAP folders with their separator."""
        import re
        status, data = self._conn.list()
        folders = []
        for item in data:
            if isinstance(item, bytes):
                # IMAP LIST format: (\\flags) "sep" "folder_name"
                match = re.match(rb'\(([^)]*)\)\s+"(.+?)"\s+(.*)', item)
                if match:
                    flags = match.group(1).decode()
                    sep = match.group(2).decode()
                    raw_name = match.group(3).decode().strip('"')
                    folders.append({
                        "name": raw_name,
                        "display_name": _decode_imap_utf7(raw_name),
                        "separator": sep,
                        "flags": flags,
                    })
        return folders

    def _select(self, folder: str, readonly: bool = False):
        """SELECT a mailbox, failing loudly.

        imaplib drops back to AUTH state when SELECT fails, so the *next* command dies
        with "illegal in state AUTH" — a message that names neither the folder nor the
        real cause. Every mutating path goes through here to get a usable error.
        """
        typ, data = self._conn.select(_imap_quote(folder), readonly=readonly)
        if typ != "OK":
            self._selection = None
            raise FolderNotSelectable(f"Cannot open folder '{folder}' — check that it exists")
        # Tout SELECT passe par ici : le marqueur reste donc exact quoi que fasse
        # l'appelant, tant qu'il ne parle pas a `_conn` directement.
        self._selection = (folder, readonly)
        return data

    def get_uids(self, folder: str = "INBOX", since_uid: str | None = None) -> list[str]:
        """Get message UIDs in a folder, optionally since a given UID."""
        self._select(folder, readonly=True)
        if since_uid:
            criteria = f"UID {int(since_uid) + 1}:*"
            status, data = self._conn.uid("SEARCH", None, criteria)
        else:
            status, data = self._conn.uid("SEARCH", None, "ALL")

        if status != "OK":
            return []
        uids = data[0].decode().split() if data[0] else []
        # Filter out UIDs <= since_uid (IMAP search can return the boundary)
        if since_uid:
            uids = [u for u in uids if int(u) > int(since_uid)]
        # RFC 3501 does not guarantee SEARCH ordering, and callers advance their
        # sync cursor to the last UID of a batch — an unsorted reply would skip mail
        return sorted(uids, key=int)

    def fetch_email(self, uid: str, folder: str = "INBOX",
                    reutiliser_selection: bool = False) -> EmailContext | None:
        """Fetch and parse a single email by UID. None if the folder or the UID is gone.

        `reutiliser_selection` fait sauter le SELECT quand le dossier est deja celui
        ouvert : la boucle de synchronisation enchaine des centaines d'emails du meme
        dossier, et un EXAMINE par email doublait le nombre d'allers-retours — donc le
        temps de synchronisation sur un serveur distant, ou tout est latence.

        La reutilisation est **explicite et jamais deduite** : un marqueur partage que
        l'appelant ne declare pas serait faux des qu'un autre chemin selectionne
        ailleurs, et un FETCH sur le mauvais dossier ne se voit pas — les UID sont
        propres a chaque boite, un meme numero y designe un autre message.
        """
        _check_uid(uid)
        if not (reutiliser_selection and getattr(self, "_selection", None) == (folder, True)):
            # An unchecked SELECT leaves the connection in AUTH state and the FETCH then
            # fails with the opaque "command FETCH illegal in state AUTH"
            sel_status, _ = self._conn.select(_imap_quote(folder), readonly=True)
            if sel_status != "OK":
                self._selection = None
                logger.warning(f"Cannot select folder {folder}")
                return None
            self._selection = (folder, True)
        status, data = self._conn.uid("FETCH", uid, "(RFC822)")
        if status != "OK" or not data or data[0] is None:
            return None

        raw = data[0][1]
        msg = email.message_from_bytes(raw)

        body_text = ""
        has_attachments = False
        attachment_names = []

        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))

            if "attachment" in disposition:
                has_attachments = True
                filename = part.get_filename() or "unnamed"
                attachment_names.append(filename)
            elif content_type == "text/plain" and not body_text:
                try:
                    payload = part.get_payload(decode=True)
                except Exception:
                    payload = part.get_payload(decode=False)
                    if isinstance(payload, str):
                        payload = payload.encode("utf-8", errors="replace")
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    try:
                        body_text = payload.decode(charset, errors="replace")
                    except (UnicodeDecodeError, LookupError):
                        body_text = payload.decode("utf-8", errors="replace")

        date_str = msg.get("Date", "")
        try:
            date_tuple = email.utils.parsedate_to_datetime(date_str)
            date_formatted = date_tuple.strftime("%Y-%m-%d %H:%M")
        except Exception:
            date_formatted = date_str

        # str() avant parseaddr : ces en-tetes aussi peuvent revenir en objet Header
        from_addr = email.utils.parseaddr(str(msg.get("From", "") or ""))[1]
        to_addr = email.utils.parseaddr(str(msg.get("To", "") or ""))[1]

        return EmailContext(
            uid=uid,
            folder=folder,
            from_addr=from_addr,
            to_addr=to_addr,
            subject=_decode_header(msg.get("Subject", "")),
            body_text=body_text,
            has_attachments=has_attachments,
            attachment_names=attachment_names,
            date=date_formatted,
            message_id=str(msg.get("Message-ID", "") or "").strip(),
            references=str(msg.get("References", "") or "").strip(),
        )

    def fetch_raw(self, uid: str, folder: str = "INBOX") -> bytes | None:
        """Fetch raw email bytes (for attachment extraction)."""
        _check_uid(uid)
        self._select(folder, readonly=True)
        status, data = self._conn.uid("FETCH", uid, "(RFC822)")
        if status != "OK" or not data or data[0] is None:
            return None
        return data[0][1]

    # --- Write operations (modify mail server state) ---

    def create_folder(self, folder: str) -> bool:
        """Create a new IMAP folder and subscribe to it."""
        _check_target(folder, "folder")
        status, data = self._conn.create(_imap_quote(folder))
        if status == "OK":
            self._conn.subscribe(_imap_quote(folder))
            logger.info(f"Created and subscribed folder: {folder}")
            return True
        resp = str(data) if data else ""
        if "ALREADYEXISTS" in resp or "already exists" in resp.lower():
            logger.debug(f"Folder already exists: {folder}")
            return True
        logger.error(f"Failed to create folder: {folder}")
        return False

    def delete_folder(self, folder: str) -> bool:
        """Delete an IMAP folder (must be empty or server empties it)."""
        _check_target(folder, "folder")
        # Unsubscribe first
        self._conn.unsubscribe(_imap_quote(folder))
        status, _ = self._conn.delete(_imap_quote(folder))
        if status == "OK":
            logger.info(f"Deleted folder: {folder}")
            return True
        logger.error(f"Failed to delete folder: {folder}")
        return False

    def rename_folder(self, old_name: str, new_name: str) -> bool:
        """Rename/move an IMAP folder."""
        _check_target(old_name, "old_name")
        _check_target(new_name, "new_name")
        status, _ = self._conn.rename(_imap_quote(old_name), _imap_quote(new_name))
        if status == "OK":
            self._conn.subscribe(_imap_quote(new_name))
            logger.info(f"Renamed folder: {old_name} -> {new_name}")
            return True
        logger.error(f"Failed to rename folder: {old_name} -> {new_name}")
        return False

    def _rollback_copy(self, copy_data, to_folder: str) -> None:
        """Remove a copy left in the target folder by a failed move (needs UIDPLUS COPYUID)."""
        copied_uids = None
        for part in copy_data or []:
            text = part.decode(errors="replace") if isinstance(part, bytes) else str(part)
            match = re.search(r"COPYUID \d+ \S+ (\S+?)[\]\s]", text)
            if match:
                copied_uids = match.group(1)
                break
        if not copied_uids:
            logger.error(f"Cannot roll back copy in {to_folder}: no COPYUID returned by the server")
            return
        try:
            self._select(to_folder)
            self._conn.uid("STORE", copied_uids, "+FLAGS", "(\\Deleted)")
            self._conn.expunge()
            logger.info(f"Rolled back orphan copy {copied_uids} in {to_folder}")
        except Exception as e:
            logger.error(f"Rollback of copy {copied_uids} in {to_folder} failed: {e}")

    def _oublier_copyuid(self) -> None:
        """A appeler AVANT un COPY ou un MOVE : `untagged_responses` s'accumule, et un
        COPYUID laisse par une commande precedente se lirait comme un succes."""
        try:
            self._conn.untagged_responses.pop("COPYUID", None)
        except Exception:
            pass

    def _copyuid(self, reponse) -> str | None:
        """UID attribues dans la cible, d'apres COPYUID (UIDPLUS).

        Son absence est la seule preuve fiable que rien n'a bouge : `UID COPY 999999`
        et `UID MOVE 999999` repondent tous deux `OK` sur les deux serveurs testes.

        Deux emplacements selon la commande, mesures et non supposes :
        - `UID COPY` le met dans la reponse **taguee** ;
        - `UID MOVE` (RFC 6851) dans une reponse **non taguee** `* OK [COPYUID ...]`,
          qu'imaplib range dans `untagged_responses`.
        """
        for part in reponse or []:
            texte = part.decode(errors="replace") if isinstance(part, bytes) else str(part)
            m = re.search(r"COPYUID \d+ \S+ (\S+?)[\]\s]", texte)
            if m:
                return m.group(1)
        try:
            brut = self._conn.untagged_responses.get("COPYUID")
        except Exception:
            brut = None
        for part in brut or []:
            texte = part.decode(errors="replace") if isinstance(part, bytes) else str(part)
            champs = texte.strip().rstrip("]").split()
            if len(champs) >= 3:
                return champs[2]
        return None

    def _uid_existe(self, uid: str) -> bool:
        """Repli pour les serveurs sans UIDPLUS : un aller-retour de plus, mais un
        « deplace » annonce a tort coute plus cher qu'un SEARCH."""
        try:
            status, data = self._conn.uid("SEARCH", None, f"UID {uid}")
            return status == "OK" and bool(data and data[0] and data[0].split())
        except Exception:
            return True  # dans le doute, on laisse la commande decider

    def move_email(self, uid: str, from_folder: str, to_folder: str) -> bool:
        """Move an email to another folder via IMAP. Creates folder if needed."""
        _check_uid(uid)
        _check_target(to_folder)
        if to_folder.strip() == (from_folder or "").strip():
            raise InvalidFolderName(f"source and target are the same folder ('{from_folder}')")
        self._select(from_folder)
        # Ensure target folder exists
        self._conn.create(_imap_quote(to_folder))

        avec_uidplus = "UIDPLUS" in self._capacites
        if not avec_uidplus and not self._uid_existe(uid):
            raise MessageGone(f"Message {uid} is no longer in '{from_folder}'")

        if "MOVE" in self._capacites:
            # RFC 6851 : une seule commande au lieu de trois. Reduit la fenetre de course
            # et divise par trois les allers-retours. Ne la SUPPRIME pas : deux MOVE
            # simultanes sur le meme UID dupliquent encore sur Dovecot (mesure), chaque
            # session gardant sa propre vue tant qu'elle n'a pas recu l'EXPUNGE.
            self._oublier_copyuid()
            status, move_data = self._conn.uid("MOVE", uid, _imap_quote(to_folder))
            if status != "OK":
                logger.error(f"MOVE failed for UID {uid} to {to_folder}")
                return False
            if avec_uidplus and self._copyuid(move_data) is None:
                raise MessageGone(f"Message {uid} is no longer in '{from_folder}'")
            self._selection = None  # le serveur a expunge sous nous
            logger.info(f"Moved UID {uid}: {from_folder} -> {to_folder}")
            return True

        # Copy then delete (MOVE not supported everywhere)
        self._oublier_copyuid()
        status, copy_data = self._conn.uid("COPY", uid, _imap_quote(to_folder))
        if avec_uidplus and status == "OK" and self._copyuid(copy_data) is None:
            raise MessageGone(f"Message {uid} is no longer in '{from_folder}'")
        if status != "OK":
            logger.error(f"Failed to copy UID {uid} to {to_folder}")
            return False
        # The copy already exists in the target folder: a failed STORE would
        # silently duplicate the message, so undo the copy instead.
        try:
            status, _ = self._conn.uid("STORE", uid, "+FLAGS", "(\\Deleted)")
        except Exception as e:
            logger.error(f"STORE \\Deleted failed for UID {uid} in {from_folder}: {e}")
            status = "NO"
        if status != "OK":
            self._rollback_copy(copy_data, to_folder)
            return False
        self._conn.expunge()
        logger.info(f"Moved UID {uid}: {from_folder} -> {to_folder}")
        return True

    def move_emails_bulk(self, uids: list[str], from_folder: str, to_folder: str) -> dict:
        """Move multiple emails in one batch. Batch COPY by UID sets (50 per call), single EXPUNGE."""
        if not uids:
            return {"moved": 0, "failed": 0}
        uids = _check_uid_list(uids)
        _check_target(to_folder)
        if to_folder.strip() == (from_folder or "").strip():
            raise InvalidFolderName(f"source and target are the same folder ('{from_folder}')")
        self._select(from_folder)
        self._conn.create(_imap_quote(to_folder))
        moved = []
        failed = 0
        copy_responses = []
        # Batch COPY: send comma-separated UID sets (chunks of 50) instead of one-by-one
        chunk_size = 50
        for i in range(0, len(uids), chunk_size):
            chunk = uids[i:i + chunk_size]
            uid_set = ",".join(chunk)
            status, data = self._conn.uid("COPY", uid_set, _imap_quote(to_folder))
            if status == "OK":
                moved.extend(chunk)
                copy_responses.append(data)
            else:
                # Fallback: try individually for this chunk
                for uid in chunk:
                    status, data = self._conn.uid("COPY", uid, _imap_quote(to_folder))
                    if status == "OK":
                        moved.append(uid)
                        copy_responses.append(data)
                    else:
                        failed += 1
        if moved:
            # Batch STORE+EXPUNGE in one go
            uid_set = ",".join(moved)
            try:
                status, _ = self._conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")
            except Exception as e:
                logger.error(f"Bulk STORE \\Deleted failed in {from_folder}: {e}")
                status = "NO"
            if status != "OK":
                for data in copy_responses:
                    self._rollback_copy(data, to_folder)
                return {"moved": 0, "failed": len(uids)}
            self._conn.expunge()
        logger.info(f"Bulk move: {len(moved)} moved to {to_folder}, {failed} failed")
        return {"moved": len(moved), "failed": failed}

    def flag_email(self, uid: str, folder: str, flag: str) -> bool:
        """Add a flag to an email."""
        _check_uid(uid)
        self._select(folder)
        imap_flag = _resolve_flag(flag)
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", f"({imap_flag})")
        if status == "OK":
            logger.info(f"Flagged UID {uid} with {imap_flag}")
            return True
        return False

    def unflag_email(self, uid: str, folder: str, flag: str) -> bool:
        """Remove a flag from an email."""
        _check_uid(uid)
        self._select(folder)
        imap_flag = _resolve_flag(flag)
        status, _ = self._conn.uid("STORE", uid, "-FLAGS", f"({imap_flag})")
        if status == "OK":
            logger.info(f"Unflagged UID {uid}: removed {imap_flag}")
            return True
        return False

    def mark_read(self, uid: str, folder: str) -> bool:
        """Mark an email as read."""
        _check_uid(uid)
        self._select(folder)
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", "(\\Seen)")
        return status == "OK"

    def mark_unread(self, uid: str, folder: str) -> bool:
        """Mark an email as unread."""
        _check_uid(uid)
        self._select(folder)
        status, _ = self._conn.uid("STORE", uid, "-FLAGS", "(\\Seen)")
        return status == "OK"

    def mark_processed(self, uid: str, folder: str) -> bool:
        """Add the MailIA processed flag."""
        _check_uid(uid)
        self._select(folder)
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", f"({PROCESSED_FLAG})")
        return status == "OK"

    def get_unprocessed_uids(self, folder: str = "INBOX") -> list[str]:
        """Get UIDs of emails not yet processed by MailIA."""
        self._select(folder, readonly=True)
        # Search for emails WITHOUT our custom flag
        status, data = self._conn.uid("SEARCH", None, f"UNKEYWORD {PROCESSED_FLAG}")
        if status != "OK":
            return []
        return data[0].decode().split() if data[0] else []

    def _find_trash_folder(self) -> str | None:
        """Find the Trash folder name, cached per connection.

        The \\Trash special-use flag comes first: it is the only reliable answer. The
        name list misses anything the list does not anticipate — the real account here
        uses "Elements supprimes", absent from it — and a miss used to mean permanent
        destruction rather than a move.
        """
        if hasattr(self, '_trash_cache'):
            return self._trash_cache
        trash_names = [
            "Trash", "INBOX.Trash", "Deleted", "INBOX.Deleted",
            "Deleted Items", "INBOX.Deleted Items",
            "Corbeille", "INBOX.Corbeille",
        ]
        self._trash_cache = None
        try:
            folders = self.list_folders()
            for f in folders:
                if "\\Trash" in f.get("flags", ""):
                    self._trash_cache = f["name"]
                    return self._trash_cache
            folder_names = [f["name"] for f in folders]
            for t in trash_names:
                if t in folder_names:
                    self._trash_cache = t
                    return t
        except Exception:
            pass
        return None

    def delete_email(self, uid: str, folder: str, permanent: bool = False) -> bool:
        """Delete an email: move it to Trash, or purge it if that is what was asked.

        Sets `last_delete_recoverable` so the caller can tell the user which happened.
        Without a Trash folder this used to purge silently while answering "deleted":
        a destruction the caller never asked for and could not detect.
        """
        _check_uid(uid)
        trash_folder = self._find_trash_folder()

        if trash_folder and folder != trash_folder and not permanent:
            self.last_delete_recoverable = True
            return self.move_email(uid, folder, trash_folder)

        # Purge. Legitimate when emptying the Trash itself, or when explicitly asked.
        if not permanent and folder != trash_folder:
            raise NoTrashFolder(
                "No Trash folder on this server: deleting would destroy the message "
                "permanently. Pass permanent=true to confirm, or create a Trash folder."
            )
        self._select(folder)
        status, _ = self._conn.uid("STORE", uid, "+FLAGS", "(\\Deleted)")
        if status == "OK":
            self._conn.expunge()
            self.last_delete_recoverable = False
            return True
        return False

    def delete_emails_bulk(self, uids: list[str], folder: str, permanent: bool = False) -> dict:
        """Delete multiple emails in one batch. Uses UID sets for efficiency.

        The returned dict carries `recoverable` so the caller can say whether the mail
        went to the Trash or was purged."""
        if not uids:
            return {"deleted": 0, "failed": 0, "recoverable": True}

        uids = _check_uid_list(uids)

        trash_folder = self._find_trash_folder()

        if not permanent and not trash_folder and folder != trash_folder:
            raise NoTrashFolder(
                f"No Trash folder on this server: deleting these {len(uids)} messages "
                "would destroy them permanently. Pass permanent=true to confirm, or "
                "create a Trash folder."
            )

        if trash_folder and folder != trash_folder and not permanent:
            # Batch move to trash by UID sets, like move_emails_bulk : supprimer EST un
            # deplacement vers la corbeille, et un COPY par message coutait 1,02 aller-retour
            # par email la ou le regroupement en coute 0,04 — 25 fois plus, pour le meme travail.
            self._select(folder)
            self._conn.create(_imap_quote(trash_folder))
            moved = []
            failed = 0
            copy_responses = []
            chunk_size = 50
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i + chunk_size]
                status, data = self._conn.uid("COPY", ",".join(chunk), _imap_quote(trash_folder))
                if status == "OK":
                    moved.extend(chunk)
                    copy_responses.append(data)
                else:
                    # Repli message par message : un seul UID fautif ne doit pas faire
                    # echouer les 49 autres du lot.
                    for uid in chunk:
                        status, data = self._conn.uid("COPY", uid, _imap_quote(trash_folder))
                        if status == "OK":
                            moved.append(uid)
                            copy_responses.append(data)
                        else:
                            failed += 1
            if moved:
                uid_set = ",".join(moved)
                try:
                    status, _ = self._conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")
                except Exception as e:
                    logger.error(f"Bulk delete STORE \\Deleted failed in {folder}: {e}")
                    status = "NO"
                if status != "OK":
                    for data in copy_responses:
                        self._rollback_copy(data, trash_folder)
                    return {"deleted": 0, "failed": len(uids), "recoverable": True}
                self._conn.expunge()
            logger.info(f"Bulk delete: {len(moved)} moved to trash, {failed} failed")
            return {"deleted": len(moved), "failed": failed, "recoverable": True}
        else:
            # Already in trash or no trash: batch flag + single EXPUNGE
            self._select(folder)
            uid_set = ",".join(uids)
            status, _ = self._conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")
            if status == "OK":
                self._conn.expunge()
                logger.info(f"Bulk delete: {len(uids)} permanently deleted from {folder}")
                return {"deleted": len(uids), "failed": 0, "recoverable": False}
            logger.error(f"Bulk delete STORE failed for {folder}")
            return {"deleted": 0, "failed": len(uids), "recoverable": False}

    def save_draft(self, raw_message: bytes) -> bool:
        """Save a message to the Drafts folder."""
        draft_names = [
            "Drafts", "INBOX.Drafts", "Draft", "INBOX.Draft",
            "Brouillons", "INBOX.Brouillons",
        ]
        try:
            folders = self.list_folders()
            # First: find folder with \Drafts flag (most reliable)
            draft_folder = None
            for f in folders:
                if "\\Drafts" in f.get("flags", ""):
                    draft_folder = f["name"]
                    break
            # Fallback: try common names
            if not draft_folder:
                folder_names = [f["name"] for f in folders]
                for d in draft_names:
                    if d in folder_names:
                        draft_folder = d
                        break
            if not draft_folder:
                draft_folder = "Drafts"
                self._conn.create(_imap_quote(draft_folder))
        except Exception:
            draft_folder = "Drafts"

        import time
        status, _ = self._conn.append(
            _imap_quote(draft_folder),
            "(\\Draft)",
            imaplib.Time2Internaldate(time.time()),
            raw_message,
        )
        return status == "OK"

    def save_to_sent(self, raw_message: bytes) -> bool:
        """Save a sent message to the Sent folder."""
        try:
            folders = self.list_folders()
            # First: find folder with \Sent flag (most reliable)
            sent_folder = None
            for f in folders:
                if "\\Sent" in f.get("flags", ""):
                    sent_folder = f["name"]
                    break
            # Fallback: try common names
            if not sent_folder:
                sent_names = [
                    "Sent", "INBOX.Sent", "Sent Items", "INBOX.Sent Items",
                    "Sent Messages", "INBOX.Sent Messages",
                    "&AMk-l&AOk-ments envoy&AOk-s",
                ]
                folder_names = [f["name"] for f in folders]
                for s in sent_names:
                    if s in folder_names:
                        sent_folder = s
                        break
            if not sent_folder:
                sent_folder = "Sent"
                self._conn.create(_imap_quote(sent_folder))
        except Exception:
            sent_folder = "Sent"

        import time
        status, _ = self._conn.append(
            _imap_quote(sent_folder),
            "(\\Seen)",
            imaplib.Time2Internaldate(time.time()),
            raw_message,
        )
        return status == "OK"

    def get_attachment_data(self, uid: str, folder: str, attachment_index: int) -> dict | None:
        """Get attachment data by index from an email."""
        _check_uid(uid)
        raw = self.fetch_raw(uid, folder)
        if not raw:
            return None
        msg = email.message_from_bytes(raw)
        idx = 0
        for part in msg.walk():
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                if idx == attachment_index:
                    try:
                        payload = part.get_payload(decode=True)
                    except Exception:
                        payload = part.get_payload(decode=False)
                        if isinstance(payload, str):
                            payload = payload.encode("utf-8", errors="replace")
                    filename = part.get_filename() or "unnamed"
                    content_type = part.get_content_type()
                    return {
                        "filename": filename,
                        "content_type": content_type,
                        "data": payload,
                    }
                idx += 1
        return None


def _uid_search(conn, criteria: str):
    """UID SEARCH tolerant du non-ASCII. Deux defauts empiles corriges ici.

    1. imaplib encode les arguments `str` en ASCII : un critere contenant « Ete »
       levait UnicodeEncodeError avant meme d'atteindre le serveur.
    2. `uid("SEARCH", "UTF-8", criteria)` n'emet PAS le mot-cle CHARSET, contrairement
       a `imaplib.search()` qui l'insere. Le charset partait donc comme s'il etait une
       cle de recherche. Il faut le passer explicitement.
    """
    try:
        criteria.encode("ascii")
    except UnicodeEncodeError:
        return conn.uid("SEARCH", "CHARSET", "UTF-8", criteria.encode("utf-8"))
    return conn.uid("SEARCH", None, criteria)


def _decode_header(raw) -> str:
    """Return a header as text, whatever the sender put in it.

    `msg.get("Subject")` returns an `email.header.Header` — not a str — when the header
    carries raw 8-bit bytes instead of an RFC 2047 encoded-word. That object then travels
    all the way to Elasticsearch, whose serializer rejects it with a message about
    `np.float_`, and the folder's sync cursor stops there for good.
    """
    import email.header
    if raw is None:
        return ""
    parts = email.header.decode_header(raw)
    morceaux = []
    for part, charset in parts:
        if isinstance(part, bytes):
            try:
                morceaux.append(part.decode(charset or "utf-8", errors="replace"))
            except (UnicodeDecodeError, LookupError):
                morceaux.append(part.decode("utf-8", errors="replace"))
        else:
            morceaux.append(part)
    return " ".join(morceaux)


def _resolve_flag(flag: str) -> str:
    """Convert human-readable flag names to IMAP flags.

    Whitelist only: the set of IMAP system flags is finite, and the value ends up
    inside STORE ... (<flag>) without escaping. Letting an unknown name through
    verbatim lets it close the parenthesis and start a new command.
    """
    mapping = {
        "important": "\\Flagged",
        "flagged": "\\Flagged",
        "read": "\\Seen",
        "seen": "\\Seen",
        "answered": "\\Answered",
        "draft": "\\Draft",
        "deleted": "\\Deleted",
    }
    resolved = mapping.get(str(flag).lower().strip())
    if resolved is None:
        raise InvalidFlag(
            f"Unknown flag {flag!r}. Allowed: {', '.join(sorted(mapping))}"
        )
    return resolved


def _encode_imap_utf7(s: str) -> str:
    """Encode a Unicode string to IMAP modified UTF-7 (RFC 3501 section 5.1.3).

    ASCII printable chars (0x20-0x7e) pass through, except '&' becomes '&-'.
    Non-ASCII chars are encoded as modified base64 between '&' and '-'.
    """
    import base64
    result = []
    non_ascii = []

    def _flush_non_ascii():
        if non_ascii:
            utf16 = ''.join(non_ascii).encode('utf-16-be')
            b64 = base64.b64encode(utf16).decode('ascii').rstrip('=')
            b64 = b64.replace('/', ',')
            result.append('&' + b64 + '-')
            non_ascii.clear()

    for ch in s:
        if ch == '&':
            _flush_non_ascii()
            result.append('&-')
        elif 0x20 <= ord(ch) <= 0x7e:
            _flush_non_ascii()
            result.append(ch)
        else:
            non_ascii.append(ch)
    _flush_non_ascii()
    return ''.join(result)


def _decode_imap_utf7(s: str) -> str:
    """Decode IMAP modified UTF-7 folder names (RFC 3501 section 5.1.3).

    IMAP uses '&' instead of '+' as shift character, and ',' instead of '/' in base64.
    '&-' encodes a literal '&'.
    """
    result = []
    i = 0
    while i < len(s):
        if s[i] == '&':
            j = s.index('-', i + 1)
            if j == i + 1:
                # &- is a literal &
                result.append('&')
            else:
                # Decode modified base64 section
                import base64
                encoded = s[i + 1:j]
                # Replace , with / for standard base64
                encoded = encoded.replace(',', '/')
                # Pad to multiple of 4
                encoded += '=' * (4 - len(encoded) % 4) if len(encoded) % 4 else ''
                try:
                    decoded_bytes = base64.b64decode(encoded)
                    result.append(decoded_bytes.decode('utf-16-be'))
                except Exception:
                    result.append(s[i:j + 1])
            i = j + 1
        else:
            result.append(s[i])
            i += 1
    return ''.join(result)

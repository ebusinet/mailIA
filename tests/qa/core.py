"""Noyau de la suite de non-regression : configuration, client HTTP, acces conteneur, registre.

Volontairement limite a la bibliotheque standard. httpx figure dans requirements.txt mais
n'est installe que dans l'image Docker, pas sur la machine de developpement : en dependre
imposerait une installation avant la premiere execution, et une suite qu'il faut installer
est une suite qu'on ne relance pas.
"""
from __future__ import annotations

import json
import mimetypes
import os
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _read_token() -> str:
    tok = os.environ.get("MAILIA_QA_TOKEN", "").strip()
    if tok:
        return tok
    f = TESTS_DIR / ".qa_token"
    if f.exists():
        return f.read_text().strip()
    return ""


def _env(nom: str, defaut: str) -> str:
    """Variable d'environnement, en traitant « definie mais vide » comme « absente ».

    `os.environ.get(nom, defaut)` renvoie la chaine vide si la variable est exportee sans
    valeur : le defaut est court-circuite et l'erreur ne surgit que bien plus tard, sous
    forme de trace incomprehensible.
    """
    v = os.environ.get(nom, "").strip()
    return v or defaut


@dataclass
class Config:
    api_url: str = field(default_factory=lambda: _env(
        "MAILIA_API_URL", "https://mailia.expert-presta.com/api").rstrip("/"))
    token: str = field(default_factory=_read_token)
    account_id_brut: str = field(default_factory=lambda: _env("MAILIA_QA_ACCOUNT_ID", "3"))
    ssh_host: str = field(default_factory=lambda: _env("MAILIA_SSH_HOST", "expert-presta"))
    api_container: str = field(default_factory=lambda: _env("MAILIA_API_CONTAINER", "mailia-api"))
    timeout_brut: str = field(default_factory=lambda: _env("MAILIA_TIMEOUT", "60"))
    # Dossiers de travail fixes : les recreer a chaque execution ferait deriver
    # l'arborescence, d'autant que certains serveurs IMAP refusent de supprimer un dossier.
    folder_src: str = "QA_AUTOTEST_SRC"
    folder_dst: str = "QA_AUTOTEST_DST"

    @property
    def account_id(self) -> int:
        return int(self.account_id_brut)

    @property
    def timeout(self) -> int:
        return int(self.timeout_brut)

    def problemes(self) -> list[str]:
        """Anomalies de configuration, formulees pour etre lisibles sans lire le code.

        Renvoyees au garde-fou plutot que levees a l'import : une configuration invalide
        doit produire un REFUS explicite (code 2), pas une trace brute confondue avec un
        echec de test (code 1).
        """
        p = []
        if not self.api_url.startswith(("http://", "https://")):
            p.append(f"MAILIA_API_URL doit commencer par http:// ou https:// "
                     f"(valeur : « {self.api_url} »)")
        for nom, brut in (("MAILIA_QA_ACCOUNT_ID", self.account_id_brut),
                          ("MAILIA_TIMEOUT", self.timeout_brut)):
            try:
                if int(brut) <= 0:
                    p.append(f"{nom} doit etre un entier positif (valeur : « {brut} »)")
            except ValueError:
                p.append(f"{nom} doit etre un entier (valeur : « {brut} »)")
        return p


CFG = Config()


# ---------------------------------------------------------------------------
# Exceptions de controle
# ---------------------------------------------------------------------------

class Skip(Exception):
    """Un test qui ne peut pas s'executer le dit, il ne passe jamais silencieusement."""


class Failure(AssertionError):
    pass


class GuardError(Exception):
    """Le garde-fou de securite a refuse de laisser demarrer la suite."""


# ---------------------------------------------------------------------------
# Client HTTP (stdlib)
# ---------------------------------------------------------------------------

class Headers(dict):
    """En-tetes insensibles a la casse : HTTP/2 les transmet en minuscules, HTTP/1.1 non."""

    def get(self, cle, defaut=None):
        bas = cle.lower()
        for k, v in self.items():
            if k.lower() == bas:
                return v
        return defaut

    def __contains__(self, cle):
        return self.get(cle) is not None


@dataclass
class Response:
    status: int
    body: bytes
    headers: Headers

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self):
        try:
            return json.loads(self.body.decode("utf-8"))
        except Exception:
            return None

    @property
    def detail(self) -> str:
        d = self.json()
        if isinstance(d, dict) and "detail" in d:
            return d["detail"] if isinstance(d["detail"], str) else json.dumps(d["detail"])
        return self.text[:200]


def _pas_une_reponse_de_l_application(r: Response, method: str, chemin: str) -> Response:
    """Distingue une panne de passerelle d'une reponse du produit.

    Pendant un redeploiement, nginx renvoie `502 Bad Gateway` avec une page HTML. Les tests
    le lisaient comme un refus manquant ou un endpoint casse : une execution a ainsi rapporte
    six echecs produit qui n'etaient que le conteneur en cours de redemarrage.

    L'application, elle, emet aussi des 502 — mais toujours en JSON
    (`{"detail": "IMAP error: ..."}`). Le corps est donc le discriminant fiable, pas le code.
    Un test qui n'a pas pu joindre l'application doit le dire, pas accuser le produit.
    """
    if r.status in (502, 503, 504) and r.json() is None:
        raise Skip(f"passerelle en erreur sur {method} {chemin} : HTTP {r.status} sans corps "
                   "JSON — l'application n'a pas repondu (redeploiement en cours ?). "
                   "Ce n'est pas un resultat de test.")
    return r


class Api:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._ctx = ssl.create_default_context()

    def request(self, method: str, path: str, *, params=None, json_body=None,
                body: bytes = None, content_type: str = None, token: str = "__default__") -> Response:
        url = self.cfg.api_url + path
        if params:
            url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
        data = body
        headers = {}
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if content_type:
            headers["Content-Type"] = content_type
        tok = self.cfg.token if token == "__default__" else token
        if tok:
            headers["Authorization"] = f"Bearer {tok}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.cfg.timeout, context=self._ctx) as r:
                return Response(r.status, r.read(), Headers(r.headers))
        except urllib.error.HTTPError as e:
            return _pas_une_reponse_de_l_application(
                Response(e.code, e.read(), Headers(e.headers or {})), method, path)
        except urllib.error.URLError as e:
            raise Skip(f"API injoignable ({self.cfg.api_url}) : {e.reason}")

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, **kw):
        return self.request("POST", path, **kw)

    def put(self, path, **kw):
        return self.request("PUT", path, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)

    def upload(self, path: str, filename: str, content: bytes, *, params=None) -> Response:
        """Envoi multipart/form-data (utilise par l'import mbox)."""
        boundary = "----qa" + uuid.uuid4().hex
        ctype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        parts = [
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode(),
            f"Content-Type: {ctype}\r\n\r\n".encode(),
            content,
            f"\r\n--{boundary}--\r\n".encode(),
        ]
        return self.request("POST", path, params=params, body=b"".join(parts),
                            content_type=f"multipart/form-data; boundary={boundary}")


API = Api(CFG)


# ---------------------------------------------------------------------------
# Acces au conteneur (tests de rang 2)
# ---------------------------------------------------------------------------

class Container:
    """Execution de code dans le conteneur API via ssh + docker exec.

    Indisponible => les tests concernes rendent SKIP avec la raison, jamais PASS.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._available = None
        self._reason = ""

    @property
    def available(self) -> bool:
        if self._available is None:
            try:
                p = subprocess.run(
                    ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", self.cfg.ssh_host,
                     f"docker exec {self.cfg.api_container} python3 -c 'print(1)'"],
                    capture_output=True, timeout=40, text=True)
                self._available = p.returncode == 0 and "1" in p.stdout
                if not self._available:
                    self._reason = (p.stderr or p.stdout or "cause inconnue").strip()[:160]
            except Exception as e:
                self._available = False
                self._reason = f"{type(e).__name__}: {e}"
        return self._available

    def require(self):
        if not self.available:
            raise Skip(f"acces conteneur indisponible (ssh {self.cfg.ssh_host}) : {self._reason}")

    def python(self, code: str, workdir: str = "/app") -> str:
        """Execute du Python dans le conteneur, renvoie stdout."""
        self.require()
        payload = code.encode("utf-8").hex()
        wrapper = (
            f"docker exec -i -w {workdir} {self.cfg.api_container} python3 -c "
            f"\"import binascii,sys;exec(binascii.unhexlify('{payload}').decode())\""
        )
        p = subprocess.run(["ssh", "-o", "BatchMode=yes", self.cfg.ssh_host, wrapper],
                           capture_output=True, timeout=self.cfg.timeout + 60, text=True)
        if p.returncode != 0:
            sortie = (p.stderr or p.stdout)[-300:]
            # Le conteneur qui redemarre n'est pas un defaut du produit. Meme raisonnement
            # que pour le 502 de nginx : c'est le message de docker qui distingue, et un
            # test qui n'a pas pu s'executer doit le dire plutot qu'accuser.
            if "is not running" in sortie or "No such container" in sortie:
                raise Skip(f"le conteneur {self.cfg.api_container} ne tourne pas "
                           "(redeploiement en cours ?) — ce test n'a pas pu s'executer, "
                           "il n'a rien constate sur le produit.")
            raise Failure(f"execution conteneur en echec : {sortie}")
        return p.stdout

    def imap(self, code: str) -> str:
        """Execute du code disposant d'une connexion IMAP ouverte sous le nom M.

        La connexion vise le serveur du compte reellement teste, lu dans la base et non
        code en dur. Avec un hote fixe, lancer la suite sur un second compte ferait
        recenser le mauvais serveur **en silence** : les tests passeraient ou echoueraient
        pour des raisons etrangeres au compte sous test.

        Le garde-fou a deja verifie que cet hote est jetable ; on ne se connecte donc
        jamais ailleurs que sur un serveur de test.
        """
        prelude = (
            "import imaplib, email.utils, time\n"
            "from email.message import EmailMessage\n"
            "from sqlalchemy import create_engine, text\n"
            "from src.config import get_settings\n"
            "from src.security import decrypt_value as _dec\n"
            "_e = create_engine(get_settings().database_url.replace('+asyncpg',''))\n"
            "with _e.connect() as _c:\n"
            "    _a = _c.execute(text('SELECT imap_host, imap_port, imap_ssl, imap_user, "
            f"imap_password_encrypted FROM mail_accounts WHERE id = {CFG.account_id}')"
            ").first()\n"
            "M = (imaplib.IMAP4_SSL if _a[2] else imaplib.IMAP4)(_a[0], _a[1])\n"
            "M.login(_a[3], _dec(_a[4]))\n"
        )
        return self.python(prelude + code + "\ntry:\n    M.logout()\nexcept Exception:\n    pass\n")

    def mcp(self, tool: str, args: dict) -> dict:
        """Invoque un outil MCP en forcant USER_ID sur le compte QA."""
        from .guard import resolved_user_id
        uid = resolved_user_id()
        code = (
            "import asyncio, json, warnings\n"
            "warnings.filterwarnings('ignore')\n"
            "import src.mcp.server as S\n"
            "from fastmcp.tools import FunctionTool\n"
            f"S.USER_ID = {uid}\n"
            f"t = getattr(S, {tool!r}, None)\n"
            "if not isinstance(t, FunctionTool):\n"
            f"    print(json.dumps({{'__erreur__': 'outil inconnu: {tool}'}}))\n"
            "else:\n"
            "    fn = t.fn\n"
            f"    a = json.loads({json.dumps(json.dumps(args))})\n"
            "    try:\n"
            "        r = asyncio.run(fn(**a)) if asyncio.iscoroutinefunction(fn) else fn(**a)\n"
            "        print('__QA__' + json.dumps(r, default=str, ensure_ascii=False))\n"
            "    except Exception as e:\n"
            "        print('__QA__' + json.dumps({'__erreur__': type(e).__name__, "
            "'message': str(e)}, ensure_ascii=False))\n"
        )
        out = self.python(code)
        for line in out.splitlines():
            if line.startswith("__QA__"):
                return json.loads(line[6:])
        raise Failure(f"reponse MCP illisible : {out[-300:]}")


BOX = Container(CFG)


# ---------------------------------------------------------------------------
# Registre de tests
# ---------------------------------------------------------------------------

@dataclass
class TestCase:
    ident: str
    title: str
    ref: str
    fn: callable
    group: str
    serveur: str = "indifferent"


REGISTRY: list[TestCase] = []


# Profil de chaque serveur de test. Ce n'est pas une preference : c'est ce qui decide si un
# test prouve quelque chose.
#
#   - Un test de securite qui conclut « aucun temoin cree » doit tourner sur le serveur le
#     plus PERMISSIF. Sur un serveur strict, la commande injectee est refusee par le serveur
#     lui-meme : le test passe au vert sans que l'application y soit pour rien.
#     Mesure a l'appui : ` a2 CREATE X` (ligne precedee d'une espace) donne
#     `OK CREATE completed` sur GreenMail et `BAD Invalid tag` sur Dovecot.
#
#   - Un test fonctionnel qui conclut « la requete marche » doit tourner sur le plus STRICT.
#     Sur un serveur limite, une requete correcte echoue et dement a tort le correctif.
#     Mesure a l'appui : la recherche d'un objet accentue aboutit sur Dovecot, pas sur
#     GreenMail.
#
# Les deux faux negatifs vont donc dans des sens opposes, et aucun serveur ne protege des
# deux. Un test declare ce dont il a besoin ; s'il tourne ailleurs, il le DIT.
PROFILS_SERVEUR = {
    "greenmail": "permissif",
    "mailia-greenmail": "permissif",
    "dovecot": "strict",
    "mailia-dovecot": "strict",
}


def profil_serveur() -> str:
    """Profil du serveur du compte teste, ou 'inconnu' si l'hote n'est pas repertorie."""
    from .guard import hote_cible
    return PROFILS_SERVEUR.get((hote_cible() or "").lower(), "inconnu")


def test(ident: str, title: str, ref: str = "", group: str = "", serveur: str = "indifferent"):
    """`serveur` : 'permissif', 'strict' ou 'indifferent' (defaut).

    Un test exigeant un profil precis rend SKIP ailleurs, avec la raison. Il ne passe jamais
    au vert sur un serveur ou il ne peut rien demontrer.
    """
    def deco(fn):
        REGISTRY.append(TestCase(ident, title, ref, fn,
                                 group or fn.__module__.rsplit(".", 1)[-1], serveur))
        return fn
    return deco


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def _cause_environnement(texte: str) -> None:
    """Transforme en Skip un echec du au serveur de test plutot qu'au produit.

    Un serveur jetable presente souvent un certificat auto-signe. L'application le refuse,
    et c'est le bon comportement — mais plus aucun envoi n'aboutit. Ce n'est la propriete
    d'aucun test : le compter comme un echec rendrait rouges cinq tests d'envoi sur un
    serveur ou ils ne peuvent simplement pas s'executer.

    Applique a `expect` **et** a `expect_status`, sans quoi la meme cause produirait un SKIP
    pour les tests qui verifient un code HTTP et un FAIL pour ceux qui verifient un effet —
    incoherence constatee en conditions reelles.
    """
    if "CERTIFICATE_VERIFY_FAILED" in (texte or ""):
        raise Skip(
            "le serveur de messagerie de test presente un certificat auto-signe, que "
            "l'application refuse — a juste titre. L'envoi ne peut pas aboutir ici : ce "
            "test n'a pas pu s'executer, il n'a rien constate sur le produit. Ce SKIP est "
            "PERMANENT sur cette instance : ajouter le certificat de test au magasin de "
            "confiance de `mailia-api` est refuse deliberement, ce conteneur detenant les "
            "identifiants IMAP du compte professionnel reel alors que la cle privee de "
            "l'autorite de test est versionnee dans le depot. Ces chemins d'envoi se "
            "verifient sur le serveur permissif.")


def expect(condition, message: str):
    if condition:
        return
    _cause_environnement(message)
    raise Failure(message)


def expect_status(resp: Response, expected, message: str = ""):
    exp = expected if isinstance(expected, (list, tuple, set)) else [expected]
    if resp.status in exp:
        return
    _cause_environnement(resp.detail)
    raise Failure(f"{message or 'code HTTP inattendu'} : attendu {exp}, obtenu "
                  f"{resp.status} — {resp.detail[:160]}")


# ---------------------------------------------------------------------------
# Utilitaires de donnees de test
# ---------------------------------------------------------------------------

def premier(msgs: list, contexte: str) -> dict:
    """Premier message d'une liste, avec un message explicite si elle est vide.

    Sans cette garde, une preparation incomplete produit un `IndexError` opaque que le
    rapport affiche sous la reference d'un bug produit — le pire signal possible.
    """
    if not msgs:
        raise Failure(f"aucun message disponible pour {contexte} : la preparation du test "
                      "n'a pas livre ce qui etait attendu")
    return msgs[0]


def unique(prefix: str = "QA") -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def build_mbox(messages: list[dict]) -> bytes:
    """Construit un mbox en memoire.

    Chaque message : {from, subject, body, date (YYYY-MM-DD HH:MM:SS), to?, cc?, headers?,
    message_id?}
    L'import mbox est le seul moyen purement HTTP de deposer un message dans un dossier
    IMAP precis avec des en-tetes maitrises.
    """
    import email.utils
    lines = []
    for m in messages:
        ts = time.mktime(time.strptime(m.get("date", "2024-01-01 12:00:00"), "%Y-%m-%d %H:%M:%S"))
        frm = m["from"]
        lines.append(f"From {frm} {time.asctime(time.localtime(ts))}")
        lines.append(f"From: {frm}")
        lines.append(f"To: {m.get('to', 'test@mailia.local')}")
        if m.get("cc"):
            lines.append(f"Cc: {m['cc']}")
        lines.append(f"Subject: {m['subject']}")
        lines.append(f"Date: {email.utils.formatdate(ts, localtime=True)}")
        # `message_id` permet de maitriser l'en-tete au lieu de le laisser genere : c'est
        # necessaire pour exercer un Message-ID replie (RFC 5322), vecteur d'injection reel
        # puisque cet en-tete est ecrit par l'expediteur du message.
        lines.append("Message-ID: " + m.get("message_id",
                                            f"<{uuid.uuid4().hex}@qa-autotest.local>"))
        for k, v in (m.get("headers") or {}).items():
            lines.append(f"{k}: {v}")
        lines.append("Content-Type: text/plain; charset=utf-8")
        lines.append("")
        lines.append(m.get("body", "corps de test"))
        lines.append("")
    return "\n".join(lines).encode("utf-8")


def messages_in(folder: str, storage: str = "imap", marker: str = "") -> list:
    """Liste les messages d'un dossier, restreinte au marqueur du test appelant.

    Chaque test travaille avec un marqueur unique injecte dans l'objet. Compter uniquement
    ses propres messages rend les tests reellement independants : ils ne dependent plus de
    l'etat laisse par les precedents, ce qui est indispensable car le serveur de test ne
    rend pas ses suppressions immediatement visibles d'une connexion IMAP a l'autre.
    """
    r = API.get(f"/accounts/{CFG.account_id}/messages",
                params={"folder": folder, "size": 300, "storage": storage})
    if r.status == 404:
        return []
    expect_status(r, 200, f"listage de « {folder} »")
    msgs = r.json()["messages"]
    if marker:
        msgs = [m for m in msgs if marker in (m.get("subject") or "")]
    return msgs


def count_in(folder: str, storage: str = "imap", marker: str = "") -> int:
    if marker:
        return len(messages_in(folder, storage, marker))
    r = API.get(f"/accounts/{CFG.account_id}/messages",
                params={"folder": folder, "size": 300, "storage": storage})
    if r.status == 404:
        return 0
    expect_status(r, 200, f"listage de « {folder} »")
    return r.json()["total"]


def work_folder(ident: str) -> str:
    """Dossier de travail dedie a un test.

    Les tests qui portent sur l'etat GLOBAL d'un dossier (filtres, regles, vidage) ne
    peuvent pas partager le meme espace : un residu laisse par un autre test fausse leurs
    comptages. Un nom fixe derive de l'identifiant du test leur donne un espace exclusif
    sans faire croitre l'arborescence d'une execution a l'autre.
    """
    return f"QA_AT_{ident.replace('-', '')}"


def ensure_folder(name: str):
    API.post(f"/accounts/{CFG.account_id}/create-folder", json_body={"folder_name": name})


def empty_folder(name: str):
    """Vide un dossier, au mieux.

    Volontairement sans assertion : le serveur de test ne rend pas toujours un EXPUNGE
    immediatement visible depuis une autre connexion IMAP. On l'appelle pour limiter la
    croissance des dossiers de travail, jamais pour garantir un etat de depart — cette
    garantie est apportee par le marqueur unique de `seed()`.
    """
    ensure_folder(name)
    API.post(f"/accounts/{CFG.account_id}/empty-folder", json_body={"folder_name": name})


def seed(folder: str, messages: list[dict], _essai: int = 0) -> str:
    """Depose des messages de test et renvoie leur marqueur unique.

    Chaque objet recoit un marqueur `[QA-xxxxxxxx]`. Les tests comptent et selectionnent
    exclusivement par ce marqueur : ils sont ainsi independants du contenu preexistant du
    dossier, donc rejouables et insensibles a l'ordre d'execution.

    L'import mbox est le seul moyen purement HTTP de deposer un message dans un dossier IMAP
    precis avec des en-tetes maitrises.
    """
    marker = f"[QA-{uuid.uuid4().hex[:8]}]"
    empty_folder(folder)
    marques = []
    for m in messages:
        c = dict(m)
        c["subject"] = f"{marker} {m['subject']}"
        marques.append(c)
    r = API.upload(f"/accounts/{CFG.account_id}/import-mbox", "qa_autotest.mbox",
                   build_mbox(marques), params={"storage": "imap", "folder": folder})
    expect_status(r, 200, "depot des messages de test (import mbox)")
    job = r.json()["job_id"]

    fin = time.time() + 30
    etat = None
    while time.time() < fin:
        time.sleep(0.5)
        j = API.get(f"/accounts/import-jobs/{job}")
        if j.status == 200:
            etat = j.json()
            if etat.get("status") in ("done", "error"):
                break
    expect(etat and etat.get("status") == "done",
           f"le job d'import ne s'est pas termine : "
           f"{etat.get('status') if etat else 'aucun statut'} — {(etat or {}).get('error')}")

    got = count_in(folder, marker=marker)
    while got != len(messages) and time.time() < fin:
        time.sleep(0.5)
        got = count_in(folder, marker=marker)
    if got != len(messages) and _essai == 0:
        # Une reprise unique : le serveur de test rend parfois un depot visible avec retard.
        # Mieux vaut une seconde tentative qu'un echec intermittent qu'on finirait par ignorer.
        return seed(folder, messages, _essai=1)
    expect(got == len(messages),
           f"depot incomplet dans « {folder} » : {got} message(s) marques {marker} sur "
           f"{len(messages)} attendus (job : {etat.get('progress')})")
    return marker


# ---------------------------------------------------------------------------
# Menage de fin d'execution
# ---------------------------------------------------------------------------

PREFIXE_QA = "qa autotest"


def _a_nous(nom: str) -> bool:
    return (nom or "").lower().startswith(PREFIXE_QA)


def nettoyage_final() -> dict:
    """Supprime les objets nommes laisses par la suite.

    Chaque test nettoie deja dans un `finally`, mais un `finally` peut lui-meme echouer
    (appel reseau perdu, processus interrompu). Ce balayage garantit qu'une execution ne
    laisse rien derriere elle, condition pour que la suivante parte du meme etat.

    Ne touche qu'aux objets dont le nom porte le prefixe de la suite : rien de ce que
    l'utilisateur aurait cree a la main n'est concerne.
    """
    bilan = {"regles_classiques": 0, "regles_ia": 0, "providers": 0,
             "contacts": 0, "groupes": 0, "signatures": 0, "comptes": 0}
    try:
        for r in API.get("/rules/classic/").json() or []:
            if _a_nous(r.get("name")):
                API.delete(f"/rules/classic/{r['id']}")
                bilan["regles_classiques"] += 1
        for r in API.get("/rules/").json() or []:
            if _a_nous(r.get("name")):
                API.delete(f"/rules/{r['id']}")
                bilan["regles_ia"] += 1
        for p in API.get("/ai/providers").json() or []:
            if _a_nous(p.get("name")):
                API.delete(f"/ai/providers/{p['id']}")
                bilan["providers"] += 1
        for c in API.get("/contacts/").json() or []:
            if _a_nous(c.get("name")):
                API.delete(f"/contacts/{c['id']}")
                bilan["contacts"] += 1
        for g in API.get("/contacts/groups").json() or []:
            if _a_nous(g.get("name")):
                API.delete(f"/contacts/groups/{g['id']}")
                bilan["groupes"] += 1
        for s in API.get("/signatures/").json() or []:
            if _a_nous(s.get("name")):
                API.delete(f"/signatures/{s['id']}")
                bilan["signatures"] += 1
        for a in API.get("/accounts/").json() or []:
            if _a_nous(a.get("name")) and a["id"] != CFG.account_id:
                API.delete(f"/accounts/{a['id']}")
                bilan["comptes"] += 1
    except Exception:
        pass
    return {k: v for k, v in bilan.items() if v}

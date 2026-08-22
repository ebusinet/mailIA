"""GARDE-FOU DE SECURITE — a lire avant toute modification de ce fichier.

POURQUOI IL EXISTE
------------------
MailIA est utilise avec un compte de messagerie professionnel reel contenant des annees
de courrier. La suite de tests ecrit, deplace et supprime des emails. Si elle etait lancee
un jour avec un jeton pointant sur ce compte — jeton copie-colle par erreur, variable
d'environnement heritee d'un autre terminal, fichier .qa_token perime remplace par celui
d'un administrateur — elle detruirait des donnees irremplacables en quelques secondes.

Aucune convention de nommage, aucun commentaire et aucune consigne dans un README ne
protege contre cela. Seul un controle execute par le programme lui-meme le fait.

CE QU'IL VERIFIE, AVANT LE PREMIER TEST
---------------------------------------
1. La configuration est exploitable (URL d'API, identifiants numeriques).
2. L'identite authentifiee (`GET /auth/me`) appartient a la liste blanche QA.
3. Elle n'appartient a aucun domaine interdit.
4. TOUS les comptes de messagerie visibles par ce jeton pointent sur un hote de test,
   en IMAP **et** en SMTP. Un seul compte hors liste blanche suffit a tout arreter : un
   jeton donnant acces a un compte de production n'a rien a faire ici, meme si les tests
   ne le visent pas.
5. Le compte cible designe existe bien et est un compte de test.

En cas d'echec, la suite REFUSE DE DEMARRER. Elle ne saute pas les tests, elle ne previent
pas : elle s'arrete avec un code de sortie non nul.

L'IDENTITE NUMERIQUE EST DERIVEE, JAMAIS CODEE EN DUR
-----------------------------------------------------
Les appels aux outils MCP s'executent sous un identifiant numerique d'utilisateur. Cet
identifiant n'est PAS une constante de ce fichier : il est lu depuis `/auth/me`, une fois
l'identite validee, et publie par `resolved_user_id()`.

Une constante en dur qu'on doit ensuite penser a comparer est plus fragile qu'une valeur
obtenue de la source de verite : elle se desynchronise en silence. Le scenario concret est
« le compte QA a ete supprime et son identifiant reattribue », ou « la suite est pointee sur
une autre instance ou cet identifiant appartient a quelqu'un de reel ». Ici, les deux
chemins — API et MCP — utilisent par construction la meme identite, celle qui vient d'etre
verifiee.

`resolved_user_id()` leve tant que le garde-fou n'a pas tourne : aucun appel MCP n'est
possible avant validation.

REGLE DE MODIFICATION
---------------------
Elargir une liste blanche est une decision de securite, pas un ajustement de confort.
Si un test echoue a cause du garde-fou, la reponse par defaut est de corriger le jeton ou
la cible, jamais d'ajouter une entree ici.
"""
from __future__ import annotations

from .core import API, CFG, GuardError

# Seules ces identites peuvent executer la suite.
ALLOWED_IDENTITIES = {"qa@mailia.local"}

# Hotes de messagerie consideres comme jetables. Tout le reste est refuse, en IMAP
# comme en SMTP : la suite envoie reellement des emails, un relais tiers ferait sortir
# du courrier depuis une adresse reelle a chaque execution.
ALLOWED_MAIL_HOSTS = {"greenmail", "localhost", "127.0.0.1", "mailia-greenmail",
                      "dovecot", "mailia-dovecot"}

# Motifs qui font echouer immediatement, quelle que soit la liste blanche.
# Defense redondante et volontaire : si quelqu'un elargit ALLOWED_* par megarde,
# ce filtre tient encore.
FORBIDDEN_SUBSTRINGS = ("ebusinet", "ovh.net", "pimienta")

# Identite validee par le dernier appel a run_guard(). None tant qu'il n'a pas tourne.
_IDENTITE_VALIDEE: dict | None = None
_HOTE_CIBLE: str | None = None


def hote_cible() -> str:
    """Hote IMAP du compte teste, tel que le garde-fou l'a valide.

    Sert a determiner le profil du serveur (permissif ou strict), qui decide si un test de
    securite peut demontrer quoi que ce soit. Lu ici plutot que redemande a l'API : c'est la
    valeur que le garde a effectivement autorisee.
    """
    return _HOTE_CIBLE or ""


def resolved_user_id() -> int:
    """Identifiant numerique de l'utilisateur QA, tel que l'API l'a renvoye.

    Leve si le garde-fou n'a pas encore valide l'identite : les outils MCP s'executent
    sous cet identifiant, ils ne doivent jamais pouvoir tourner sans validation prealable.
    """
    if _IDENTITE_VALIDEE is None:
        raise GuardError(
            "appel MCP tente avant validation du garde-fou : aucun identifiant "
            "d'utilisateur verifie n'est disponible."
        )
    return _IDENTITE_VALIDEE["id"]


def resolved_identity() -> dict | None:
    return dict(_IDENTITE_VALIDEE) if _IDENTITE_VALIDEE else None


def _forbidden(value: str) -> str | None:
    low = (value or "").lower()
    for bad in FORBIDDEN_SUBSTRINGS:
        if bad in low:
            return bad
    return None


def check_identity(me: dict) -> None:
    """Verifie l'identite authentifiee. Leve GuardError si elle n'est pas le compte QA."""
    email = (me.get("email") or "").lower()
    if not email:
        raise GuardError("l'API n'a renvoye aucune identite : jeton invalide ou expire ?")
    bad = _forbidden(email)
    if bad:
        raise GuardError(
            f"REFUS : le jeton authentifie « {email} », qui contient le motif interdit "
            f"« {bad} ». Ce jeton donne acces a un compte reel."
        )
    if email not in ALLOWED_IDENTITIES:
        raise GuardError(
            f"REFUS : le jeton authentifie « {email} », qui ne figure pas dans la liste "
            f"blanche QA ({', '.join(sorted(ALLOWED_IDENTITIES))}). "
            "Verifiez MAILIA_QA_TOKEN ou tests/.qa_token."
        )
    uid = me.get("id")
    if not isinstance(uid, int) or uid <= 0:
        raise GuardError(
            f"REFUS : l'API n'a pas renvoye d'identifiant utilisateur exploitable "
            f"({uid!r}). Les outils MCP s'executent sous cet identifiant, il ne peut pas "
            "etre devine."
        )


def _host_ok(host: str) -> bool:
    return (host or "").lower() in ALLOWED_MAIL_HOSTS


def check_accounts(accounts: list[dict], target_id: int) -> dict:
    """Verifie que tous les comptes visibles sont jetables et renvoie le compte cible."""
    if not accounts:
        raise GuardError(
            "REFUS : ce jeton ne voit aucun compte de messagerie. La suite a besoin d'un "
            "compte de test pointant sur GreenMail."
        )
    for a in accounts:
        for champ in ("imap_host", "smtp_host", "imap_user", "smtp_user", "name"):
            bad = _forbidden(str(a.get(champ) or ""))
            if bad:
                raise GuardError(
                    f"REFUS : le compte {a.get('id')} « {a.get('name')} » a un champ "
                    f"{champ} contenant le motif interdit « {bad} ». "
                    "Ce jeton donne acces a un compte reel."
                )
        # IMAP : toujours renseigne, toujours verifie.
        if not _host_ok(a.get("imap_host")):
            raise GuardError(
                f"REFUS : le compte {a.get('id')} « {a.get('name')} » pointe sur l'hote IMAP "
                f"« {(a.get('imap_host') or '').lower()} », qui n'est pas un serveur de test "
                f"({', '.join(sorted(ALLOWED_MAIL_HOSTS))}). La suite ne demarre pas tant "
                "qu'un compte non jetable est accessible."
            )
        # SMTP : facultatif (un compte sans SMTP est legitime), mais s'il est renseigne il
        # doit etre jetable — la suite envoie reellement des emails.
        smtp = a.get("smtp_host")
        if smtp and not _host_ok(smtp):
            raise GuardError(
                f"REFUS : le compte {a.get('id')} « {a.get('name')} » envoie via l'hote SMTP "
                f"« {str(smtp).lower()} », qui n'est pas un serveur de test "
                f"({', '.join(sorted(ALLOWED_MAIL_HOSTS))}). La suite envoie de vrais emails : "
                "elle passerait par un relais tiers depuis une adresse reelle."
            )
    cible = next((a for a in accounts if a.get("id") == target_id), None)
    if cible is None:
        vus = ", ".join(str(a.get("id")) for a in accounts)
        raise GuardError(
            f"REFUS : le compte cible {target_id} n'est pas accessible avec ce jeton "
            f"(comptes visibles : {vus}). Ajustez MAILIA_QA_ACCOUNT_ID."
        )
    return cible


def run_guard() -> dict:
    """Controle complet. Leve GuardError si la suite ne doit pas demarrer."""
    global _IDENTITE_VALIDEE, _HOTE_CIBLE
    _IDENTITE_VALIDEE = None
    _HOTE_CIBLE = None

    for probleme in CFG.problemes():
        raise GuardError(f"REFUS : configuration inexploitable — {probleme}")

    if not CFG.token:
        raise GuardError(
            "REFUS : aucun jeton fourni. Renseignez MAILIA_QA_TOKEN ou creez "
            "tests/.qa_token avec le jeton du compte QA."
        )

    r = API.get("/auth/me")
    if r.status == 401:
        raise GuardError("REFUS : jeton refuse par l'API (401). Il est probablement expire.")
    if r.status != 200:
        raise GuardError(f"REFUS : /auth/me a repondu {r.status} — {r.detail[:120]}")
    me = r.json() or {}
    check_identity(me)

    ra = API.get("/accounts/")
    if ra.status != 200:
        raise GuardError(f"REFUS : impossible de lister les comptes ({ra.status}).")
    comptes = ra.json() or []
    cible = check_accounts(comptes, CFG.account_id)

    _HOTE_CIBLE = (cible.get("imap_host") or "").lower()
    _IDENTITE_VALIDEE = {"id": me["id"], "email": me.get("email")}
    return {
        "identite": me.get("email"),
        "user_id": me.get("id"),
        "compte_cible": f"{cible.get('id')} « {cible.get('name')} » -> {cible.get('imap_host')}",
        "nb_comptes": len(comptes),
    }

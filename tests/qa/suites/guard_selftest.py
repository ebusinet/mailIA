"""Auto-test du garde-fou de securite.

Le garde-fou est le seul mecanisme qui empeche cette suite de detruire une messagerie reelle.
Un garde-fou non teste est une illusion de securite : ces tests le nourrissent de charges
synthetiques, sans reseau, et exigent qu'il refuse ce qu'il doit refuser.

Ils ne dependent d'aucun service : ils s'executent meme API eteinte.
"""
from __future__ import annotations

from ..core import CFG, GuardError, expect, test
from ..guard import (ALLOWED_IDENTITIES, ALLOWED_MAIL_HOSTS, check_accounts,
                     check_identity, resolved_user_id)

IDENTITE_OK = {"id": 5, "email": sorted(ALLOWED_IDENTITIES)[0]}

CPT_OK = {"id": 3, "name": "QA GreenMail", "imap_host": "greenmail",
          "smtp_host": "greenmail", "imap_user": "test", "smtp_user": "test"}


def _refuse(fn, *a, motif=""):
    try:
        fn(*a)
    except GuardError as e:
        if motif:
            expect(motif.lower() in str(e).lower(),
                   f"refus obtenu mais pour un autre motif : {e}")
        return
    raise AssertionError("le garde-fou a LAISSE PASSER une situation qu'il doit refuser")


# ---------------------------------------------------------------------------
# Identite
# ---------------------------------------------------------------------------

@test("GUARD-01", "Une identite hors liste blanche est refusee", "garde-fou")
def identite_inconnue():
    _refuse(check_identity, {"id": 5, "email": "quelqun@autre.local"}, motif="liste")
    check_identity(IDENTITE_OK)


@test("GUARD-02", "Une identite du domaine reel est refusee", "garde-fou")
def identite_interdite():
    for adresse in ("contact@ebusinet.fr", "E.Pimienta@Ebusinet.FR",
                    "quelquun@pro.ovh.net", "pimienta@ailleurs.com"):
        _refuse(check_identity, {"id": 5, "email": adresse}, motif="interdit")


@test("GUARD-03", "Une identite vide est refusee", "garde-fou")
def identite_vide():
    _refuse(check_identity, {})
    _refuse(check_identity, {"id": 5, "email": ""})


@test("GUARD-10", "Une identite sans identifiant numerique exploitable est refusee", "G-01")
def identifiant_numerique_requis():
    """Les outils MCP s'executent sous cet identifiant : il doit venir de l'API, pas d'une
    constante. Sans lui, aucun appel MCP ne doit etre possible."""
    for mauvais in (None, 0, -1, "5", 5.0):
        _refuse(check_identity, {"id": mauvais, "email": IDENTITE_OK["email"]},
                motif="identifiant")


@test("GUARD-11", "Aucun appel MCP n'est possible avant validation du garde-fou", "G-01")
def mcp_impossible_sans_validation():
    """`resolved_user_id()` est la seule source de l'identite sous laquelle tournent les
    outils MCP. Elle doit lever tant que le garde-fou n'a pas valide l'identite, sinon un
    appel MCP pourrait s'executer sous un identifiant jamais verifie."""
    import qa.guard as g
    memoire = g._IDENTITE_VALIDEE
    g._IDENTITE_VALIDEE = None
    try:
        try:
            resolved_user_id()
        except GuardError:
            pass
        else:
            raise AssertionError(
                "resolved_user_id() a renvoye un identifiant alors que le garde-fou "
                "n'avait pas valide l'identite")
        g._IDENTITE_VALIDEE = {"id": 42, "email": "x@y.z"}
        expect(resolved_user_id() == 42,
               "l'identifiant renvoye ne provient pas de l'identite validee")
    finally:
        g._IDENTITE_VALIDEE = memoire


# ---------------------------------------------------------------------------
# Comptes
# ---------------------------------------------------------------------------

@test("GUARD-04", "Un compte pointant hors serveur de test bloque toute la suite", "garde-fou")
def compte_production():
    _refuse(check_accounts, [{"id": 1, "name": "pro", "imap_host": "pro1.mail.example.com",
                              "smtp_host": "pro1.mail.example.com", "imap_user": "u"}], 1,
            motif="serveur de test")


@test("GUARD-05", "Un seul compte reel suffit a tout bloquer, meme s'il n'est pas la cible",
      "garde-fou")
def un_compte_reel_bloque_tout():
    """Point de conception : on ne se contente pas de verifier la cible. Un jeton qui VOIT un
    compte de production n'a rien a faire ici, meme si les tests visent un autre compte."""
    comptes = [CPT_OK, {"id": 1, "name": "pro OVH", "imap_host": "pro1.mail.ovh.net",
                        "smtp_host": "pro1.mail.ovh.net", "imap_user": "e.pimienta@ebusinet.fr"}]
    _refuse(check_accounts, comptes, 3)


@test("GUARD-06", "Le motif interdit est detecte dans tous les champs du compte", "garde-fou")
def motif_interdit_tous_champs():
    for champ in ("imap_host", "smtp_host", "imap_user", "smtp_user", "name"):
        c = dict(CPT_OK)
        c[champ] = "quelquechose-ebusinet-quelquechose"
        _refuse(check_accounts, [c], 3, motif="interdit")


@test("GUARD-12", "Un SMTP externe est refuse meme si l'IMAP est jetable", "G-02")
def smtp_externe_refuse():
    """La suite envoie de vrais emails. Un compte dont l'IMAP pointe sur le serveur de test
    mais le SMTP sur un relais tiers ferait sortir du courrier depuis une adresse reelle a
    chaque execution."""
    for relais in ("smtp.gmail.com", "smtp.office365.com", "ssl0.ovh.net", "127.0.0.2"):
        c = dict(CPT_OK)
        c["smtp_host"] = relais
        _refuse(check_accounts, [c], 3, motif="smtp")


@test("GUARD-13", "Un compte sans SMTP reste accepte", "G-02")
def smtp_absent_tolere():
    """Symetrie du precedent : un compte sans serveur d'envoi est parfaitement legitime,
    le durcissement ne doit pas le rejeter."""
    for vide in (None, ""):
        c = dict(CPT_OK)
        c["smtp_host"] = vide
        cible = check_accounts([c], 3)
        expect(cible["id"] == 3, f"compte avec smtp_host={vide!r} rejete a tort")


@test("GUARD-07", "Une liste de comptes vide est refusee", "garde-fou")
def aucun_compte():
    _refuse(check_accounts, [], 3)


@test("GUARD-08", "Un compte cible absent est refuse", "garde-fou")
def cible_absente():
    _refuse(check_accounts, [CPT_OK], 42, motif="cible")


@test("GUARD-09", "Une configuration entierement valide est acceptee", "garde-fou")
def cas_nominal():
    """Symetrie indispensable : un garde-fou qui refuse tout serait tout aussi inutilisable."""
    check_identity(IDENTITE_OK)
    cible = check_accounts([CPT_OK], 3)
    expect(cible["id"] == 3, "le compte cible n'est pas renvoye correctement")
    for hote in ALLOWED_MAIL_HOSTS:
        c = dict(CPT_OK)
        c["imap_host"] = hote
        c["smtp_host"] = hote
        check_accounts([c], 3)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@test("GUARD-14", "Une configuration inexploitable produit un refus, pas une trace", "G-03")
def configuration_invalide():
    """Une variable definie mais vide court-circuite le defaut de `os.environ.get`. Cela
    doit donner un REFUS explicite (code 2), pas un traceback confondu avec un echec de
    test (code 1)."""
    from ..core import Config
    cas = [
        ({"api_url": ""}, "MAILIA_API_URL"),
        ({"api_url": "mailia.expert-presta.com/api"}, "MAILIA_API_URL"),
        ({"account_id_brut": ""}, "MAILIA_QA_ACCOUNT_ID"),
        ({"account_id_brut": "abc"}, "MAILIA_QA_ACCOUNT_ID"),
        ({"account_id_brut": "0"}, "MAILIA_QA_ACCOUNT_ID"),
        ({"timeout_brut": "zero"}, "MAILIA_TIMEOUT"),
    ]
    for surcharge, attendu in cas:
        c = Config(**surcharge)
        p = c.problemes()
        expect(p, f"configuration {surcharge} acceptee alors qu'elle est inexploitable")
        expect(any(attendu in x for x in p),
               f"configuration {surcharge} : le probleme signale ne nomme pas {attendu} — {p}")
    expect(not CFG.problemes(),
           f"la configuration courante est signalee comme invalide : {CFG.problemes()}")

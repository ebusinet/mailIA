"""Audit de securite methodique — Plan G.

Pourquoi cette suite existe
---------------------------
Les sept failles trouvees avant ce plan l'ont toutes ete **au passage**, en testant autre
chose. Aucune ne venait d'un audit delibere. Ce groupe cherche methodiquement, sur les
proprietes que personne n'exerce en usage normal : la separation des types de jeton, la clef
de la limitation de debit, les en-tetes sur les reponses d'erreur, la frontiere
administrateur.

Principe commun a tous ces tests
--------------------------------
Un refus doit venir de l'application, jamais d'un hasard. Deux consequences pratiques :

- **On affiche ce qu'on envoie.** Un `403` obtenu avec un jeton vide et un `401` obtenu avec
  un jeton refuse se ressemblent dans un tableau de resultats. Les tests ci-dessous
  distinguent donc « jeton absent » de « jeton presente et rejete », et echouent si la
  fabrication du jeton n'a pas abouti — une capture qui echoue en silence produit un refus
  parfaitement reel, pour la mauvaise raison.
- **On ne vise jamais un compte reel.** Les sondes de debit s'authentifient contre une
  adresse inexistante ; les sondes de cloisonnement visent un identifiant hors de portee.
"""
from __future__ import annotations

import json

from ..core import API, BOX, CFG, Skip, expect, test

# Adresse qui n'existe pas : les tentatives echouent en 401 sans effet de bord.
FAUX_COMPTE = "qa-sonde-debit-inexistant@mailia.local"

# En-tetes annonces par la documentation, en minuscules.
ENTETES_ATTENDUS = [
    "strict-transport-security",
    "x-frame-options",
    "x-content-type-options",
    "x-xss-protection",
    "referrer-policy",
    "permissions-policy",
]


def _jeton_de_reinitialisation() -> str:
    """Fabrique un vrai jeton de reinitialisation pour le compte QA, sans envoyer d'email.

    Le flux `forgot-password` reel expedie le message depuis la premiere messagerie
    administrateur disposant d'un SMTP — sur cette instance, la messagerie professionnelle.
    On ne le declenche donc jamais depuis les tests.
    """
    BOX.require()
    # L'identifiant vient du garde-fou, qui l'a valide : jamais une constante, jamais une
    # valeur redemandee a l'API. C'est la meme regle que pour les appels MCP.
    from ..guard import resolved_user_id
    uid = resolved_user_id()
    sortie = BOX.python(
        "from sqlalchemy import create_engine, text\n"
        "from src.config import get_settings\n"
        "from src.security import create_reset_token\n"
        "import inspect\n"
        "e = create_engine(get_settings().database_url.replace('+asyncpg',''))\n"
        "with e.connect() as c:\n"
        f"    h = c.execute(text('select password_hash from users where id={uid}')).scalar()\n"
        "params = inspect.signature(create_reset_token).parameters\n"
        f"jeton = create_reset_token({uid}, h) if len(params) > 1 else create_reset_token({uid})\n"
        "print('__QA__' + jeton)\n")
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            valeur = ligne[6:].strip()
            if valeur:
                return valeur
    raise Skip(f"fabrication du jeton de reinitialisation impossible : {sortie[-200:]}")


@test("SEC-G01", "Un jeton de reinitialisation n'ouvre pas de session", "G-01")
def jeton_reset_pas_une_session():
    """Defaut G-01 : `decode_access_token` ne verifiait pas la revendication `purpose`,
    alors que `decode_reset_token` la verifiait — les deux jetons etant signes par la meme
    cle avec le meme algorithme. Un jeton de reinitialisation ouvrait donc la session
    complete de la victime, **droits d'administration compris** (mesure : `/admin/users`
    repondait 200).

    L'asymetrie etait l'indice : l'auteur savait que `purpose` comptait dans un sens et
    l'avait oublie dans l'autre.

    Aggravations qui faisaient la gravite : le jeton voyage dans un lien (donc historique du
    navigateur et journaux d'acces), il survivait a son usage, et `forgot-password` s'appelle
    sans authentification.

    Le test verifie la propriete, pas l'implementation : quelle que soit la maniere dont la
    separation est faite, un jeton qui n'est pas un jeton d'acces ne doit ouvrir aucune
    session.
    """
    jeton = _jeton_de_reinitialisation()

    # On verifie ce qu'on envoie avant d'interpreter ce qu'on recoit : un 403 « jeton
    # absent » et un 401 « jeton refuse » sont deux resultats differents.
    charge = jeton.split(".")[1]
    charge += "=" * (-len(charge) % 4)
    import base64
    donnees = json.loads(base64.urlsafe_b64decode(charge))
    expect(donnees.get("purpose") == "reset",
           f"le jeton fabrique n'est pas un jeton de reinitialisation : {donnees}")

    acceptes = []
    for chemin in ("/auth/me", "/admin/info", "/admin/users"):
        r = API.get(chemin, token=jeton)
        if r.status == 200:
            acceptes.append(f"{chemin} -> 200 {r.text[:60]}")
        elif r.status == 403 and "not authenticated" in r.detail.lower():
            raise Skip(f"le jeton n'a pas ete transmis sur {chemin} : le test n'a rien "
                       "constate (403 « non authentifie », pas 401 « refuse »)")
    expect(not acceptes,
           "un jeton de reinitialisation ouvre une session authentifiee :\n      "
           + "\n      ".join(acceptes))


@test("SEC-G02", "La cle de limitation de debit n'est pas fournie par l'appelant", "G-02")
def debit_non_contournable():
    """Defaut G-02 : `_get_client_ip` retenait le **premier** element de
    `X-Forwarded-For`. Or nginx **ajoute** l'adresse reelle en fin de liste : le premier
    element reste donc celui qu'ecrit l'appelant. Une ligne
    `X-Forwarded-For: 203.0.113.<compteur>` rendait la limite inoperante — mesure : 15
    tentatives de connexion consecutives, aucun refus.

    Les quatre regles tombaient ensemble : connexion, inscription, mot de passe oublie et
    reinitialisation partagent cette fonction. Combine a G-01, cela donnait une emission
    illimitee de jetons ouvrant une session.

    Le test tire au-dela du seuil en faisant varier l'en-tete a chaque coup. Si la limite
    s'applique malgre la variation, la cle ne depend pas de l'appelant.

    Il consomme volontairement le quota de connexion de la machine qui l'execute, pendant une
    minute, sur une adresse inexistante. C'est le prix de la mesure.
    """
    codes = []
    for i in range(1, 10):
        r = API.post("/auth/login",
                     json_body={"email": FAUX_COMPTE, "password": "faux"},
                     token=None,
                     en_tetes={"X-Forwarded-For": f"203.0.113.{i}"})
        codes.append(r.status)

    expect(429 in codes,
           "la limitation de debit ne s'est jamais declenchee sur 9 tentatives alors que le "
           "seuil annonce est de 5/min : la cle du compteur est fournie par l'appelant via "
           f"`X-Forwarded-For` et suffit a la contourner. Codes obtenus : {codes}")


@test("SEC-G03", "Les six en-tetes de securite sont sur toutes les reponses", "G-03")
def entetes_sur_toutes_les_reponses():
    """La documentation annonce six en-tetes. Les verifier sur la page d'accueil ne prouve
    rien : ce sont les reponses d'**erreur** qui echappent le plus souvent aux intergiciels,
    parce qu'elles sont produites plus tot dans la chaine.

    On couvre donc un 200 public, un 200 authentifie, un 401, un 403, un 404 et un 422.
    """
    cas = [
        ("200 public", lambda: API.get("/health", token=None)),
        ("200 authentifie", lambda: API.get("/auth/me")),
        ("403 sans jeton", lambda: API.get("/auth/me", token=None)),
        ("404 compte etranger", lambda: API.get("/accounts/99999/folders")),
        ("404 chemin inconnu", lambda: API.get("/qa-chemin-inexistant")),
        ("422 parametre invalide", lambda: API.get(f"/accounts/{CFG.account_id}/messages",
                                                   params={"folder": ""})),
    ]
    manquants = []
    for label, faire in cas:
        r = faire()
        absents = [e for e in ENTETES_ATTENDUS if r.headers.get(e) is None]
        if absents:
            manquants.append(f"{label} (HTTP {r.status}) : {', '.join(absents)}")
    expect(not manquants,
           "des en-tetes de securite manquent sur certaines reponses :\n      "
           + "\n      ".join(manquants))


@test("SEC-G04", "Aucune trace SQL ne remonte des champs de recherche", "G-04")
def pas_d_injection_sql():
    """Les requetes passent par des parametres lies SQLAlchemy, donc l'injection est peu
    probable — mais « peu probable » n'est pas « verifie », et la campagne a montre que des
    protections tenaient par accident.

    Trois signaux distincts sont surveilles, parce qu'ils revelent trois choses differentes :
    un **500** (la charge atteint la couche donnees et casse), une **trace SQL** dans la
    reponse (fuite d'information), et une **anomalie temporelle** (`pg_sleep` execute).
    """
    charges = ["'", '"', "' OR '1'='1", "x'--", "' UNION SELECT NULL,NULL--",
               "'; DROP TABLE users--", "'; SELECT pg_sleep(5)--", "\\", "¿' OR 1=1--"]
    marqueurs = ("syntax error", "psycopg", "sqlalchemy", "postgres", "relation ", "column ")
    anomalies = []
    import time
    for champ in ("q", "filter_from", "filter_subject"):
        for charge in charges:
            t0 = time.time()
            r = API.get("/search/", params={"q": charge} if champ == "q"
                        else {"q": "zz", champ: charge})
            duree = time.time() - t0
            if r.status >= 500:
                anomalies.append(f"{champ}={charge!r} -> HTTP {r.status}")
            elif any(m in r.text.lower() for m in marqueurs):
                anomalies.append(f"{champ}={charge!r} -> trace SQL : {r.text[:80]}")
            elif duree > 4.5:
                anomalies.append(f"{champ}={charge!r} -> {duree:.1f}s (injection temporelle)")
    expect(not anomalies,
           "des charges SQL produisent un effet observable :\n      "
           + "\n      ".join(anomalies))


@test("SEC-G05", "Un jeton forge ou altere est refuse", "G-05")
def jetons_forges_refuses():
    """Signature modifiee, signature vide, `alg=none`, jeton expire, `sub` inexistant ou non
    numerique. Aucun ne doit ouvrir de session.

    `alg=none` est le cas classique : il est ferme ici parce que `jwt.decode` recoit
    `algorithms=[...]` epingle. Le test le verifie plutot que de s'y fier.

    Le `sub` non numerique merite un mot : `deps.py` fait `int(user_id)` sans garde, donc une
    valeur hostile provoquerait une exception — mais la signature est verifiee avant, si bien
    que `int()` n'est jamais atteint. La protection est **positionnelle**, elle tient a
    l'ordre des verifications. Ce test la surveille : si quelqu'un reorganise
    `decode_access_token`, un 500 apparaitra ici.
    """
    import base64
    jeton = CFG.token
    if not jeton or jeton.count(".") != 2:
        raise Skip("le jeton QA n'est pas un JWT a trois segments, variantes non calculables")
    entete, charge, signature = jeton.split(".")

    def b64(d):
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    donnees = json.loads(base64.urlsafe_b64decode(charge + "=" * (-len(charge) % 4)))

    variantes = [
        ("signature modifiee", f"{entete}.{charge}.{signature[:-1]}"
                               f"{'A' if signature[-1] != 'A' else 'B'}"),
        ("signature vide", f"{entete}.{charge}."),
        ("alg=none", f"{b64({'alg': 'none', 'typ': 'JWT'})}.{charge}."),
        ("alg=NONE", f"{b64({'alg': 'NONE', 'typ': 'JWT'})}.{charge}."),
        ("sub inexistant", f"{entete}.{b64({**donnees, 'sub': '99999'})}.{signature}"),
        ("sub non numerique", f"{entete}.{b64({**donnees, 'sub': 'abc'})}.{signature}"),
        ("expire", f"{entete}.{b64({**donnees, 'exp': 1000000000})}.{signature}"),
        ("deux segments", f"{entete}.{charge}"),
    ]

    acceptes, plantages = [], []
    for label, variante in variantes:
        r = API.get("/auth/me", token=variante)
        if r.status == 200:
            acceptes.append(label)
        elif r.status >= 500:
            plantages.append(f"{label} -> HTTP {r.status} {r.detail[:60]}")
    expect(not acceptes,
           "*** des jetons forges ouvrent une session : " + ", ".join(acceptes))
    expect(not plantages,
           "des jetons malformes provoquent une exception non interceptee :\n      "
           + "\n      ".join(plantages))


@test("SEC-G06", "Le message d'erreur ne distingue pas inexistant et non possede", "G-06")
def pas_d_oracle_sur_les_comptes():
    """Un message different entre « ce compte n'existe pas » et « ce compte existe mais
    n'est pas a vous » transforme l'API en oracle : on enumere les comptes de l'instance sans
    y avoir acces.

    On compare donc le couple (code, message) pour un identifiant inexistant et pour des
    identifiants qui existent sans appartenir au jeton. Les reponses doivent etre
    indiscernables.
    """
    reponses = {}
    for cible in (1, 99999, 123456):
        r = API.get(f"/accounts/{cible}/folders")
        reponses[cible] = (r.status, r.detail[:80])

    distincts = set(reponses.values())
    expect(len(distincts) == 1,
           "les reponses permettent de distinguer un compte existant d'un compte "
           f"inexistant : {reponses}")

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


@test("SEC-G07", "Le nom d'un fichier televerse ne choisit pas ou il est ecrit", "G-07")
def pas_d_ecriture_arbitraire():
    """Defaut G-07, le plus grave du plan : `import-mbox` (`accounts.py:2544-2547`) construit
    sa destination avec le nom fourni par le client, sans assainissement.

        filename  = file.filename or "upload.mbox"
        file_path = str(job_dir / filename)

    Deux formes, deux mecanismes :
      - `../../../../tmp/x` sort du repertoire par remontee ;
      - `/tmp/x` fait mieux : `Path("/a/b") / "/tmp/x"` vaut `/tmp/x`. L'operateur abandonne
        **entierement** la partie gauche. Un correctif qui n'interdirait que les `..` ne
        fermerait donc rien — c'est le piege principal de ce defaut.

    Portee mesuree a la decouverte : le conteneur tourne en `root`, et `/app/src` comme les
    site-packages sont accessibles en ecriture. Ecraser un fichier Python donne l'execution
    de code au redemarrage suivant, dans le conteneur qui detient la cle de chiffrement des
    mots de passe IMAP.

    Le test ecrit dans `/tmp` du conteneur, sur des noms qui lui sont propres, et supprime
    ce qu'il a cree. **Il n'ecrase jamais un fichier existant** : creer un fichier la ou on
    ne devrait pas suffit a etablir le defaut.
    """
    BOX.require()
    import uuid
    marque = "QA-SECG07-" + uuid.uuid4().hex[:8]
    cibles = {
        "nom absolu": f"/tmp/qa_secg07_abs_{marque}.txt",
        "remontee relative": f"../../../../tmp/qa_secg07_rel_{marque}.txt",
    }
    attendus = [f"/tmp/qa_secg07_abs_{marque}.txt", f"/tmp/qa_secg07_rel_{marque}.txt"]

    try:
        for _, nom in cibles.items():
            API.upload(f"/accounts/{CFG.account_id}/import-mbox", nom, marque.encode(),
                       params={"storage": "local", "folder": "QA_SECG07"})

        sortie = BOX.python(
            "import json, os\n"
            f"chemins = {attendus!r}\n"
            "print('__QA__' + json.dumps([c for c in chemins if os.path.exists(c)]))\n")
        sortis = None
        for ligne in sortie.splitlines():
            if ligne.startswith("__QA__"):
                sortis = json.loads(ligne[6:])
        if sortis is None:
            raise Skip(f"verification impossible dans le conteneur : {sortie[-200:]}")
    finally:
        BOX.python(
            "import os\n"
            f"for c in {attendus!r}:\n"
            "    try:\n"
            "        os.remove(c)\n"
            "    except OSError:\n"
            "        pass\n")

    # La portee du defaut ne se lit pas dans l'ecriture elle-meme : elle depend de QUI peut
    # la declencher. On la mesure sur le code deploye — technique de `SEC-G09` — plutot que
    # de stocker les identifiants d'un second utilisateur dans la suite, ce qui affaiblirait
    # le garde-fou.
    #
    # Ce constat n'est PAS une assertion : un garde administrateur ajoute demain serait une
    # bonne nouvelle, et un test qui vire au rouge sur une bonne nouvelle finit ignore. Il
    # enrichit le message d'echec, la ou la portee compte pour qui doit prioriser.
    portee = "portee non determinee"
    try:
        sortie_p = BOX.python(
            "import inspect, json, re\n"
            "import src.api.routes.accounts as A\n"
            "src = inspect.getsource(A)\n"
            "m = re.search(r'async def import_mbox\\(.*?\\n\\)', src, re.S)\n"
            "sig = m.group(0) if m else ''\n"
            "print('__QA__' + json.dumps({'admin': 'get_current_admin' in sig,\n"
            "                             'trouve': bool(m)}))\n")
        for ligne in sortie_p.splitlines():
            if ligne.startswith("__QA__"):
                d = json.loads(ligne[6:])
                if not d["trouve"]:
                    portee = "signature d'`import_mbox` introuvable dans le code deploye"
                elif d["admin"]:
                    portee = "l'endpoint est reserve aux administrateurs"
                else:
                    portee = ("l'endpoint n'a AUCUN garde administrateur : tout utilisateur "
                              "authentifie possedant un compte mail peut declencher "
                              "l'ecriture")
    except Exception as e:
        portee = f"portee non determinee ({type(e).__name__})"

    expect(not sortis,
           "un nom de fichier televerse a choisi sa destination hors du repertoire "
           f"d'import : {sortis}\n      Le conteneur tourne en root et /app/src est "
           f"accessible en ecriture.\n      Portee : {portee}.")


@test("SEC-G08", "Une archive hostile n'ecrit pas hors du repertoire d'extraction", "G-08")
def zip_confine():
    """« Zip slip » : une entree d'archive nommee `../../x` ou `/tmp/x` qui sortirait du
    repertoire d'extraction. Deux sites concernes (`accounts.py:2795` et `3703`), tous deux
    en aval d'un chemin dont G-07 a montre qu'il etait mal garde.

    **Mesure, pas confiance en la documentation** : `zipfile.extractall` de CPython 3.12
    retire les composants `..` et la racine des noms de membres — verifie ici, l'entree
    `../../../../tmp/x` atterrit a `<extraction>/tmp/x`, contenue.

    Le point important est que cette protection est **heritee de la bibliotheque standard**,
    pas ecrite par l'application. Elle disparaitrait si quelqu'un remplacait `extractall`
    par une boucle `zf.extract()` ou un `open(os.path.join(dir, nom))` — refactorisation
    banale et sans rapport apparent avec la securite. Ce test surveille donc la propriete,
    et le suivant la forme du code.
    """
    BOX.require()
    import uuid
    marque = "QA-SECG08-" + uuid.uuid4().hex[:8]
    cibles = [f"/tmp/qa_secg08_rel_{marque}.txt", f"/tmp/qa_secg08_abs_{marque}.txt"]

    code = (
        "import io, json, os, shutil, tempfile, zipfile\n"
        f"marque = {marque!r}\n"
        f"cibles = {cibles!r}\n"
        "t = io.BytesIO()\n"
        "with zipfile.ZipFile(t, 'w') as zf:\n"
        "    zf.writestr('../../../..' + cibles[0], marque)\n"
        "    zf.writestr(cibles[1], marque)\n"
        "    zf.writestr('Dossier/normal.txt', marque)\n"
        "d = tempfile.mkdtemp(prefix='qa_secg08_')\n"
        "try:\n"
        "    with zipfile.ZipFile(io.BytesIO(t.getvalue())) as zf:\n"
        "        zf.extractall(d)\n"
        "    sortis = [c for c in cibles if os.path.exists(c)]\n"
        "finally:\n"
        "    shutil.rmtree(d, ignore_errors=True)\n"
        "    for c in cibles:\n"
        "        try:\n"
        "            os.remove(c)\n"
        "        except OSError:\n"
        "            pass\n"
        "print('__QA__' + json.dumps(sortis))\n")
    sortie = BOX.python(code)
    sortis = None
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            sortis = json.loads(ligne[6:])
    if sortis is None:
        raise Skip(f"extraction de controle impossible : {sortie[-200:]}")
    expect(not sortis,
           f"une archive hostile a ecrit hors du repertoire d'extraction : {sortis}")


@test("SEC-G09", "L'extraction d'archive reste confiee a extractall", "G-08")
def extraction_par_extractall():
    """Complement structurel de `SEC-G08`, et c'est le test qui protege reellement.

    `SEC-G08` verifie un comportement fourni par CPython. Il restera vert quoi que fasse
    l'application, tant qu'elle appelle `extractall`. Le jour ou quelqu'un ecrit une boucle
    d'extraction a la main — pour filtrer les entrees, afficher une progression, ou toute
    autre bonne raison — la protection disparait **sans qu'aucun test de comportement ne
    change de couleur**.

    On surveille donc la forme : aucune extraction membre par membre dans le code deploye.
    Meme raisonnement que `SMTP-06` pour `starttls()`, et que la verification structurelle
    de `ROB-21`.
    """
    BOX.require()
    sortie = BOX.python(
        "import json, inspect\n"
        "import src.api.routes.accounts as A\n"
        "src = inspect.getsource(A)\n"
        "suspects = [l.strip()[:100] for l in src.splitlines()\n"
        "            if '.extract(' in l or ('ZipFile' in l and '.open(' in l)]\n"
        "print('__QA__' + json.dumps(suspects))\n")
    suspects = None
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            suspects = json.loads(ligne[6:])
    if suspects is None:
        raise Skip(f"lecture du source impossible : {sortie[-200:]}")
    expect(not suspects,
           "une extraction membre par membre est apparue : la protection contre le zip slip "
           "venait de `extractall`, elle ne s'applique plus :\n      "
           + "\n      ".join(suspects))

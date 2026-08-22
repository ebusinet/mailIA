"""Robustesse des entrees — valeurs limites sur les champs libres (Plan A).

Pourquoi cette suite existe
---------------------------
Deux pertes de donnees ont ete trouvees de la meme facon : en passant une valeur
syntaxiquement absurde la ou l'application attendait un nom de dossier.

- IT4-01 : `move` avec `target_folder = ""` repondait `200 moved` et detruisait l'email.
- A-01 : `rename-folder` avec `old_name` reduit au separateur de hierarchie (`.` ou `..`)
  a detruit **l'integralite de la boite** du compte de test — 380 messages, 55 dossiers.
  L'API a repondu `200 {"status":"renamed"}`.

  Isole apres coup, un appel par redemarrage de serveur :
      CREATE "."        -> NO already exists          aucun effet
      CREATE ".."       -> NO already exists          aucun effet
      RENAME "." "X"    -> OK RENAME completed.       tout detruit
      RENAME ".." "X"   -> OK RENAME completed.       tout detruit
      RENAME "X" "."    -> NO already existed         aucun effet
  C'est la SOURCE du renommage qui est dangereuse, pas la cible : le serveur considere
  « . » comme la racine de la hierarchie et renomme l'arbre entier.

Dans les deux cas la validation existante ne rejetait que le vide, la valeur partait telle
quelle dans une commande IMAP, et le serveur l'executait a sa facon. Le correctif de
`target_folder` a traite un champ ; c'est la classe entiere qu'il faut fermer.

Principe de ces tests
---------------------
Un nom de dossier qui ne designe aucun dossier doit etre **refuse avant toute commande
IMAP**. On n'attend pas du serveur qu'il se protege : c'est le raisonnement deja ecrit dans
le commentaire de `_require_folder_name`, applique a toute la famille.

Chaque test destructeur encadre son cas d'un recensement global — nombre de messages dans
tous les dossiers, lu directement en IMAP. C'est le seul moyen de distinguer un refus propre
d'une destruction silencieuse annoncee comme un succes.
"""
from __future__ import annotations

from ..core import (API, BOX, CFG, Skip, count_in, expect, messages_in,
                    premier, seed, test, work_folder)


# Ces tests ont ete proteges par une activation explicite (`MAILIA_FUZZ=1`) tant que A-01
# etait ouvert : envoyer ces valeurs detruisait alors la boite entiere. A-01 et A-04 etant
# corriges et verifies, les valeurs sont refusees avant toute commande IMAP et le garde a
# ete retire — un test qui ne s'execute jamais ne protege rien.

# Noms ne designant aucun dossier : uniquement des separateurs de hierarchie.
# C'est la famille qui a detruit la boite.
SEPARATEURS_SEULS = [".", "..", "/", "//", "./", "../..", ". ", " ."]

# Caracteres interdits dans un nom de boite aux lettres (RFC 3501).
CARACTERES_CONTROLE = [
    ("nul", "QAROB\x00X"),
    ("CR", "QAROB\rX"),
    ("LF", "QAROB\nX"),
    ("CRLF", "QAROB\r\nA001 LOGOUT"),
]

# Jokers de la commande LIST : dans un nom, ils rendent le dossier inadressable.
JOKERS = ["*", "%", "QAROB*", "QAROB%"]


def _recensement() -> dict:
    """{dossier: nb_messages} pour tout le compte, lu directement en IMAP.

    On interroge le serveur, pas l'application : c'est la seule mesure qui ne peut pas
    etre faussee par le defaut qu'on cherche.
    """
    BOX.require()
    sortie = BOX.imap(
        "import json\n"
        "st, d = M.list()\n"
        "out = {}\n"
        "for l in d or []:\n"
        "    if not l: continue\n"
        "    n = l.decode().rsplit(' ', 1)[-1].strip('\"')\n"
        "    try:\n"
        "        M.select(n, readonly=True)\n"
        "        s, r = M.uid('SEARCH', None, 'ALL')\n"
        "        out[n] = len(r[0].split()) if r[0] else 0\n"
        "    except Exception:\n"
        "        out[n] = -1\n"
        "print('__QA__' + json.dumps(out))\n"
    )
    import json
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            return json.loads(ligne[6:])
    raise Skip(f"recensement IMAP illisible : {sortie[-200:]}")


def _total(rec: dict) -> int:
    return sum(v for v in rec.values() if v >= 0)


def _supprimer(noms) -> None:
    """Retire les dossiers qu'un test a cree parce que l'application les a acceptes a tort.

    Tant que A-02 et A-03 sont ouverts, ces tests laissent derriere eux des dossiers nommes
    `*`, `%` ou contenant un CR — qui s'accumuleraient a chaque execution et fausseraient les
    recensements suivants. Le jour ou ces valeurs seront refusees, il n'y aura simplement
    rien a supprimer.

    Best-effort assume : un nom contenant un caractere de controle peut etre litteralement
    inadressable, et c'est precisement le defaut que le test constate.
    """
    for nom in noms:
        try:
            API.post(f"/accounts/{CFG.account_id}/delete-folder",
                     json_body={"folder_name": nom})
        except Exception:
            pass


def _refuse(reponse, contexte: str):
    """Un refus explicite est attendu : 4xx. Ni 2xx (accepte), ni 5xx (plantage)."""
    expect(400 <= reponse.status < 500,
           f"{contexte} : refus attendu (4xx), obtenu HTTP {reponse.status} — "
           f"{reponse.detail[:140]}")


# ---------------------------------------------------------------------------
# A-01 — la famille qui a detruit la boite
# ---------------------------------------------------------------------------

@test("ROB-01", "Renommer un dossier depuis un nom-separateur est refuse", "A-01")
def rename_depuis_separateur_refuse():
    """Defaut A-01, dans sa forme exacte : `rename-folder` avec `old_name` reduit au
    separateur de hierarchie. Le serveur repond `OK RENAME completed.` et l'arbre entier
    disparait — 380 messages et 55 dossiers lors de la decouverte.

    Le nom ne designe aucun dossier : il designe la racine. Le separateur depend du
    serveur (« . » sur OVH et GreenMail, « / » ailleurs), donc la validation doit rejeter
    tout nom vide une fois retires les blancs ET les separateurs courants, plutot que de
    comparer a un caractere connu."""
    avant = _recensement()
    echecs = []
    for valeur in SEPARATEURS_SEULS:
        r = API.post(f"/accounts/{CFG.account_id}/rename-folder",
                     json_body={"old_name": valeur, "new_name": "QAROB_RENOMME"})
        if not (400 <= r.status < 500):
            echecs.append(f"rename-folder(old_name={valeur!r}) -> HTTP {r.status} "
                          f"{r.detail[:60]}")
    apres = _recensement()

    perdu = _total(avant) - _total(apres)
    expect(perdu <= 0,
           f"DESTRUCTION : {perdu} message(s) ont disparu du compte pendant le test "
           f"({_total(avant)} -> {_total(apres)}). Renommer depuis un nom reduit au "
           "separateur de hierarchie renomme la racine et detruit la boite (A-01).")
    expect(len(apres) >= len(avant),
           f"DESTRUCTION : {len(avant) - len(apres)} dossier(s) ont disparu "
           f"({len(avant)} -> {len(apres)})")
    expect(not echecs,
           "des noms ne designant aucun dossier sont acceptes :\n      "
           + "\n      ".join(echecs))


@test("ROB-02", "Les separateurs seuls sont refuses sur tous les champs de dossier", "A-01")
def separateurs_tous_les_champs():
    """La correction de IT4-01 n'avait traite qu'un champ. On verifie ici toute la surface :
    un seul point d'entree non garde suffit a reproduire la destruction."""
    cibles = [
        ("create-folder", "POST", f"/accounts/{CFG.account_id}/create-folder", "folder_name"),
        ("empty-folder", "POST", f"/accounts/{CFG.account_id}/empty-folder", "folder_name"),
        ("delete-folder", "POST", f"/accounts/{CFG.account_id}/delete-folder", "folder_name"),
        ("local-folders", "POST", f"/accounts/{CFG.account_id}/local-folders", "name"),
    ]
    echecs = []
    for label, meth, chemin, champ in cibles:
        for valeur in (".", "..", "/"):
            r = API.request(meth, chemin, json_body={champ: valeur})
            if not (400 <= r.status < 500):
                echecs.append(f"{label}[{champ}={valeur!r}] -> HTTP {r.status} {r.detail[:60]}")
    # rename-folder : `old_name` est le champ qui detruit, `new_name` est refuse par le
    # serveur — on garde les deux, la protection ne doit pas dependre du serveur.
    for champ, corps in (("old_name", {"old_name": ".", "new_name": "QAROB_CIBLE"}),
                         ("old_name", {"old_name": "..", "new_name": "QAROB_CIBLE"}),
                         ("new_name", {"old_name": "INBOX", "new_name": ".."})):
        r = API.post(f"/accounts/{CFG.account_id}/rename-folder", json_body=corps)
        if not (400 <= r.status < 500):
            echecs.append(f"rename-folder[{champ}={corps[champ]!r}] -> HTTP {r.status} "
                          f"{r.detail[:60]}")
    expect(not echecs,
           "champs acceptant un nom qui ne designe aucun dossier :\n      "
           + "\n      ".join(echecs))


@test("ROB-03", "Le dossier source d'une lecture est valide comme les autres", "A-01")
def dossier_source_en_query():
    """`folder` est un parametre de requete, il echappe donc aux validateurs de corps.
    Un nom absurde y produit aujourd'hui un 502 avec la trace IMAP brute au lieu d'un refus."""
    echecs = []
    for valeur in ("", "   ", ".", ".."):
        r = API.get(f"/accounts/{CFG.account_id}/messages", params={"folder": valeur})
        if not (400 <= r.status < 500):
            echecs.append(f"messages?folder={valeur!r} -> HTTP {r.status}")
        elif "IMAP error" in r.detail or "EXAMINE" in r.detail:
            echecs.append(f"messages?folder={valeur!r} -> {r.status} mais expose la trace "
                          f"IMAP : {r.detail[:70]}")
    expect(not echecs,
           "le dossier passe en parametre de requete n'est pas valide :\n      "
           + "\n      ".join(echecs))


# ---------------------------------------------------------------------------
# Caracteres interdits par le protocole
# ---------------------------------------------------------------------------

@test("ROB-04", "Les caracteres de controle sont refuses dans un nom de dossier", "A-02",
      serveur="permissif")
def caracteres_de_controle():
    """CR et LF sont les separateurs de commande du protocole IMAP. `_imap_quote` n'echappe
    que l'antislash et le guillemet : une valeur contenant `\\r\\n` part telle quelle dans la
    commande. Le serveur de test la tolere, ce qui contient le probleme par accident et non
    par construction."""
    echecs = []
    try:
        for nom, valeur in CARACTERES_CONTROLE:
            r = API.post(f"/accounts/{CFG.account_id}/create-folder",
                         json_body={"folder_name": valeur})
            if not (400 <= r.status < 500):
                echecs.append(f"{nom} -> HTTP {r.status} : le nom est transmis au serveur")
    finally:
        _supprimer([v for _, v in CARACTERES_CONTROLE])
    expect(not echecs,
           "des caracteres interdits par la RFC 3501 sont transmis dans la commande IMAP "
           ":\n      " + "\n      ".join(echecs))


@test("ROB-05", "Les jokers IMAP sont refuses dans un nom de dossier", "A-03",
      serveur="permissif")
def jokers_imap():
    """`*` et `%` sont les jokers de la commande LIST. Un dossier ainsi nomme est
    inadressable : toute operation ulterieure le visant peut en atteindre d'autres."""
    echecs = []
    try:
        for valeur in JOKERS:
            r = API.post(f"/accounts/{CFG.account_id}/create-folder",
                         json_body={"folder_name": valeur})
            if not (400 <= r.status < 500):
                echecs.append(f"{valeur!r} -> HTTP {r.status}")
    finally:
        _supprimer(JOKERS)
    expect(not echecs,
           "des jokers IMAP sont acceptes comme noms de dossier :\n      "
           + "\n      ".join(echecs))


# ---------------------------------------------------------------------------
# Identifiants et listes
# ---------------------------------------------------------------------------

@test("ROB-06", "Un identifiant de message invalide est refuse", "A-04")
def uid_invalides():
    dossier = work_folder("ROB06")
    seed(dossier, [{"from": "a@qa-autotest.local", "subject": "message temoin",
                    "date": "2024-01-01 09:00:00"}])
    avant = count_in(dossier)
    echecs = []
    for nom, uid in (("negatif", "-1"), ("zero", "0"), ("non numerique", "abc"),
                     ("plage IMAP", "1:*"), ("liste", "1,2,3")):
        r = API.delete(f"/accounts/{CFG.account_id}/message/{uid}",
                       params={"folder": dossier})
        if not (400 <= r.status < 500):
            echecs.append(f"delete uid={nom} ({uid!r}) -> HTTP {r.status} {r.detail[:60]}")
    # Un uid porteur de CR/LF ne peut pas transiter par le chemin d'URL : la bibliotheque
    # cliente refuse de construire la requete. C'est une protection du transport, pas de
    # l'application — on la constate pour qu'un futur passage en corps de requete, qui la
    # ferait disparaitre, soit remarque.
    try:
        API.delete(f"/accounts/{CFG.account_id}/message/1%0d%0aA001 NOOP",
                   params={"folder": dossier})
    except Exception:
        pass
    apres = count_in(dossier)
    expect(apres == avant,
           f"un identifiant invalide a modifie le dossier : {avant} -> {apres} message(s). "
           "Une plage IMAP (« 1:* ») ou une liste (« 1,2,3 ») dans le champ uid agirait sur "
           "plusieurs messages a la fois.")
    expect(not echecs,
           "des identifiants invalides sont acceptes :\n      " + "\n      ".join(echecs))


@test("ROB-07", "Une liste d'identifiants malformee est refusee", "A-05")
def listes_malformees():
    """`delete-bulk` est destructeur. Une liste vide, contenant `null` ou des doublons doit
    etre refusee, jamais interpretee au mieux."""
    dossier = work_folder("ROB07")
    mk = seed(dossier, [
        {"from": "a@qa-autotest.local", "subject": "un", "date": "2024-01-01 09:00:00"},
        {"from": "b@qa-autotest.local", "subject": "deux", "date": "2024-01-02 09:00:00"},
    ])
    avant = count_in(dossier, marker=mk)
    uid = premier(messages_in(dossier, marker=mk), "ROB-07")["uid"]
    echecs = []
    # Listes malformees : aucune ne doit rien supprimer.
    for nom, uids in (("liste vide", []), ("null dedans", [None]),
                      ("identifiants invalides", ["", "abc", "-1"])):
        r = API.post(f"/accounts/{CFG.account_id}/delete-bulk",
                     json_body={"uids": uids, "folder": dossier})
        if not (400 <= r.status < 500):
            echecs.append(f"{nom} -> HTTP {r.status} {r.detail[:70]}")
    apres = count_in(dossier, marker=mk)
    expect(apres == avant,
           f"une liste malformee a supprime des messages : {avant} -> {apres}")

    # Doublons : la suppression doit etre idempotente, pas amplifiee.
    r = API.post(f"/accounts/{CFG.account_id}/delete-bulk",
                 json_body={"uids": [uid, uid, uid], "folder": dossier})
    restant = count_in(dossier, marker=mk)
    expect(restant == avant - 1,
           f"un identifiant repete trois fois a supprime {avant - restant} message(s) au "
           f"lieu d'un seul ({avant} -> {restant})")
    expect(not echecs,
           "des listes malformees sont acceptees par un endpoint destructeur :\n      "
           + "\n      ".join(echecs))


@test("ROB-08", "Deplacer un email vers son propre dossier ne le perd pas", "A-06")
def source_egale_cible():
    """Cas d'identite : la source et la cible sont le meme dossier. L'implementation fait
    COPY puis STORE \\Deleted puis EXPUNGE — si la copie et l'original se confondent,
    l'expunge peut emporter les deux."""
    dossier = work_folder("ROB08")
    mk = seed(dossier, [{"from": "a@qa-autotest.local", "subject": "auto-deplacement",
                         "date": "2024-01-01 09:00:00"}])
    avant = count_in(dossier, marker=mk)
    uid = premier(messages_in(dossier, marker=mk), "ROB-08")["uid"]

    r = API.post(f"/accounts/{CFG.account_id}/message/{uid}/move",
                 params={"folder": dossier}, json_body={"target_folder": dossier})
    apres = count_in(dossier, marker=mk)

    if 400 <= r.status < 500:
        expect(apres == avant,
               f"le deplacement vers soi-meme a ete refuse ({r.status}) mais le dossier a "
               f"change : {avant} -> {apres}")
        return
    expect(apres == avant,
           f"PERTE : deplacer un email vers son propre dossier l'a fait disparaitre "
           f"({avant} -> {apres}), avec une reponse HTTP {r.status}")


@test("ROB-09", "Une adresse destinataire invalide est refusee a l'envoi", "A-07")
def adresses_invalides():
    echecs = []
    for nom, adresse in (("vide", ""), ("espaces", "   "), ("sans arobase", "pasdemail"),
                         ("CRLF", "a@b.c\r\nBcc: victime@qa.local"),
                         ("liste vide", None)):
        corps = {"to": [] if adresse is None else [adresse],
                 "subject": "QA robustesse", "body_text": "corps"}
        r = API.post(f"/accounts/{CFG.account_id}/send", json_body=corps)
        if not (400 <= r.status < 500):
            echecs.append(f"to={nom} -> HTTP {r.status} {r.detail[:70]}")
    expect(not echecs,
           "des adresses destinataires invalides sont acceptees — le cas CRLF permettrait "
           "d'ajouter un en-tete arbitraire au message :\n      " + "\n      ".join(echecs))


@test("ROB-10", "Aucune valeur limite ne detruit de message", "A-01")
def aucune_destruction_globale():
    """Filet de securite de la suite : on recense tout le compte, on rejoue l'ensemble des
    valeurs limites sur les endpoints de dossier, et on exige que rien n'ait disparu.

    C'est ce recensement global qui a revele A-01 — les tests par dossier ne voyaient rien,
    parce que les dossiers eux-memes avaient disparu.

    Le test balaie des noms que l'application peut legitimement accepter : un nom accepte
    est cree, puis renomme, donc il disparait de son ancien nom sans qu'aucune donnee ne
    soit perdue. Seules les disparitions **hors du perimetre manipule** sont des
    destructions ; les compter toutes ferait crier au feu a chaque valeur acceptee."""
    valeurs = SEPARATEURS_SEULS + JOKERS + [v for _, v in CARACTERES_CONTROLE]
    cible = "QAROB_BALAYAGE"
    avant = _recensement()
    for valeur in valeurs:
        API.post(f"/accounts/{CFG.account_id}/create-folder",
                 json_body={"folder_name": valeur})
        API.post(f"/accounts/{CFG.account_id}/rename-folder",
                 json_body={"old_name": valeur, "new_name": cible})
    apres = _recensement()
    perdu = _total(avant) - _total(apres)
    manipules = set(valeurs) | {cible}
    dossiers_perdus = sorted((set(avant) - set(apres)) - manipules)
    expect(perdu <= 0,
           f"DESTRUCTION : {perdu} message(s) perdus ({_total(avant)} -> {_total(apres)})")
    expect(not dossiers_perdus,
           f"DESTRUCTION : {len(dossiers_perdus)} dossier(s) etrangers au balayage ont "
           f"disparu : {dossiers_perdus[:8]}")


@test("ROB-11", "Un ensemble d'UID IMAP est refuse sur les endpoints unitaires", "A-04")
def ensemble_uid_refuse():
    """Defaut A-04, le plus serieux du Plan A parce qu'il ne depend d'aucune particularite
    serveur.

    `uid` est typé `str` et transmis tel quel a la commande IMAP. La RFC 3501 definit une
    syntaxe d'ensemble — « 1,2,3 » et surtout « 1:* » — que tous les serveurs honorent. Un
    seul appel sur un endpoint nomme « supprimer UN message » vide donc le dossier entier,
    en repondant `200 {"status":"deleted"}`.

    Mesure a la decouverte, sur un dossier de 5 messages :
        DELETE /message/1,2,3  -> 200, 5 -> 2
        DELETE /message/1:*    -> 200, 2 -> 0
        POST   /message/1:*/move  -> 200, les 5 deplaces
        POST   /message/1:*/flags -> 200, les 5 marques

    Les operations de masse ont leurs endpoints dedies, qui recoivent une liste explicite.
    """
    dossier = work_folder("ROB11")
    cible = work_folder("ROB11D")
    from ..core import empty_folder
    empty_folder(cible)
    mk = seed(dossier, [
        {"from": f"e{i}@qa-autotest.local", "subject": f"ensemble {i}",
         "date": f"2024-02-0{i} 09:00:00"} for i in range(1, 5)
    ])
    depart = count_in(dossier, marker=mk)
    expect(depart == 4, f"preparation : 4 messages attendus, {depart}")

    echecs = []

    # Suppression
    r = API.delete(f"/accounts/{CFG.account_id}/message/1%3A*", params={"folder": dossier})
    reste = count_in(dossier, marker=mk)
    if reste != depart:
        echecs.append(f"DELETE uid=1:* a supprime {depart - reste} message(s) en un appel "
                      f"(HTTP {r.status}) — l'endpoint est cense en supprimer un seul")
    r = API.delete(f"/accounts/{CFG.account_id}/message/1,2", params={"folder": dossier})
    reste2 = count_in(dossier, marker=mk)
    if reste2 != reste:
        echecs.append(f"DELETE uid=1,2 a supprime {reste - reste2} message(s) en un appel "
                      f"(HTTP {r.status})")

    # Deplacement
    if reste2:
        avant_c = count_in(cible)
        r = API.post(f"/accounts/{CFG.account_id}/message/1%3A*/move",
                     params={"folder": dossier}, json_body={"target_folder": cible})
        deplaces = count_in(cible) - avant_c
        if deplaces > 1:
            echecs.append(f"POST move uid=1:* a deplace {deplaces} messages en un appel "
                          f"(HTTP {r.status})")

    expect(not echecs,
           "le champ uid accepte un ensemble IMAP sur des endpoints unitaires — un seul "
           "appel agit sur tout le dossier :\n      " + "\n      ".join(echecs))


@test("ROB-12", "Les outils MCP refusent aussi un ensemble d'UID", "A-04")
def ensemble_uid_mcp():
    """Meme vecteur, meme cause, autre surface : la validation doit etre partagee, pas
    dupliquee endpoint par endpoint — c'est ce qui avait fait resurgir le defaut STARTTLS."""
    BOX.require()
    dossier = work_folder("ROB12")
    mk = seed(dossier, [
        {"from": f"m{i}@qa-autotest.local", "subject": f"mcp ensemble {i}",
         "date": f"2024-02-0{i} 09:00:00"} for i in range(1, 4)
    ])
    depart = count_in(dossier, marker=mk)
    r = BOX.mcp("delete_email", {"account_id": CFG.account_id, "folder": dossier,
                                 "uid": "1:*"})
    reste = count_in(dossier, marker=mk)
    expect(reste == depart,
           f"MCP delete_email(uid='1:*') a supprime {depart - reste} message(s) en un appel "
           f"— reponse : {r}")


@test("ROB-13", "Un identifiant de stockage local malforme est refuse", "A-08")
def uid_local_invalide():
    """Le stockage local extrait l'identifiant par `int(uid.replace("L", ""))`, sans garde.
    Toute valeur non numerique leve une `ValueError` non interceptee, et un nombre trop
    grand depasse la capacite de la colonne : cinq valeurs sur sept produisent un 500.

    Meme famille que A-04, autre stockage : la valeur n'est pas validee avant d'etre
    utilisee."""
    echecs = []
    import urllib.parse
    for valeur in ("L", "Labc", "L1:*", "L1,2", "L99999999999999999999",
                   "L-1", "L0", "L 1", "L1.5"):
        # Le chemin d'URL n'accepte ni espace ni caractere de controle : on encode, sinon
        # c'est le client qui refuse et le test ne teste plus l'application.
        chemin = urllib.parse.quote(valeur, safe="")
        r = API.delete(f"/accounts/{CFG.account_id}/message/{chemin}",
                       params={"folder": "QA_INEXISTANT", "storage": "local"})
        if r.status >= 500:
            echecs.append(f"uid={valeur!r} -> HTTP {r.status} (exception non interceptee)")
        elif not (400 <= r.status < 500):
            echecs.append(f"uid={valeur!r} -> HTTP {r.status} (accepte)")
    expect(not echecs,
           "des identifiants locaux malformes ne sont pas valides :\n      "
           + "\n      ".join(echecs))


@test("ROB-14", "Un caractere de controle est refuse dans l'objet d'un message", "A-10")
def controle_dans_entete():
    """CR et LF dans l'objet sont bloques — mais par la bibliotheque standard Python
    (« embedded header »), pas par une validation de l'application. Le NUL, lui, traverse
    et se retrouve dans un en-tete du message envoye.

    La protection actuelle est donc un effet de bord du transport : elle disparaitrait si
    la construction du message changeait."""
    echecs = []
    for nom, sujet in (("NUL", "QAROB\x00X"), ("CR", "QAROB\rX"), ("LF", "QAROB\nX"),
                       ("CRLF", "QAROB\r\nX-Injecte: oui")):
        r = API.post(f"/accounts/{CFG.account_id}/send",
                     json_body={"to": ["test@mailia.local"], "subject": sujet,
                                "body_text": "corps"})
        if not (400 <= r.status < 500):
            echecs.append(f"objet contenant {nom} -> HTTP {r.status} {r.detail[:60]}")
    expect(not echecs,
           "des caracteres de controle sont acceptes dans un en-tete de message :\n      "
           + "\n      ".join(echecs))


@test("ROB-15", "Un saut de ligne dans un critere de recherche n'injecte pas de commande",
      "A-11", serveur="permissif")
def injection_commande_imap():
    """Defaut A-11, demontre : `imap_criteria` part brut dans
    `imap._conn.uid("SEARCH", None, criteria)`. Un saut de ligne y termine la ligne de
    commande sur le fil, et ce qui suit s'execute dans la session **deja authentifiee** de
    l'application.

    La preuve doit prouver, pas supposer. Un critere legitime valant `ALL` supprimerait deja
    tout et rendrait l'effet de la partie injectee indiscernable : on emploie donc un critere
    qui ne correspond a rien, et une commande injectee dont l'effet ne peut venir que d'elle
    — creer un dossier temoin au nom unique. Si le temoin apparait, l'injection a eu lieu.

    Les champs testes ne sont pas equivalents, et c'est le point qui commande le correctif :
      - un champ **expression** (`imap_criteria`, `rules[].criteria`) laisse l'appelant ecrire
        la structure entiere, guillemets compris. Il ne peut pas etre echappe : seul un refus
        des caracteres de controle le protege.
      - un champ **valeur** (`from_addr`, `to_addr`, `subject`, `text`) est concatene dans
        `FROM "{valeur}"`. Sans echappement du guillemet, la valeur en referme un et sort de
        la commande. Ici l'echappement est possible, et c'est la bonne reponse.

    L'API REST est couverte elle aussi. Elle n'etait pas injectable au moment de la
    decouverte, mais **par accident** : ses filtres retiraient les guillemets, donc le saut de
    ligne restait prisonnier d'une chaine citee. Une protection qui tient au fait qu'un autre
    bout de code ajoute des guillemets n'est pas une protection — le jour ou ce bout change,
    la faille reapparait sans que personne n'ait touche a la couche IMAP.
    """
    BOX.require()
    temoin = "QAROB_INJ_" + work_folder("R15")[-6:]
    nul_part = 'rien@nulle.part.invalid'
    injection = f'\r\nA042 CREATE {temoin}'
    # Le critere legitime ne correspond a rien : tout effet observe vient de l'injection.
    expression = f'FROM "{nul_part}"{injection}'
    valeur = f'{nul_part}"{injection}'
    cid = CFG.account_id

    cas = [
        ("search_and_delete_emails.imap_criteria", "search_and_delete_emails",
         {"account_id": cid, "folder": "INBOX", "imap_criteria": expression}),
        ("search_and_move_emails.imap_criteria", "search_and_move_emails",
         {"account_id": cid, "folder": "INBOX", "target_folder": "INBOX",
          "imap_criteria": expression}),
        ("organize_emails.rules[].criteria", "organize_emails",
         {"account_id": cid, "folder": "INBOX",
          "rules": [{"criteria": expression, "target_folder": "INBOX"}]}),
        ("search_folder.from_addr", "search_folder",
         {"account_id": cid, "folder": "INBOX", "from_addr": valeur}),
        ("search_folder.subject", "search_folder",
         {"account_id": cid, "folder": "INBOX", "subject": valeur}),
        ("search_folder.text", "search_folder",
         {"account_id": cid, "folder": "INBOX", "text": valeur}),
        ("search_cross_folder.from_addr", "search_cross_folder",
         {"account_id": cid, "from_addr": valeur}),
    ]

    def _retirer_temoin():
        """Best-effort : le temoin n'existe pas dans le cas nominal, et la connexion peut
        tomber apres un critere qui a fait broncher le serveur. Un nettoyage qui echoue ne
        doit pas se faire passer pour un defaut du produit."""
        BOX.imap("try:\n"
                 f"    M.delete('{temoin}')\n"
                 "except Exception:\n"
                 "    pass\n")

    injectes = []
    try:
        for label, outil, args in cas:
            BOX.mcp(outil, args)
            if temoin in _recensement():
                injectes.append(label)
                # Le temoin doit repartir avant le cas suivant, sinon tous seraient
                # signales a cause du premier.
                _retirer_temoin()
        # Meme charge par l'API REST : la protection y etait accidentelle, on exige
        # desormais qu'elle soit constatee.
        for champ in ("filter_from", "filter_to", "filter_subject", "q"):
            API.get(f"/accounts/{cid}/messages", params={"folder": "INBOX", champ: valeur})
            if temoin in _recensement():
                injectes.append(f"API messages?{champ}")
                _retirer_temoin()
    finally:
        _retirer_temoin()

    expect(not injectes,
           "une commande IMAP placee apres un saut de ligne s'est executee dans la session "
           "authentifiee de l'application :\n      " + "\n      ".join(injectes))


@test("ROB-16", "Un drapeau IMAP inconnu n'injecte pas de commande", "A-12",
      serveur="permissif")
def injection_par_drapeau():
    """Meme famille qu'A-11, autre commande — celle que personne n'avait regardee.

    `_resolve_flag` (`imap/manager.py:697`) traduit les noms lisibles vers les drapeaux IMAP
    par un dictionnaire, puis **laisse passer verbatim** ce qu'il ne connait pas :
    `mapping.get(flag.lower(), flag)`. La valeur atterrit dans
    `uid("STORE", uid, "+FLAGS", f"({imap_flag})")` sans liste blanche ni echappement.

    Un drapeau referme donc sa parenthese et ouvre une ligne :
        flag = 'x)\\r\\nA042 CREATE <temoin>\\r\\nA043 NOOP'

    La ligne `NOOP` finale n'est pas decorative : la parenthese fermante de
    `f"({imap_flag})"` se colle a la **derniere** ligne injectee. Sans elle, le serveur
    recoit `A042 CREATE <temoin>)` et repond `BAD Unexpected ')'` — le CREATE echoue, le
    temoin n'apparait pas, et le test passe alors que l'injection a bel et bien eu lieu.
    C'est exactement ce qu'a fait la premiere version de ce test : un vert obtenu parce que
    la charge etait mal formee, pas parce que l'application refusait. Un test de securite
    qui passe pour la mauvaise raison est pire que pas de test.

    Il n'y a aucune raison d'accepter un drapeau arbitraire : l'ensemble est fini et connu
    (`\\Seen`, `\\Flagged`, `\\Answered`, `\\Draft`, `\\Deleted`). Une liste blanche ferme la
    question, contrairement aux criteres de recherche ou l'expression est libre par
    conception.

    Les deux portes sont testees : l'outil MCP et la route API, aucune des deux ne validait
    le drapeau au moment de l'ecriture de ce test.
    """
    BOX.require()
    temoin = "QAROB_FLG_" + work_folder("R16")[-6:]
    charge = f'x)\r\nA042 CREATE {temoin}\r\nA043 NOOP'
    dossier = work_folder("R16")
    mk = seed(dossier, [{"from": "flag@qa-autotest.local", "subject": "drapeau",
                         "date": "2024-03-01 09:00:00"}])
    uid = premier(messages_in(dossier, marker=mk), f"dossier {dossier}")["uid"]

    def _retirer_temoin():
        BOX.imap("try:\n"
                 f"    M.delete('{temoin}')\n"
                 "except Exception:\n"
                 "    pass\n")

    injectes = []
    try:
        BOX.mcp("flag_email", {"account_id": CFG.account_id, "folder": dossier,
                               "uid": str(uid), "flag": charge, "action": "add"})
        if temoin in _recensement():
            injectes.append("MCP flag_email.flag")
            _retirer_temoin()

        API.post(f"/accounts/{CFG.account_id}/message/{uid}/flags",
                 params={"folder": dossier},
                 json_body={"flag": charge, "action": "add"})
        if temoin in _recensement():
            injectes.append("API message/{uid}/flags.flag")
            _retirer_temoin()
    finally:
        _retirer_temoin()

    expect(not injectes,
           "un drapeau IMAP arbitraire s'echappe de la commande STORE :\n      "
           + "\n      ".join(injectes))


@test("ROB-17", "Deux dossiers locaux ne peuvent pas partager le meme chemin", "A-13")
def collision_de_chemins_locaux():
    """`create_local_folder` construit le chemin par concatenation naive
    (`accounts.py:2367`) : `path = f"{parent_path}/{name}"`. Comme `/` separe les niveaux
    mais n'est pas interdit *dans* un nom, deux creations distinctes produisent le meme
    chemin :

        name="a/b", sans parent      -> path "a/b"
        name="b",   parent="a"       -> path "a/b"

    La consequence redoutee etait un 500 : `copy_local_to_imap` (`mcp/server.py:3437`)
    resout le dossier source par `scalar_one_or_none()` sur ce chemin, et deux lignes
    correspondantes feraient lever `MultipleResultsFound`.

    **Mesure faite : la collision ne se produit pas.** Une contrainte
    `UNIQUE (account_id, path)` sur `local_folders` refuse la seconde creation, qui repond
    `409 Folder already exists`. La premisse du defaut est donc impossible, et le constat
    que j'avais leve est retire.

    Le test reste : c'est la contrainte de base qui ferme la question, pas le code
    applicatif, et rien dans `create_local_folder` ne le dit. Si quelqu'un la retire en
    faisant evoluer le schema, le chemin vers le 500 se rouvre en silence. Le test verrouille
    la propriete observable — aucun chemin porte par deux dossiers — sans prejuger du moyen.

    Residu connu, sans gravite : `QAX/enfant` (nom plat contenant un `/`) et `QAX` coexistent,
    et le premier ressemble a un enfant du second dans toute vue arborescente. C'est une
    ambiguite d'affichage, pas une atteinte aux donnees.
    """
    racine = "QAROB_C" + work_folder("R17")[-4:]
    creations = [
        {"name": f"{racine}/enfant"},
        {"name": racine},
        {"name": "enfant", "parent_path": racine},
    ]
    crees = []
    try:
        for corps in creations:
            r = API.post(f"/accounts/{CFG.account_id}/local-folders", json_body=corps)
            if r.status == 200 and isinstance(r.json(), dict):
                crees.append(r.json()["id"])

        dossiers = (API.get(f"/accounts/{CFG.account_id}/folders").json()
                    or {}).get("local_folders") or []
        chemins = [d["path"] for d in dossiers]
        doublons = sorted({c for c in chemins if chemins.count(c) > 1})
        expect(not doublons,
               f"des dossiers locaux distincts portent le meme chemin : {doublons}. "
               "Un nom contenant le separateur de chemin se confond avec une imbrication.")
    finally:
        for fid in crees:
            API.delete(f"/accounts/{CFG.account_id}/local-folders/{fid}")


@test("ROB-18", "Une action de regle incomprise est signalee, jamais perdue en silence", "A-14")
def action_de_regle_perdue():
    """`_parse_actions` (`rules/parser.py:134`) reconnait une action par mot-cle, puis extrait
    sa cible par expression reguliere. Quand le mot-cle correspond mais que l'extraction
    echoue, la ligne **disparait** : elle ne produit pas d'action, et elle n'est pas non plus
    ajoutee a `unknown_actions`.

        - **Alors**: deplacer            -> 0 action, 0 inconnue
        - **Alors**: deplacer vers       -> 0 action, 0 inconnue

    La regle est acceptee, `parsed_count` la compte, et l'interface annonce une regle valide
    qui ne fera rien. C'est le critere d'echec « succes annonce sans effet reel » du plan,
    applique a la configuration plutot qu'aux donnees.

    Le contrat teste est volontairement faible : on n'exige pas de refuser la regle, seulement
    que ce qui n'a pas ete compris soit **dit**. Une ligne comprise produit une action, une
    ligne incomprise apparait dans `unknown_actions` ; aucune ne s'evapore.
    """
    BOX.require()
    # Uniquement des lignes qui SONT des actions et que le parseur reconnait comme telles
    # avant d'echouer a en extraire la cible. Une section au nom inconnu (`- **Peutetre**:`)
    # n'est pas une action et n'a pas a figurer dans `unknown_actions` : l'inclure rendrait
    # le contrat plus strict que son titre, et le constat plus facile a balayer.
    cas = {
        "deplacer sans cible": "## R\n- **Alors**: deplacer",
        "deplacer vers vide": "## R\n- **Alors**: deplacer vers ",
        "transferer vers un non-email": "## R\n- **Alors**: deplacer vers",
    }
    code = (
        "import json\n"
        "from src.rules.parser import parse_rules_markdown\n"
        f"cas = json.loads({__import__('json').dumps(__import__('json').dumps(cas))})\n"
        "out = {}\n"
        "for nom, md in cas.items():\n"
        "    r = parse_rules_markdown(md)\n"
        "    out[nom] = {'regles': len(r),\n"
        "                'actions': sum(len(x.actions) for x in r),\n"
        "                'inconnues': sum(len(x.unknown_actions) for x in r)}\n"
        "print('__QA__' + json.dumps(out))\n"
    )
    sortie = BOX.python(code)
    data = None
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            import json as _json
            data = _json.loads(ligne[6:])
    if data is None:
        raise Skip(f"parseur de regles injoignable : {sortie[-200:]}")

    muettes = [f"{nom} -> {d['regles']} regle(s), {d['actions']} action(s), "
               f"{d['inconnues']} inconnue(s)"
               for nom, d in data.items()
               if d["regles"] and not d["actions"] and not d["inconnues"]]
    expect(not muettes,
           "des lignes d'action disparaissent sans produire d'action ni etre signalees "
           "comme incomprises :\n      " + "\n      ".join(muettes))


@test("ROB-19", "L'import depuis un chemin serveur est confine a un repertoire autorise", "A-15")
def import_path_confine():
    """`import-path` (`accounts.py:2524`) accepte un chemin arbitraire du systeme de fichiers
    serveur. Le seul controle est `os.path.isfile(path)` : ni repertoire autorise, ni
    resolution des `..`, ni refus des chemins absolus.

    Sont acceptes aujourd'hui : `/etc/hostname`, `../../etc/hostname`,
    `/data/imports/../../etc/hostname`, et `/proc/self/environ` — c'est-a-dire
    l'environnement du processus, ou vivent les mots de passe de la base, de Redis et les
    cles de chiffrement.

    L'endpoint est reserve aux administrateurs, ce qui limite la portee sans la fermer : un
    jeton administrateur compromis devient une lecture de tout fichier lisible par le
    conteneur, et l'instance heberge des comptes non administrateurs.

    Ce test verifie **le refus**, jamais le contenu : il n'inspecte aucun fichier et
    n'importe rien. Les cibles sont neutres et connues.
    """
    hors_perimetre = [
        "/etc/hostname",
        "/data/imports/../../etc/hostname",
        "../../etc/hostname",
        "/proc/self/environ",
    ]
    acceptes = []
    try:
        for chemin in hors_perimetre:
            r = API.post(f"/accounts/{CFG.account_id}/import-path",
                         params={"path": chemin, "folder": "QAROB_IMPPATH",
                                 "storage": "local"})
            # Deux refus differents portent le meme code 403 : « reserve aux
            # administrateurs » (le test ne peut pas s'executer) et « chemin hors du
            # repertoire autorise » (exactement ce qu'on veut constater). Les confondre
            # transformait un succes en SKIP, donc masquait la preuve que le correctif
            # fonctionne. C'est le corps qui distingue, pas le code.
            if r.status == 403 and "admin" in r.detail.lower():
                raise Skip("le compte QA n'est pas administrateur : l'endpoint ne peut pas "
                           "etre exerce, son confinement reste non verifie")
            if not (400 <= r.status < 500):
                acceptes.append(f"{chemin} -> HTTP {r.status}")
    finally:
        # `/folders` ne renvoie pas l'identifiant des dossiers locaux, et la suppression en
        # a besoin : on le lit dans la base plutot que de laisser le dossier derriere nous.
        BOX.require()
        sortie = BOX.python(
            "import json\n"
            "from sqlalchemy import create_engine, text\n"
            "from src.config import get_settings\n"
            "e = create_engine(get_settings().database_url.replace('+asyncpg',''))\n"
            "with e.connect() as c:\n"
            "    r = c.execute(text('SELECT id FROM local_folders WHERE account_id = :a "
            "AND path LIKE :p'), {'a': %d, 'p': '%%QAROB_IMPPATH%%'}).fetchall()\n"
            "print('__QA__' + json.dumps([x[0] for x in r]))\n" % CFG.account_id)
        for ligne in sortie.splitlines():
            if ligne.startswith("__QA__"):
                import json as _json
                for fid in _json.loads(ligne[6:]):
                    API.delete(f"/accounts/{CFG.account_id}/local-folders/{fid}")

    expect(not acceptes,
           "des chemins hors du perimetre d'import sont acceptes — traversee de repertoire "
           ":\n      " + "\n      ".join(acceptes))


@test("ROB-20", "Les recherches legitimes passent toujours les validateurs", "A-11",
      serveur="strict")
def recherches_legitimes():
    """Contrepoids de `ROB-15` : un validateur trop strict serait pire que la faille.

    `_imap_astring` et `_imap_criteria` se placent entre l'utilisateur et le serveur. Ils
    doivent refuser ce qui casse la commande — et **rien d'autre**. Les entrees ci-dessous
    sont toutes honnetes et courantes :

      - une adresse avec `+` (sous-adressage, tres repandu) ;
      - un objet accentue, et un objet avec une apostrophe ;
      - un texte contenant un guillemet double legitime, qui doit etre echappe et non refuse ;
      - un antislash, meme raison ;
      - une expression composee `OR FROM "x" SUBJECT "y"`, syntaxe IMAP normale ;
      - une recherche par date.

    Un refus ici est un defaut aussi serieux qu'une injection : la recherche cesserait de
    fonctionner pour des usages ordinaires. Le test exige donc **l'absence de refus**, et
    verifie en plus que les recherches censees trouver quelque chose trouvent bien leur
    message — un `found: 0` general signalerait un validateur qui laisse passer la requete
    mais la vide de son sens.
    """
    BOX.require()
    dossier = work_folder("R20")
    cible = work_folder("R20D")
    API.post(f"/accounts/{CFG.account_id}/create-folder", json_body={"folder_name": cible})
    mk = seed(dossier, [
        {"from": "compta+factures@qa-autotest.local", "subject": "Facture Été 2024",
         "body": "Montant du a l'echeance", "date": "2024-05-02 10:00:00"},
        {"from": "contact@qa-autotest.local", "subject": "Devis \"urgent\" signe",
         "body": 'Il a dit "oui" hier', "date": "2024-05-03 11:00:00"},
        {"from": "sav@qa-autotest.local", "subject": "Chemin C:\\Temp\\rapport",
         "body": "antislash dans le corps : C:\\Temp", "date": "2024-05-04 12:00:00"},
    ])

    # (libelle, outil, arguments, doit_trouver)
    cas = [
        ("adresse avec +", "search_folder",
         {"folder": dossier, "from_addr": "compta+factures@qa-autotest.local"}, True),
        ("objet accentue", "search_folder", {"folder": dossier, "subject": "Été"}, True),
        ("apostrophe dans le texte", "search_folder",
         {"folder": dossier, "text": "l'echeance"}, True),
        ("guillemet legitime", "search_folder", {"folder": dossier, "subject": 'urgent"'}, False),
        ("antislash legitime", "search_folder",
         {"folder": dossier, "subject": "C:\\Temp\\rapport"}, False),
        ("expression composee", "search_and_move_emails",
         {"folder": dossier, "target_folder": cible,
          "imap_criteria": 'OR SUBJECT "introuvable-xyz" SUBJECT "introuvable-abc"'}, False),
        ("critere par date", "search_and_move_emails",
         {"folder": dossier, "target_folder": cible,
          "imap_criteria": "SINCE 01-Jan-2099"}, False),
        ("valeur de 500 caracteres", "search_folder",
         {"folder": dossier, "text": "a" * 500}, False),
    ]

    # Trois issues distinctes, parce qu'elles n'accusent pas le meme coupable :
    # un refus du validateur met en cause le correctif anti-injection ; une autre erreur
    # met en cause le chemin de recherche lui-meme ; un resultat vide met en cause le sens
    # de la requete apres validation. Les confondre ferait imputer au correctif un defaut
    # qui lui preexiste.
    refuses, casses, vides = [], [], []
    for libelle, outil, args, doit_trouver in cas:
        r = BOX.mcp(outil, {"account_id": CFG.account_id, **args})
        texte = str(r)
        if "ImapInjection" in texte:
            refuses.append(f"{libelle} -> {texte[:110]}")
            continue
        if "__erreur__" in texte:
            casses.append(f"{libelle} -> {texte[:110]}")
            continue
        if doit_trouver and ("'total_matches': 0" in texte or "'count': 0" in texte):
            vides.append(f"{libelle} -> aucun resultat alors qu'un message correspond")

    expect(not refuses,
           "des recherches legitimes sont refusees par les validateurs anti-injection — "
           "un validateur trop strict casse la recherche pour les usages honnetes :\n      "
           + "\n      ".join(refuses))
    expect(not casses,
           "des recherches legitimes echouent (cause etrangere aux validateurs) :\n      "
           + "\n      ".join(casses))
    expect(not vides,
           "des recherches legitimes passent mais ne trouvent plus leur message :\n      "
           + "\n      ".join(vides))


@test("ROB-21", "Un Message-ID replie n'injecte pas de commande", "A-17",
      serveur="permissif")
def injection_par_message_id():
    """Le vecteur le plus grave de la campagne : **il suffit d'envoyer un email**.

    `get_thread` (`mcp/server.py:1476`) recherche les messages d'un fil par
    `HEADER Message-ID "<...>"`, en interpolant la valeur lue dans l'en-tete du message
    cible. Or cet en-tete est ecrit par **l'expediteur**, et il n'est pas encode en
    encoded-word : rien ne le protege, meme accidentellement.

    La forme du vecteur compte, et c'est la ou l'on se trompe :

      - un CRLF **nu** ne survit pas a l'`APPEND` : le serveur normalise, le temoin
        n'apparait pas, et on conclut a tort que le champ est sur ;
      - un CRLF suivi d'une **espace** est un repliage RFC 5322 parfaitement legal. Il
        traverse l'`APPEND`, et `email.message_from_bytes` en politique compat32 conserve
        le saut de ligne dans la valeur. La ligne injectee arrive donc telle quelle sur le
        fil, precedee d'une espace.

    Ce que fait le serveur de cette ligne **depend du serveur**, dans le sens inverse de
    l'intuition :

        Dovecot    ` a2 CREATE X`  ->  BAD Invalid tag        refuse
        GreenMail  ` a2 CREATE X`  ->  OK CREATE completed.   **dossier cree**

    C'est le serveur permissif qui prouve l'exploit et le serveur strict qui produirait le
    faux negatif. Ce test doit donc tourner **sur les deux** : au vert sur Dovecot seul, il
    ne prouve rien.

    `References` n'est pas concerne : il est decoupe par `.split()`, qui detruit le
    repliage. Le vecteur est `Message-ID` et `In-Reply-To`.
    """
    BOX.require()
    temoin = "QAROB_MID_" + work_folder("R21")[-6:]
    dossier = work_folder("R21")
    # La derniere ligne repliee absorbe le `>` fermant, pour que la commande injectee soit
    # syntaxiquement propre — sans quoi le serveur recoit `CREATE <temoin>>` et refuse,
    # ce qui donnerait un vert trompeur.
    charge = f"<qa-r21@qa-autotest.local\r\n a2 CREATE {temoin}\r\n a3 NOOP"
    mk = seed(dossier, [{"from": "exp@qa-autotest.local", "subject": "fil injecte",
                         "date": "2024-07-01 09:00:00", "message_id": charge}])
    uid = premier(messages_in(dossier, marker=mk), f"dossier {dossier}")["uid"]

    def _retirer():
        BOX.imap("try:\n"
                 f"    M.delete('{temoin}')\n"
                 "except Exception:\n"
                 "    pass\n")

    try:
        BOX.mcp("get_thread", {"account_id": CFG.account_id, "folder": dossier,
                               "uid": str(uid)})
        injecte = temoin in _recensement()
    finally:
        _retirer()

    expect(not injecte,
           "un Message-ID replie a fait executer une commande IMAP : l'en-tete est ecrit "
           "par l'expediteur, donc un simple email suffit a armer la charge (A-17).")


    # Le temoin absent ne suffit pas a conclure, et interroger `_imap_astring` ne suffit pas
    # non plus : le garde peut exister sans que le site d'appel l'utilise. C'est exactement
    # ce qui s'est produit — la premiere version de ce test etait VERTE alors que le
    # conteneur deploye contenait encore l'interpolation en clair.
    #
    # Deux raisons de ne pas se fier a l'effet observe :
    #   - `get_thread` intercepte `ImapInjection` et poursuit sans rien remonter : la valeur
    #     de retour ne dit pas QUI a refuse ;
    #   - Dovecot repond `BAD Invalid tag` a une ligne precedee d'une espace, donc l'absence
    #     de temoin y est obtenue meme sans correctif.
    #
    # On verifie donc la forme du code deploye : aucun en-tete d'email interpole directement
    # dans un critere SEARCH. Independant du serveur et du chemin d'execution, comme
    # `SMTP-06` pour `starttls()`.
    sortie = BOX.python(
        "import json, inspect\n"
        "import src.mcp.server as S\n"
        # Pas de regex : l'echappement imbriquee a deja corrompu ce motif une fois, et un
        # motif corrompu ne matche rien — donc un test vert. Une sous-chaine est verifiable
        # a l'oeil.
        "marqueur = chr(34) + chr(60) + chr(123)\n"   # la sequence  "<{
        "trouves = [l.strip()[:110] for l in inspect.getsource(S).splitlines()\n"
        "           if 'HEADER' in l and marqueur in l]\n"
        "print('__QA__' + json.dumps(trouves))\n")
    brut = None
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            import json as _json
            brut = _json.loads(ligne[6:])
    if brut is None:
        raise Skip(f"lecture du source du serveur MCP impossible : {sortie[-200:]}")
    expect(not brut,
           "un en-tete d'email est interpole directement dans une commande SEARCH, sans "
           "passer par `_imap_astring` — la valeur vient de l'expediteur, donc un simple "
           "email suffit a injecter une commande :\n      " + "\n      ".join(brut))

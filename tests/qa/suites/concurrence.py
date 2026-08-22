"""Concurrence — Plan E.

Pourquoi ce groupe est different des autres
-------------------------------------------
Tout le reste de la suite mesure par recensement avant/apres. **La concurrence invalide ce
motif** : quand deux operations se chevauchent, il n'existe pas d'instant « avant » ni
« apres » commun. Un recensement pris entre les deux ne decrit aucun etat coherent, et un
test bati dessus mesure du bruit.

Les quatre regles qui le remplacent sont dans `qa/course.py`. La plus importante, et la plus
facile a oublier : **prouver que la course a eu lieu**. Deux requetes lancees a la suite dans
une boucle ne se chevauchent pas forcement, et un test de concurrence qui ne fait que de la
sequence **passe toujours** — il passera encore le jour ou le defaut apparaitra. C'est un
faux negatif permanent, et c'est pire qu'une sonde ratee, qui finit par se voir.

Chaque test ci-dessous rend donc `Skip` s'il n'a pas obtenu de chevauchement, et annonce le
nombre de courses reellement simultanees quand il passe. « Vert sur 12 courses dont 12 avec
chevauchement prouve » n'est pas la meme information que « vert ».
"""
from __future__ import annotations

import json
import uuid

from ..core import (API, BOX, CFG, Skip, count_in, expect, messages_in, noter,
                    premier, seed, test, work_folder)
from ..course import lancer

# Nombre de courses par scenario. Un defaut de concurrence ne se manifeste pas a tous les
# coups ; une course unique ne prouve rien.
COURSES = 12


def _uid_unique(dossier: str, marqueur: str) -> str:
    return premier(messages_in(dossier, marker=marqueur), f"dossier {dossier}")["uid"]


def _bilan(simultanees: int, tentees: int) -> str:
    """Ce qu'un vert signifie reellement, ecrit dans le resultat."""
    return f"{simultanees} course(s) simultanee(s) prouvee(s) sur {tentees} tentee(s)"


@test("CONC-01", "Deux deplacements simultanes du meme email n'en font pas deux", "E-01")
def deux_deplacements_du_meme_email():
    """Scenario de reference : deux clients deplacent le meme message au meme instant, vers
    deux dossiers differents.

    L'invariant ne suppose aucun instant : **le message existe exactement une fois** a la
    fin. Deux exemplaires, c'est une duplication ; zero, c'est une perte. C'est la
    generalisation de l'invariant qui a trouve A-18 — porte sur une identite plutot que sur
    un comptage.

    On lit aussi les deux reponses : exactement une doit reussir. Deux succes annoncent une
    duplication meme si l'etat final semble correct, ce qui signalerait un defaut latent que
    le comptage seul manquerait.
    """
    source = work_folder("CONC01")
    cible_a = work_folder("CONC01A")
    cible_b = work_folder("CONC01B")
    for c in (cible_a, cible_b):
        API.post(f"/accounts/{CFG.account_id}/create-folder", json_body={"folder_name": c})

    simultanees, anomalies = 0, []
    for tour in range(COURSES):
        marque = seed(source, [{"from": "conc@qa-autotest.local",
                                "subject": f"course {tour}",
                                "date": "2024-08-01 10:00:00"}])
        uid = _uid_unique(source, marque)
        API.post(f"/accounts/{CFG.account_id}/empty-folder", json_body={"folder_name": cible_a})
        API.post(f"/accounts/{CFG.account_id}/empty-folder", json_body={"folder_name": cible_b})

        def deplacer(cible):
            return API.post(f"/accounts/{CFG.account_id}/message/{uid}/move",
                            params={"folder": source}, json_body={"target_folder": cible})

        course = lancer(("vers_a", lambda: deplacer(cible_a)),
                        ("vers_b", lambda: deplacer(cible_b)))
        if not course.simultanee:
            continue
        simultanees += 1

        total = (count_in(source, marker=marque)
                 + count_in(cible_a, marker=marque)
                 + count_in(cible_b, marker=marque))
        succes = sum(1 for t in course.tirs
                     if t.resultat is not None and t.resultat.status == 200)
        if total != 1:
            anomalies.append(
                f"course {tour} : le message existe {total} fois "
                f"({'DUPLICATION' if total > 1 else 'PERTE'}) — {course}")
        elif succes != 1:
            anomalies.append(
                f"course {tour} : {succes} deplacement(s) annonces reussis pour un seul "
                f"message — un client croit avoir deplace ce qu'il n'a pas deplace ({course})")

    if simultanees == 0:
        raise Skip("aucune des courses ne s'est chevauchee : ce test n'a rien constate sur "
                   "la concurrence")
    noter(_bilan(simultanees, COURSES))
    expect(not anomalies, "\n      ".join(anomalies[:5]))


@test("CONC-02", "Supprimer pendant une lecture ne rend pas un contenu tronque", "E-02")
def suppression_pendant_lecture():
    """Une suppression et une lecture du meme message, lancees ensemble.

    Les deux issues correctes sont : la lecture aboutit avec le contenu complet, ou elle
    echoue proprement parce que le message n'est plus la. **L'issue interdite est une
    reussite partielle** — un 200 avec un corps vide ou tronque, qui ferait croire a un
    message vide plutot qu'a un message supprime.
    """
    dossier = work_folder("CONC02")
    corps_attendu = "corps complet de reference pour la course"
    simultanees, anomalies = 0, []

    for tour in range(COURSES):
        marque = seed(dossier, [{"from": "conc2@qa-autotest.local",
                                 "subject": f"lecture {tour}",
                                 "body": corps_attendu,
                                 "date": "2024-08-02 10:00:00"}])
        uid = _uid_unique(dossier, marque)

        course = lancer(
            ("lire", lambda: API.get(f"/accounts/{CFG.account_id}/message/{uid}",
                                     params={"folder": dossier})),
            ("supprimer", lambda: API.delete(f"/accounts/{CFG.account_id}/message/{uid}",
                                             params={"folder": dossier})))
        if not course.simultanee:
            continue
        simultanees += 1

        lecture = course.tirs[0].resultat
        if lecture is None:
            anomalies.append(f"course {tour} : la lecture a leve {course.tirs[0].erreur}")
        elif lecture.status == 200:
            donnees = lecture.json() or {}
            texte = str(donnees.get("body_text") or donnees.get("body") or "")
            if corps_attendu not in texte:
                anomalies.append(
                    f"course {tour} : lecture 200 mais corps incomplet — un message "
                    f"supprime pendant sa lecture est rendu comme un message vide "
                    f"({len(texte)} caracteres) ({course})")
        elif lecture.status >= 500:
            anomalies.append(f"course {tour} : la lecture rend {lecture.status} — "
                             f"{lecture.detail[:80]} ({course})")

    if simultanees == 0:
        raise Skip("aucune des courses ne s'est chevauchee : rien constate sur la concurrence")
    noter(_bilan(simultanees, COURSES))
    expect(not anomalies, "\n      ".join(anomalies[:5]))


@test("CONC-03", "Creer le meme dossier deux fois n'en cree qu'un", "E-03")
def creation_simultanee_du_meme_dossier():
    """Deux creations du meme nom au meme instant.

    L'invariant est structurel : **un seul dossier porte ce nom a la fin**. Les deux
    reponses peuvent legitimement etre des succes — `create_folder` traite « already
    exists » comme un succes idempotent, ce qui est le bon comportement. Ce qui serait
    fautif, c'est un doublon dans l'arborescence, ou une erreur 500 sur la course perdante.
    """
    simultanees, anomalies = 0, []
    for tour in range(COURSES):
        nom = f"QA_CONC03_{uuid.uuid4().hex[:8]}"

        def creer():
            return API.post(f"/accounts/{CFG.account_id}/create-folder",
                            json_body={"folder_name": nom})

        course = lancer(("client_a", creer), ("client_b", creer))
        if course.simultanee:
            simultanees += 1
            for t in course.tirs:
                if t.resultat is not None and t.resultat.status >= 500:
                    anomalies.append(f"course {tour} : {t.nom} rend {t.resultat.status} — "
                                     f"{t.resultat.detail[:70]} ({course})")
            dossiers = _noms_de_dossiers()
            if dossiers.count(nom) > 1:
                anomalies.append(f"course {tour} : {dossiers.count(nom)} dossiers portent "
                                 f"le nom {nom} ({course})")
        API.post(f"/accounts/{CFG.account_id}/delete-folder", json_body={"folder_name": nom})

    if simultanees == 0:
        raise Skip("aucune des courses ne s'est chevauchee : rien constate sur la concurrence")
    noter(_bilan(simultanees, COURSES))
    expect(not anomalies, "\n      ".join(anomalies[:5]))


def _noms_de_dossiers() -> list[str]:
    def parcourir(noeuds):
        for n in noeuds:
            yield n["name"]
            yield from parcourir(n.get("children") or [])
    donnees = API.get(f"/accounts/{CFG.account_id}/folders").json() or {}
    return list(parcourir(donnees.get("folders") or []))


@test("CONC-04", "Un deplacement qui ne deplace rien n'annonce pas un succes", "E-03")
def move_sans_effet_annonce_un_succes():
    """Trouve en cherchant autre chose, et c'est le defaut qui rend les deux precedents
    invisibles cote client.

    `move` sur un UID qui n'existe pas repond `200 {"status": "moved"}` :

        POST /message/999999/move?folder=QAE_SRC  ->  200 {"status":"moved"}
        contenu du dossier cible                  ->  0 message

    Meme famille qu'IT4-01 et qu'A-18 : **un succes annonce sans effet reel.** Ici la
    consequence depasse le confort d'usage — dans une course, les deux clients recoivent
    « moved » alors qu'un seul a deplace quelque chose. Le second croit avoir agi, et rien
    dans la reponse ne le detrompe.

    Le test n'a pas besoin de concurrence : il constate la propriete directement, ce qui
    est plus fiable et plus rapide.
    """
    dossier = work_folder("CONC04")
    cible = work_folder("CONC04D")
    for f in (dossier, cible):
        API.post(f"/accounts/{CFG.account_id}/create-folder", json_body={"folder_name": f})
    API.post(f"/accounts/{CFG.account_id}/empty-folder", json_body={"folder_name": cible})

    r = API.post(f"/accounts/{CFG.account_id}/message/999999/move",
                 params={"folder": dossier}, json_body={"target_folder": cible})
    arrives = count_in(cible)

    expect(not (r.status == 200 and arrives == 0),
           f"deplacer l'UID inexistant 999999 rend HTTP {r.status} {r.detail[:60]} alors "
           f"que le dossier cible contient {arrives} message : un client ne peut pas "
           "distinguer un deplacement reussi d'un deplacement qui n'a rien fait.")


# Le scenario « deux clients sur le meme brouillon » a ete retire apres mesure.
#
# Il echouait 12 fois sur 12 — deux brouillons pour un enregistrement logique — et j'allais
# le rapporter comme un defaut de concurrence. Le temoin sequentiel l'a refute : **deux
# appels successifs a `save-draft` produisent aussi deux brouillons.** L'endpoint n'a pas
# d'identifiant de brouillon, chaque appel fait un APPEND ; la concurrence n'y est pour rien.
#
# C'etait une propriete de l'endpoint prise pour un defaut de course. Sans temoin sequentiel,
# j'aurais impute a la concurrence un comportement qui lui prexiste — l'erreur inverse de
# celle commise sur A-18, ou j'avais impute a mon test un defaut du produit.
#
# Un test de concurrence sur les brouillons demanderait un `update-draft` identifie, qui
# n'existe pas cote API.


@test("CONC-05", "Deux suppressions simultanees du meme email n'en dupliquent pas", "E-05")
def deux_suppressions_du_meme_email():
    """Deux clients suppriment le meme message. La suppression deplace vers la corbeille :
    le risque est donc **deux exemplaires dans la corbeille** pour un seul message d'origine.

    C'est la variante la plus proche de A-18 et de `F-02`, jouee en concurrence.
    """
    dossier = work_folder("CONC05")
    simultanees, anomalies = 0, []

    for tour in range(COURSES):
        marque = seed(dossier, [{"from": "conc5@qa-autotest.local",
                                 "subject": f"double suppression {tour}",
                                 "date": "2024-08-05 10:00:00"}])
        uid = _uid_unique(dossier, marque)

        def supprimer():
            return API.delete(f"/accounts/{CFG.account_id}/message/{uid}",
                              params={"folder": dossier})

        course = lancer(("client_a", supprimer), ("client_b", supprimer))
        if not course.simultanee:
            continue
        simultanees += 1

        total = count_in(dossier, marker=marque) + count_in("Trash", marker=marque)
        if total > 1:
            anomalies.append(f"course {tour} : DUPLICATION, le message existe {total} fois "
                             f"apres deux suppressions simultanees ({course})")
        elif total == 0:
            anomalies.append(f"course {tour} : le message a disparu sans passer par la "
                             f"corbeille ({course})")
        API.post(f"/accounts/{CFG.account_id}/empty-folder", json_body={"folder_name": "Trash"})

    if simultanees == 0:
        raise Skip("aucune des courses ne s'est chevauchee : rien constate sur la concurrence")
    noter(_bilan(simultanees, COURSES))
    expect(not anomalies, "\n      ".join(anomalies[:5]))


@test("CONC-06", "Aucune connexion IMAP n'est mise en cache ou partagee", "E-06")
def pas_de_pool_de_connexions():
    """Garde structurel. Il ne surveille pas un symptome mais **la cause unique** qui
    rouvrirait deux risques distincts.

    Ce que la mesure a etabli, en quatre modeles successifs dont trois faux :

      - la fenetre de duplication vaut **moins d'une milliseconde** — la duree de la commande
        `MOVE` elle-meme. En dessous de 0,5 ms la duplication est **deterministe** (5/5 sur
        Dovecot) ; au-dela de 1 ms elle ne se produit jamais ;
      - les envois HTTP partent pourtant a **0,06 - 0,24 ms** les uns des autres, soit plus
        serre que la fenetre, et ne dupliquent jamais (0 sur 16 courses, deux serveurs) ;
      - ce n'est donc **pas l'espacement des requetes** qui protege, mais la **variance du
        preambule** que chaque requete execute avant d'atteindre le `MOVE` : ouverture de
        connexion IMAP, `LOGIN`, `SELECT`.

    Mesure de cette variance, barriere placee **avant** la connexion (elle demande un banc
    interne, elle n'est pas observable depuis l'API) :

        clients   preambule median   ecart-type   ecart MIN entre MOVE voisins
             6            12 ms         5,0 ms                  2,06 - 2,43 ms
            12            20 ms         9,0 ms                  1,86 - 2,01 ms
            24            35 ms        18,1 ms                  0,93 - 1,70 ms
            48            65 ms        35,1 ms                  1,00 - 1,28 ms

    **La marge reelle est un facteur 2 a 3, pas un ordre de grandeur.** A 24 clients l'ecart
    minimal touche 0,93 ms, le bord exact de la fenetre.

    Et la protection est **auto-regulee** : plus il y a de clients, plus le preambule
    ralentit, ce qui re-etale les requetes. L'ecart minimal ne s'effondre pas sous la charge,
    il plafonne vers la milliseconde. **Ce n'est pas une marge de securite, c'est un
    equilibre — et personne ne l'a concu.**

    **Un pool de connexions supprimerait exactement cette variance** et transformerait le
    chemin API en banc protocolaire : connexions etablies, dossier deja selectionne, `MOVE`
    immediat. La fenetre deviendrait atteignable — et il n'y a que 2 a 3 fois de marge avant
    d'y etre.

    Et c'est **la meme cause** que le risque `_selection` d'A-20 : la reutilisation de la
    selection de dossier n'est sure aujourd'hui que parce que rien ne partage
    d'`IMAPManager`. Un pool rouvre les deux d'un coup.

    **Reserve : cette protection ne couvre pas le chemin worker.** La boucle de
    synchronisation ne paie pas le preambule a chaque operation — elle garde son
    `IMAPManager` et son dossier selectionne pendant tout un lot. Elle ne beneficie donc pas
    de la variance qui protege le chemin API, et rien ne garantit l'espacement de ses `MOVE`
    vis-a-vis d'une action utilisateur simultanee.

    Non mesurable tant que le worker est arrete : consigne comme **non couvert**, pas comme
    ferme. Ce test ne dit donc rien de ce chemin-la.

    D'ou un test de forme plutot que de comportement. Un test protocolaire serait rouge en
    permanence, testerait le serveur plutot que MailIA, et doublonnerait le banc du
    correcteur. Celui-ci ne depend ni du serveur ni du calendrier — meme technique que
    `SMTP-06` pour `starttls()` et `SEC-G09` pour `extractall`.
    """
    BOX.require()
    # Analyse par AST plutot que ligne a ligne : un decorateur `@lru_cache` vit sur une
    # autre ligne que la construction qu'il met en cache, et une recherche textuelle le
    # manque. C'est le cas que la premiere version de ce test laissait passer.
    sortie = BOX.python(
        "import ast, inspect, json\n"
        "import src.imap.manager as M, src.mcp.context as C\n"
        "import src.api.routes.accounts as A\n"
        "CACHANTS = ('cache', 'lru_cache', 'cached', 'cached_property', 'memoize')\n"
        "STRUCTURES = ('cache', 'pool', 'managers', 'connexions', 'connections', 'registry')\n"
        "suspects = []\n"
        "def construit_manager(noeud):\n"
        "    return any(isinstance(n, ast.Call) and getattr(n.func, 'id', '') == 'IMAPManager'\n"
        "               for n in ast.walk(noeud))\n"
        "for nom, mod in (('imap/manager', M), ('mcp/context', C), ('api/accounts', A)):\n"
        "    src = inspect.getsource(mod)\n"
        "    arbre = ast.parse(src)\n"
        "    for n in ast.walk(arbre):\n"
        "        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and construit_manager(n):\n"
        "            for d in n.decorator_list:\n"
        "                nd = getattr(d, 'id', None) or getattr(d, 'attr', None) \\\n"
        "                     or getattr(getattr(d, 'func', None), 'attr', None) \\\n"
        "                     or getattr(getattr(d, 'func', None), 'id', None) or ''\n"
        "                if any(c in nd.lower() for c in CACHANTS):\n"
        "                    suspects.append('%s:%d  @%s sur %s()' % (nom, n.lineno, nd, n.name))\n"
        # Un manager range dans une structure qui survit a l'appel : affectation dont la
        # cible est un abonnement, un attribut, ou un nom evoquant un stockage partage.
        "        if isinstance(n, (ast.Assign, ast.AugAssign)) and construit_manager(n):\n"
        "            cibles = n.targets if isinstance(n, ast.Assign) else [n.target]\n"
        "            for c in cibles:\n"
        "                texte = ast.unparse(c).lower()\n"
        "                if isinstance(c, (ast.Subscript, ast.Attribute)) or \\\n"
        "                        any(m in texte for m in STRUCTURES):\n"
        "                    suspects.append('%s:%d  %s = IMAPManager(...)' % (nom, n.lineno, ast.unparse(c)[:60]))\n"
        # Un appel a .append()/.add() sur une structure, avec un manager en argument.
        "        if isinstance(n, ast.Call) and construit_manager(n):\n"
        "            f = n.func\n"
        "            if isinstance(f, ast.Attribute) and f.attr in ('append', 'add', 'put', 'setdefault'):\n"
        "                suspects.append('%s:%d  %s(...IMAPManager...)' % (nom, n.lineno, ast.unparse(f)[:60]))\n"
        "print('__QA__' + json.dumps(sorted(set(suspects))))\n")
    suspects = None
    for ligne in sortie.splitlines():
        if ligne.startswith("__QA__"):
            suspects = json.loads(ligne[6:])
    if suspects is None:
        raise Skip(f"lecture du source impossible : {sortie[-200:]}")
    expect(not suspects,
           "un `IMAPManager` semble mis en cache ou partage. Cela supprimerait la variance "
           "du preambule qui rend aujourd'hui la fenetre de duplication (<1 ms) "
           "inatteignable par l'API, et rouvrirait en meme temps la course sur `_selection` "
           "(A-20) :\n      " + "\n      ".join(suspects))

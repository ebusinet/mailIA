"""Cloisonnement entre utilisateurs — la suite la plus importante.

Historique : F-00 (itération 1) — sept endpoints de l'API retournaient AVANT le controle de
propriete des le parametre `storage=local`, exposant en lecture et en ecriture les emails
locaux de n'importe quel utilisateur. N-04 (itération 2) — le meme defaut subsistait dans
l'outil MCP `move_local_email`.

Principe des sondes : on vise un identifiant de compte que le jeton QA ne possede pas, avec
des ressources volontairement inexistantes. Si le controle de propriete fonctionne, la
reponse est « compte introuvable ». S'il est absent, la requete atteint la couche donnees et
renvoie une autre erreur — ce qui suffit a le detecter sans qu'aucune donnee reelle ne soit
touchee.
"""
from __future__ import annotations

from ..core import API, BOX, CFG, Skip, expect, test

# Identifiant volontairement hors de portee du jeton QA.
FOREIGN = 99999

# (methode, chemin, parametres, corps) — les 7 endpoints de F-00 plus delete-bulk.
API_PROBES = [
    ("GET", "/accounts/{a}/messages", {"folder": "INBOX"}, None, "list_messages (imap)"),
    ("GET", "/accounts/{a}/messages", {"folder": "X", "storage": "local"}, None, "list_messages (local)"),
    ("GET", "/accounts/{a}/message/L1", {"folder": "X", "storage": "local"}, None, "get_message (local)"),
    ("GET", "/accounts/{a}/message/1/attachment/0", {"folder": "INBOX"}, None, "download_attachment"),
    ("POST", "/accounts/{a}/message/L1/flags", {"folder": "X", "storage": "local"},
     {"flag": "seen", "action": "add"}, "update_flags (local)"),
    ("POST", "/accounts/{a}/message/L1/move",
     {"folder": "X", "storage": "local", "target_storage": "local"},
     {"target_folder": "X"}, "move_message (local)"),
    ("DELETE", "/accounts/{a}/message/L1", {"folder": "X", "storage": "local"}, None, "delete_message (local)"),
    ("POST", "/accounts/{a}/delete-bulk", {"storage": "local"},
     {"uids": ["L1"], "folder": "X"}, "delete_bulk (local)"),
]


@test("ISO-01", "Les 8 endpoints API refusent un compte non possede", "F-00")
def api_endpoints_refusent_compte_etranger():
    echecs = []
    for method, path, params, body, label in API_PROBES:
        r = API.request(method, path.format(a=FOREIGN), params=params, json_body=body)
        if r.status != 404 or "Account not found" not in r.detail:
            echecs.append(f"{label} -> {r.status} {r.detail[:80]}")
    expect(not echecs,
           "controle de propriete absent ou incomplet :\n      " + "\n      ".join(echecs))


@test("ISO-02", "Aucun endpoint ne distingue « compte inconnu » de « ressource inconnue »", "F-00")
def messages_erreur_uniformes():
    """Le defaut d'origine se lisait dans le message : `Local folder not found` au lieu de
    `Account not found` signalait que la requete avait depasse le controle de propriete."""
    fuites = []
    for method, path, params, body, label in API_PROBES:
        r = API.request(method, path.format(a=FOREIGN), params=params, json_body=body)
        d = r.detail.lower()
        if "folder not found" in d or "email not found" in d or "message not found" in d:
            fuites.append(f"{label} -> « {r.detail[:70]} »")
    expect(not fuites,
           "la reponse revele que la couche donnees a ete atteinte avant le controle :\n      "
           + "\n      ".join(fuites))


@test("ISO-03", "Un jeton absent ou invalide est refuse", "")
def authentification_requise():
    sans = API.get("/accounts/", token="")
    expect(sans.status in (401, 403), f"sans jeton : attendu 401/403, obtenu {sans.status}")
    faux = API.get("/accounts/", token="bidon.invalide.xxx")
    expect(faux.status == 401, f"jeton invalide : attendu 401, obtenu {faux.status}")


@test("ISO-04", "L'inscription reste reservee aux administrateurs", "")
def inscription_fermee():
    r = API.post("/auth/register", token="",
                 json_body={"email": "intrus@qa-autotest.local", "username": "intrus",
                            "password": "MotDePasse123!"})
    expect(r.status in (401, 403), f"attendu 401/403, obtenu {r.status} — {r.detail[:80]}")


@test("ISO-05", "Tous les outils MCP a `account_id` refusent un compte etranger", "N-04")
def mcp_outils_refusent_compte_etranger():
    """Introspection : on n'enumere pas les outils a la main, on lit leur signature.
    Un outil ajoute plus tard sera donc couvert automatiquement."""
    BOX.require()
    code = (
        "import asyncio, inspect, json, warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "import src.mcp.server as S\n"
        "from fastmcp.tools import FunctionTool\n"
        "S.USER_ID = 5\n"
        "SAFE = {'account_id': %d,\n"
        "  'folder': '__QA_INEXISTANT__', 'folder_path': '__QA_INEXISTANT__',\n"
        "  'target_folder': '__QA_INEXISTANT__', 'target_folder_path': '__QA_INEXISTANT__',\n"
        "  'source_folder': '__QA_INEXISTANT__', 'local_folder_path': '__QA_INEXISTANT__',\n"
        "  'imap_folder': '__QA_INEXISTANT__', 'folder_name': '__QA_INEXISTANT__',\n"
        "  'old_name': '__QA_INEXISTANT__', 'new_name': '__QA_INEXISTANT2__',\n"
        "  'path': '__QA_INEXISTANT__', 'name': '__QA_INEXISTANT__',\n"
        "  'uid': '999999999', 'old_uid': '999999999', 'uids': ['999999999'],\n"
        "  'email_id': 999999999, 'folder_id': 999999999, 'rule_id': 999999999,\n"
        "  'index': 0, 'attachment_index': 0, 'limit': 1, 'size': 1, 'days': 1,\n"
        "  'query': '__qa__', 'text': '__qa__', 'question': 'q', 'subject': 'qa',\n"
        "  'body': 'qa', 'to': ['test@mailia.local'], 'categories': ['a'], 'fields': ['a'],\n"
        "  'imap_criteria': 'SUBJECT \\\"__qa__\\\"',\n"
        "  'moves': [{'folder': '__QA_INEXISTANT__', 'uid': '999999999',"
        " 'target': '__QA_INEXISTANT__'}],\n"
        "  'rules': [{'target': '__QA_INEXISTANT__', 'subject_contains': '__qa__'}],\n"
        "  'markdown': '## x', 'rules_markdown': '## x',\n"
        "  'email': 'a@b.c', 'email_address': 'a@b.c', 'address': 'a@b.c',\n"
        "  'instruction': 'x', 'info_type': 'x', 'flag': 'important'}\n"
        "async def main():\n"
        "    refus, sans_refus, non_sondes = [], [], []\n"
        "    for n in sorted(x for x in dir(S) if isinstance(getattr(S, x), FunctionTool)):\n"
        "        fn = getattr(S, n).fn\n"
        "        sig = inspect.signature(fn)\n"
        "        if 'account_id' not in sig.parameters: continue\n"
        "        kw, ok = {}, True\n"
        "        for pn, p in sig.parameters.items():\n"
        "            if pn in SAFE: kw[pn] = SAFE[pn]\n"
        "            elif p.default is not inspect.Parameter.empty: continue\n"
        "            else: ok = False; break\n"
        "        if not ok: non_sondes.append(n); continue\n"
        "        try:\n"
        "            r = await fn(**kw) if asyncio.iscoroutinefunction(fn) else fn(**kw)\n"
        "            sans_refus.append((n, 'aucune erreur: ' + json.dumps(r, default=str)[:70]))\n"
        "        except Exception as e:\n"
        "            if 'not found for user' in str(e): refus.append(n)\n"
        "            else: sans_refus.append((n, type(e).__name__ + ': ' + str(e)[:70]))\n"
        "    print('__QA__' + json.dumps({'refus': refus, 'sans_refus': sans_refus,\n"
        "                                 'non_sondes': non_sondes}))\n"
        "asyncio.run(main())\n" % FOREIGN
    )
    out = BOX.python(code)
    data = None
    for line in out.splitlines():
        if line.startswith("__QA__"):
            import json as _j
            data = _j.loads(line[6:])
    expect(data is not None, f"sortie de sonde illisible : {out[-200:]}")

    # Outils isoles par partitionnement (index Elasticsearch par utilisateur, filtre user_id
    # en base) plutot que par validation de l'account_id : ils ne refusent pas, mais ne
    # fuient rien. Verifie par lecture du code en iteration 3.
    ISOLES_AUTREMENT = {"count_emails", "search_emails", "get_folders_stats",
                        "get_senders_stats", "get_processing_logs"}
    # Refus emis par un garde en amont du controle de propriete. Rien n'est execute, donc
    # aucune fuite — mais le controle de propriete n'est PAS exerce dans ce cas.
    GARDES_AMONT = ("No sync worker is running",)

    fuites, non_concluants = [], []
    for n, d in data["sans_refus"]:
        if n in ISOLES_AUTREMENT:
            continue
        if d.startswith("aucune erreur"):
            fuites.append(f"{n} — l'appel a REUSSI sur un compte etranger ({d})")
        elif any(g in d for g in GARDES_AMONT):
            non_concluants.append(n)
        else:
            fuites.append(f"{n} ({d})")

    expect(not fuites,
           f"{len(fuites)} outil(s) MCP n'ont pas refuse le compte {FOREIGN} :\n      "
           + "\n      ".join(fuites))
    if non_concluants:
        # Non silencieux : on le dit dans la sortie plutot que de compter un PASS complet.
        print(f"       > controle de propriete non exerce (garde en amont) : "
              f"{', '.join(non_concluants)}", flush=True)
    expect(len(data["refus"]) >= 40,
           f"seulement {len(data['refus'])} outils sondes — la sonde ne couvre plus la surface "
           f"attendue (non sondes : {', '.join(data['non_sondes'])})")


@test("ISO-06", "Un email local d'un autre utilisateur n'est ni lisible ni deplacable", "N-04")
def mcp_email_local_etranger_inaccessible():
    """Sonde sur une donnee reelle : on cherche en base un email local appartenant a un AUTRE
    utilisateur, on releve son dossier, on tente de le lire et de le deplacer, puis on verifie
    qu'il n'a pas bouge. Si le controle fonctionne — ce qui est attendu — rien n'est modifie."""
    BOX.require()
    from ..guard import resolved_user_id
    code = (
        "import json\n"
        "from sqlalchemy import create_engine, text\n"
        "from src.config import get_settings\n"
        "u = get_settings().database_url.replace('+asyncpg','')\n"
        "e = create_engine(u)\n"
        "with e.connect() as c:\n"
        "    r = c.execute(text('''SELECT le.id, le.folder_id FROM local_emails le\n"
        "        JOIN local_folders lf ON lf.id = le.folder_id\n"
        "        JOIN mail_accounts ma ON ma.id = lf.account_id\n"
        "        WHERE ma.user_id <> :u ORDER BY le.id LIMIT 1'''), {'u': %d}).first()\n"
        "print('__QA__' + json.dumps({'id': r[0], 'folder_id': r[1]} if r else {}))\n"
        % resolved_user_id()
    )
    import json as _j
    cible = {}
    for line in BOX.python(code).splitlines():
        if line.startswith("__QA__"):
            cible = _j.loads(line[6:])
    if not cible:
        raise Skip("aucun email local appartenant a un autre utilisateur sur cette instance")

    eid, dossier_avant = cible["id"], cible["folder_id"]
    lu = BOX.mcp("read_local_email", {"email_id": eid})
    expect("__erreur__" in lu,
           f"FUITE : read_local_email a renvoye le contenu de l'email {eid} d'un autre utilisateur")

    # Cible de deplacement dans le compte QA
    dossier = "QA_AUTOTEST_LOCAL"
    API.post(f"/accounts/{CFG.account_id}/local-folders", json_body={"name": dossier})
    dep = BOX.mcp("move_local_email", {"email_id": eid, "target_folder_path": dossier,
                                       "account_id": CFG.account_id})
    expect("__erreur__" in dep,
           f"FUITE : move_local_email a deplace l'email {eid} d'un autre utilisateur")

    verif = (
        "import json\n"
        "from sqlalchemy import create_engine, text\n"
        "from src.config import get_settings\n"
        "e = create_engine(get_settings().database_url.replace('+asyncpg',''))\n"
        "with e.connect() as c:\n"
        "    r = c.execute(text('SELECT folder_id FROM local_emails WHERE id = :i'),\n"
        "                  {'i': %d}).first()\n"
        "print('__QA__' + json.dumps({'folder_id': r[0] if r else None}))\n" % eid
    )
    apres = {}
    for line in BOX.python(verif).splitlines():
        if line.startswith("__QA__"):
            apres = _j.loads(line[6:])
    expect(apres.get("folder_id") == dossier_avant,
           f"DONNEE DEPLACEE : l'email {eid} est passe du dossier {dossier_avant} a "
           f"{apres.get('folder_id')}")

    # menage
    for f in API.get(f"/accounts/{CFG.account_id}/folders").json().get("local_folders", []):
        if f["path"] == dossier:
            ident = _j.loads(BOX.python(
                "import json\n"
                "from sqlalchemy import create_engine, text\n"
                "from src.config import get_settings\n"
                "e = create_engine(get_settings().database_url.replace('+asyncpg',''))\n"
                "with e.connect() as c:\n"
                "    r = c.execute(text(\"SELECT id FROM local_folders WHERE path = :p\"),\n"
                "                  {'p': %r}).first()\n"
                "print('__QA__' + json.dumps({'id': r[0] if r else None}))\n" % dossier
            ).split("__QA__")[1])
            if ident.get("id"):
                API.delete(f"/accounts/{CFG.account_id}/local-folders/{ident['id']}")


@test("ISO-07", "`trigger_sync` ne met jamais en file la synchro d'un autre utilisateur", "IT3/lot3")
def mcp_trigger_sync_cadre():
    """Le defaut d'origine : sans argument, l'outil appelait `sync_all_accounts`, qui couvre
    les comptes de TOUS les utilisateurs. On verifie le refus ET que rien n'est mis en file."""
    BOX.require()
    avant = _longueur_file()
    r1 = BOX.mcp("trigger_sync", {"account_id": FOREIGN})
    expect("__erreur__" in r1, f"trigger_sync a accepte le compte {FOREIGN} : {r1}")

    r2 = BOX.mcp("trigger_sync", {})
    sans_worker = "__erreur__" in r2 and "No sync worker" in str(r2.get("message", ""))

    # Propriete de securite verifiable dans tous les cas : rien n'est mis en file.
    apres = _longueur_file()
    if avant is None or apres is None:
        raise Skip(
            "longueur de la file Celery illisible : la propriete « aucune tache mise en "
            "file » ne peut pas etre verifiee. Un PASS ici affirmerait plus que ce qui a "
            "ete controle.")
    expect(apres == avant,
           f"la file Celery est passee de {avant} a {apres} : une tache a ete mise en file "
           "alors qu'aucune ne devait l'etre")

    if sans_worker:
        # Le garde `worker_online()` s'execute AVANT le controle de propriete : celui-ci
        # n'est donc pas atteint. Aucune tache n'est mise en file, la propriete de securite
        # tient, mais le cadrage par utilisateur reste a verifier worker demarre.
        raise Skip(
            "aucun worker Celery actif : `trigger_sync` court-circuite avant le controle de "
            "propriete. La non-mise en file est verifiee, le cadrage par utilisateur ne l'est "
            "pas — a rejouer une fois le worker autorise a demarrer.")

    expect("__erreur__" not in r2, f"trigger_sync sans argument a echoue : {r2}")
    vises = r2.get("account_ids", [])
    comptes_qa = {a["id"] for a in API.get("/accounts/").json()}
    hors = [i for i in vises if i not in comptes_qa]
    expect(not hors, f"trigger_sync a vise des comptes hors du perimetre de l'appelant : {hors}")


def _longueur_file():
    """Longueur de la file Celery, ou None si illisible (le test reste alors partiel)."""
    import subprocess
    try:
        cmd = ("PW=$(grep REDIS_PASSWORD /var/www/vhosts/expert-presta.com/mailia/.env "
               "| cut -d= -f2); docker exec mailia-redis redis-cli -a \"$PW\" -n 1 "
               "LLEN celery 2>/dev/null")
        p = subprocess.run(["ssh", "-o", "BatchMode=yes", CFG.ssh_host, cmd],
                           capture_output=True, text=True, timeout=40)
        return int(p.stdout.strip())
    except Exception:
        return None

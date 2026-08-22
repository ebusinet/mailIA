"""Dossiers, spam, contacts, signatures, comptes, IA, administration et en-tetes de securite.

Historique couvert : F-03 (drapeaux ignores a la creation d'un compte), F-12 (impossible de
detacher une signature avec null), F-15 (500 brut sur une erreur de fournisseur IA), F-16
(en-tete Bearer vide), N-11 (scores anti-spam contradictoires), N-14 (exceptions brutes).
"""
from __future__ import annotations

from ..core import (API, BOX, CFG, Skip, count_in, expect, expect_status,
                    messages_in, premier, seed, test, unique, work_folder)



# ---------------------------------------------------------------------------
# Dossiers
# ---------------------------------------------------------------------------

@test("FLD-01", "Creation, renommage et protection des dossiers systeme", "")
def dossiers():
    nom = f"QA_AUTOTEST_TMP_{unique()}"
    r = API.post(f"/accounts/{CFG.account_id}/create-folder", json_body={"folder_name": nom})
    expect_status(r, 200, "creation")
    r2 = API.post(f"/accounts/{CFG.account_id}/rename-folder",
                  json_body={"old_name": nom, "new_name": nom + "R"})
    expect_status(r2, 200, "renommage")
    for corps, label in (({"old_name": "INBOX", "new_name": "Pirate"}, "renommer INBOX"),):
        k = API.post(f"/accounts/{CFG.account_id}/rename-folder", json_body=corps)
        expect(k.status == 400, f"{label} : 400 attendu, {k.status} obtenu")
    k = API.post(f"/accounts/{CFG.account_id}/delete-folder", json_body={"folder_name": "Trash"})
    expect(k.status == 400, f"supprimer un dossier systeme : 400 attendu, {k.status} obtenu")
    # Menage : certains serveurs IMAP de test refusent DELETE, on n'en fait pas un echec.
    API.post(f"/accounts/{CFG.account_id}/delete-folder",
             json_body={"folder_name": nom + "R", "force": True})


@test("FLD-02", "L'arborescence decode les noms accentues", "")
def decodage_utf7():
    nom = f"QA_AUTOTEST_Éléments_{unique()}"
    API.post(f"/accounts/{CFG.account_id}/create-folder", json_body={"folder_name": nom})
    r = API.get(f"/accounts/{CFG.account_id}/folders")
    expect_status(r, 200, "arborescence")

    def noms(nodes):
        for n in nodes:
            yield n.get("display_path", n["name"])
            yield from noms(n.get("children") or [])

    expect(any(nom in x for x in noms(r.json()["folders"])),
           "le dossier accentue n'apparait pas decode dans l'arborescence")
    API.post(f"/accounts/{CFG.account_id}/delete-folder",
             json_body={"folder_name": nom, "force": True})


@test("FLD-03", "Vider un dossier supprime bien son contenu", "")
def vider_dossier():
    SRC = work_folder("FLD03")
    mk = seed(SRC, [{"from": "a@qa-autotest.local", "subject": "message a vider",
                     "date": "2024-01-01 09:00:00"}])
    expect(count_in(SRC, marker=mk) == 1, "preparation en echec")
    r = API.post(f"/accounts/{CFG.account_id}/empty-folder", json_body={"folder_name": SRC})
    expect_status(r, 200, "vidage")
    reste = count_in(SRC, marker=mk)
    expect(reste == 0, f"le dossier contient encore {reste} message(s) du test apres vidage")


# ---------------------------------------------------------------------------
# Spam
# ---------------------------------------------------------------------------

@test("SPAM-01", "Detection de spam sur en-tetes et effet de la liste blanche", "")
def spam():
    """Le scan porte sur tout le dossier : on raisonne donc sur les UID de nos propres
    messages plutot que sur un total, insensible aux residus d'executions precedentes."""
    SRC = work_folder("SPAM01")
    marqueur = unique("SPAM").lower()
    adresse = f"escroc@loterie-{marqueur}.local"
    mk = seed(SRC, [
        {"from": adresse, "subject": "URGENT You WON 1000000 dollars !!!",
         "date": "2024-01-01 03:00:00",
         "headers": {"X-Spam-Flag": "YES", "X-Spam-Status": "Yes, score=12.5 required=5.0",
                     "X-Spam-Score": "12.5"}},
        {"from": "normal@qa-autotest.local", "subject": "message ordinaire",
         "date": "2024-01-02 09:00:00"},
    ])
    miens = {m["uid"]: m["subject"] for m in messages_in(SRC, marker=mk)}
    uid_spam = next(u for u, s in miens.items() if "WON" in s)
    uid_sain = next(u for u, s in miens.items() if "ordinaire" in s)

    r = API.post(f"/accounts/{CFG.account_id}/spam-scan", json_body={"folders": [SRC]})
    expect_status(r, 200, "scan")
    detectes = set(r.json()["folders"][SRC]["spam_uids"])
    expect(uid_spam in detectes,
           "le message portant X-Spam-Flag: YES n'est pas detecte : l'analyse des en-tetes "
           "SpamAssassin ne fonctionne plus")
    expect(uid_sain not in detectes,
           "un message ordinaire est classe comme spam : faux positif")

    filtres = {m["uid"] for m in messages_in(SRC, marker=mk)
               if m["uid"] in {x["uid"] for x in
                               API.get(f"/accounts/{CFG.account_id}/messages",
                                       params={"folder": SRC, "size": 300,
                                               "filter_spam": "true"}).json()["messages"]}}
    expect(filtres == {uid_spam},
           f"filter_spam devrait renvoyer exactement le spam du test : {filtres} vs {uid_spam}")

    w = API.post(f"/accounts/{CFG.account_id}/spam-whitelist",
                 json_body={"entry_type": "email", "value": adresse})
    expect_status(w, 200, "ajout en liste blanche")
    try:
        r2 = API.post(f"/accounts/{CFG.account_id}/spam-scan", json_body={"folders": [SRC]})
        expect(uid_spam not in set(r2.json()["folders"][SRC]["spam_uids"]),
               "la liste blanche ne neutralise plus le classement en spam")
    finally:
        API.delete(f"/accounts/{CFG.account_id}/spam-whitelist/by-sender",
                   params={"email": adresse})
    reste = API.get(f"/accounts/{CFG.account_id}/spam-whitelist").json()
    expect(not any(e["value"] == adresse for e in reste), "la liste blanche n'a pas ete nettoyee")


@test("SPAM-02", "La liste noire force le classement en spam", "")
def blacklist():
    SRC = work_folder("SPAM02")
    adresse = f"indesirable-{unique().lower()}@qa-autotest.local"
    mk = seed(SRC, [{"from": adresse, "subject": "message anodin",
                     "date": "2024-01-01 09:00:00"}])
    uid = premier(messages_in(SRC, marker=mk), "misc")["uid"]

    avant = set(API.post(f"/accounts/{CFG.account_id}/spam-scan",
                         json_body={"folders": [SRC]}).json()["folders"][SRC]["spam_uids"])
    expect(uid not in avant, "le message anodin est deja classe spam avant la liste noire")

    b = API.post(f"/accounts/{CFG.account_id}/spam-blacklist",
                 json_body={"entry_type": "email", "value": adresse})
    expect_status(b, 200, "ajout en liste noire")
    try:
        apres = set(API.post(f"/accounts/{CFG.account_id}/spam-scan",
                             json_body={"folders": [SRC]}).json()["folders"][SRC]["spam_uids"])
        expect(uid in apres,
               "la liste noire n'a pas d'effet : le message de l'expediteur bloque n'est pas "
               "classe comme spam")
    finally:
        API.delete(f"/accounts/{CFG.account_id}/spam-blacklist/by-sender",
                   params={"email": adresse})


# ---------------------------------------------------------------------------
# Contacts et signatures
# ---------------------------------------------------------------------------

@test("CNT-01", "Cycle complet contacts, groupes et autocompletion", "")
def contacts():
    suffixe = unique()
    c = API.post("/contacts/", json_body={"name": f"QA autotest contact {suffixe}",
                                          "emails": [f"qa-{suffixe.lower()}@autotest.local"]})
    expect_status(c, 200, "creation du contact")
    cid = c.json()["id"]
    g = API.post("/contacts/groups", json_body={"name": f"QA autotest groupe {suffixe}"})
    expect_status(g, 200, "creation du groupe")
    gid = g.json()["id"]
    try:
        m = API.post(f"/contacts/groups/{gid}/members", json_body={"contact_ids": [cid]})
        expect_status(m, 200, "ajout au groupe")
        expect(m.json()["member_count"] == 1, "le membre n'a pas ete ajoute")
        a = API.get("/contacts/autocomplete", params={"q": suffixe.lower()[:5]})
        expect_status(a, 200, "autocompletion")
        expect(any(suffixe.lower() in x["email"].lower() for x in a.json()),
               "le contact cree n'apparait pas dans l'autocompletion")
        d = API.delete(f"/contacts/groups/{gid}/members/{cid}")
        expect_status(d, 200, "retrait du groupe")
    finally:
        API.delete(f"/contacts/groups/{gid}")
        API.delete(f"/contacts/{cid}")
    expect(API.delete(f"/contacts/{cid}").status == 404,
           "la suppression d'un contact inexistant ne renvoie pas 404")


@test("SIG-01", "Priorite de resolution : contact > groupe > defaut", "")
def signatures():
    suffixe = unique()
    adresse = f"sig-{suffixe.lower()}@autotest.local"
    ids = {}
    for cle, defaut in (("defaut", True), ("groupe", False), ("contact", False)):
        r = API.post("/signatures/", json_body={"name": f"QA autotest sig {cle} {suffixe}",
                                                "body_html": f"<p>{cle}</p>",
                                                "is_default": defaut})
        expect_status(r, 200, f"creation de la signature {cle}")
        ids[cle] = r.json()["id"]
    c = API.post("/contacts/", json_body={"name": f"QA autotest sigcontact {suffixe}", "emails": [adresse]})
    cid = c.json()["id"]
    g = API.post("/contacts/groups", json_body={"name": f"QA autotest siggroupe {suffixe}"})
    gid = g.json()["id"]
    try:
        API.post(f"/contacts/groups/{gid}/members", json_body={"contact_ids": [cid]})

        def resolue():
            r = API.get("/signatures/resolve", params={"emails": adresse})
            expect_status(r, 200, "resolution")
            s = r.json().get("signature")
            return s["name"] if s else None

        API.put(f"/contacts/{cid}", json_body={"signature_id": ids["contact"]})
        expect(resolue() == f"QA autotest sig contact {suffixe}",
               f"la signature du contact n'est pas prioritaire : « {resolue()} »")

        API.put(f"/contacts/{cid}", json_body={"signature_id": None})
        API.put(f"/contacts/groups/{gid}", json_body={"signature_id": ids["groupe"]})
        expect(resolue() == f"QA autotest sig groupe {suffixe}",
               f"detachement par null inoperant ou groupe non prioritaire : « {resolue()} » (F-12)")

        API.put(f"/contacts/groups/{gid}", json_body={"signature_id": None})
        expect(resolue() == f"QA autotest sig defaut {suffixe}",
               f"la signature par defaut n'est pas retenue : « {resolue()} »")
    finally:
        API.delete(f"/contacts/groups/{gid}")
        API.delete(f"/contacts/{cid}")
        for i in ids.values():
            API.delete(f"/signatures/{i}")


# ---------------------------------------------------------------------------
# Comptes
# ---------------------------------------------------------------------------

@test("ACC-01", "La creation d'un compte honore smtp_ssl et sync_enabled", "F-03")
def creation_compte():
    """Les deux drapeaux etaient absents du constructeur et retombaient sur True, rendant
    inutilisable tout compte a SMTP en clair."""
    r = API.post("/accounts/", json_body={
        "name": f"QA autotest compte {unique()}", "imap_host": "greenmail", "imap_port": 3143,
        "imap_ssl": False, "imap_user": "test", "imap_password": "testpass123",
        "smtp_host": "greenmail", "smtp_port": 3025, "smtp_ssl": False,
        "smtp_user": "test", "smtp_password": "testpass123", "sync_enabled": False,
    })
    expect_status(r, 200, "creation du compte")
    a = r.json()
    try:
        expect(a["smtp_ssl"] is False,
               "smtp_ssl=false ignore a la creation : le compte retombe sur TLS implicite (F-03)")
        expect(a["sync_enabled"] is False,
               "sync_enabled=false ignore a la creation : la synchro demarre sans qu'on l'ait demandee")
        t = API.post(f"/accounts/{a['id']}/test-smtp")
        expect(t.status == 200 and t.json().get("status") == "ok",
               f"le compte cree n'arrive pas a joindre son SMTP : {t.json().get('message')}")
        u = API.put(f"/accounts/{a['id']}", json_body={"smtp_ssl": True, "sync_enabled": True})
        expect_status(u, 200, "modification")
        expect(u.json()["smtp_ssl"] is True and u.json()["sync_enabled"] is True,
               "la modification des drapeaux n'est pas prise en compte")
    finally:
        API.delete(f"/accounts/{a['id']}")


@test("ACC-02", "Le formulaire de compte expose le reglage SSL du SMTP", "FS-01")
def formulaire_smtp_ssl():
    """Le correctif backend de F-03 etait inatteignable : l'interface n'avait aucun champ
    pour le reglage SSL du SMTP et ne transmettait jamais la valeur."""
    from pathlib import Path
    front = Path(__file__).resolve().parents[3] / "src" / "web" / "static" / "index.html"
    if not front.exists():
        raise Skip("frontend indisponible depuis cette machine")
    t = front.read_text(errors="replace")
    expect("acc-smtp-ssl" in t,
           "le formulaire de compte n'a plus de champ SSL pour le SMTP : un serveur en clair "
           "redevient inconfigurable depuis l'interface (FS-01)")
    expect("smtp_ssl:" in t or "smtp_ssl :" in t,
           "le champ existe mais `smtp_ssl` n'est plus transmis dans la requete")


# ---------------------------------------------------------------------------
# IA
# ---------------------------------------------------------------------------

@test("AI-01", "Cycle de vie d'un fournisseur IA, cle jamais exposee", "")
def providers():
    r = API.post("/ai/providers", json_body={
        "name": f"QA autotest {unique()}", "provider_type": "openai",
        "endpoint": "http://127.0.0.1:9/v1", "api_key": "sk-qa-autotest-secret",
        "model": "gpt-4o-mini"})
    expect_status(r, 200, "creation du fournisseur")
    pid = r.json()["id"]
    try:
        expect("api_key" not in r.json(), "la cle API est renvoyee dans la reponse")
        liste = API.get("/ai/providers").json()
        expect(all("api_key" not in p for p in liste), "la cle API apparait dans la liste")
        t = API.post(f"/ai/providers/{pid}/test", json_body={})
        expect_status(t, 200, "test de connexion")
        expect(t.json().get("status") == "error",
               "un endpoint mort devrait produire une erreur structuree")
    finally:
        API.delete(f"/ai/providers/{pid}")


@test("AI-02", "Une erreur de fournisseur produit une reponse structuree, pas un 500", "F-15")
def chat_erreurs():
    r = API.post("/ai/chat", json_body={"message": "test", "provider_id": 999999})
    expect(r.status == 404,
           f"provider_id inexistant : 404 attendu, {r.status} obtenu — un 500 brut signale "
           "l'absence de validation (F-15)")
    p = API.post("/ai/providers", json_body={
        "name": f"QA autotest chat {unique()}", "provider_type": "openai",
        "endpoint": "http://127.0.0.1:9/v1", "api_key": "sk-x", "model": "gpt-4o-mini"})
    pid = p.json()["id"]
    try:
        c = API.post("/ai/chat", json_body={"message": "test", "provider_id": pid})
        expect(c.status != 500,
               "un fournisseur injoignable produit un 500 brut au lieu d'une erreur structuree (F-15)")
    finally:
        API.delete(f"/ai/providers/{pid}")


@test("AI-03", "Le digest echoue proprement quand l'IA est indisponible", "")
def digest():
    r = API.get("/digest/weekly", params={"days": 7})
    expect(r.status in (200, 502),
           f"reponse inattendue : {r.status} — {r.detail[:100]}")
    v = API.get("/digest/weekly", params={"days": 99})
    expect(v.status == 422, f"days=99 : 422 attendu, {v.status} obtenu")


@test("AI-04", "Les outils IA du MCP renvoient une erreur typee", "N-14")
def mcp_outils_ia():
    SRC = work_folder("AI04")
    BOX.require()
    mk = seed(SRC, [{"from": "a@qa-autotest.local", "subject": "message pour IA",
                     "date": "2024-01-01 09:00:00"}])
    uid = premier(messages_in(SRC, marker=mk), "misc")["uid"]
    r = BOX.mcp("summarize_email", {"account_id": CFG.account_id, "folder": SRC, "uid": uid})
    if "__erreur__" not in r:
        raise Skip("un fournisseur IA fonctionnel est configure : le chemin d'erreur "
                   "n'est pas exercable")
    expect(r["__erreur__"] == "ToolError",
           f"exception brute remontee au lieu d'un ToolError : {r['__erreur__']} (N-14)")


# ---------------------------------------------------------------------------
# Administration et securite
# ---------------------------------------------------------------------------

@test("ADM-01", "Parametres systeme : liste blanche de cles et masquage des secrets", "")
def admin_settings():
    r = API.get("/admin/settings")
    if r.status == 403:
        raise Skip("le compte QA n'est pas administrateur sur cette instance")
    expect_status(r, 200, "lecture des parametres")
    k = API.put("/admin/settings", json_body=[{"key": "cle_inexistante_qa", "value": "x"}])
    expect(k.status == 400, f"cle inconnue : 400 attendu, {k.status} obtenu")

    avant = next((s["value"] for s in r.json() if s["key"] == "anthropic_api_key"), "")
    API.put("/admin/settings", json_body=[{"key": "anthropic_api_key",
                                           "value": "sk-ant-QAAUTOTEST123"}])
    try:
        relu = next(s["value"] for s in API.get("/admin/settings").json()
                    if s["key"] == "anthropic_api_key")
        expect("QAAUTOTEST123" not in relu and "*" in relu,
               f"le secret est renvoye en clair : « {relu} »")
    finally:
        API.put("/admin/settings", json_body=[{"key": "anthropic_api_key", "value": avant}])


@test("ADM-02", "Un administrateur ne peut pas retirer ses propres droits", "")
def admin_auto_retrait():
    me = API.get("/auth/me").json()
    if not me.get("is_admin"):
        raise Skip("le compte QA n'est pas administrateur sur cette instance")
    r = API.put(f"/admin/users/{me['id']}", json_body={"is_admin": False})
    expect(r.status == 400, f"400 attendu, {r.status} obtenu")
    expect(API.get("/auth/me").json()["is_admin"] is True,
           "les droits administrateur ont ete retires malgre le refus annonce")
    k = API.put("/admin/users/999999", json_body={"is_active": False})
    expect(k.status == 404, f"utilisateur inexistant : 404 attendu, {k.status} obtenu")


@test("SEC-01", "Les six en-tetes de securite sont presents", "")
def entetes_securite():
    r = API.get("/health")
    attendus = ["strict-transport-security", "x-frame-options", "x-content-type-options",
                "x-xss-protection", "referrer-policy", "permissions-policy"]
    presents = {k.lower() for k in r.headers}
    manquants = [h for h in attendus if h not in presents]
    expect(not manquants, f"en-tetes absents : {', '.join(manquants)}")
    expect(r.headers.get("X-Frame-Options", "").upper() == "DENY",
           f"X-Frame-Options vaut « {r.headers.get('X-Frame-Options')} » au lieu de DENY")


@test("SEC-02", "La limitation de debit protege la connexion", "")
def rate_limit():
    codes = []
    for _ in range(7):
        r = API.post("/auth/login", token="",
                     json_body={"email": "qa-autotest-inexistant@nulle.part",
                                "password": "mauvais"})
        codes.append(r.status)
        if r.status == 429:
            break
    expect(429 in codes,
           f"aucune limitation apres {len(codes)} tentatives de connexion echouees : {codes}")

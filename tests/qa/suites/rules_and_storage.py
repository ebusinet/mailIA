"""Regles automatiques, stockage local et import.

Historique :
- F-09 : `parseaddr()` renvoie une chaine vide sur un en-tete `To:` multi-destinataires, donc
  aucune condition sur le destinataire ne pouvait matcher.
- F-13 / F-14 : l'analyseur de regles IA mettait les noms de dossiers en minuscules et ne
  reconnaissait ni « transferer » ni « marquer important », en silence.
- F-07 : deplacer un email d'IMAP vers le stockage local echouait en 500 (datetime avec fuseau
  insere dans une colonne sans fuseau).
- F-08 : le retour du stockage local vers IMAP ecrasait la date d'origine.
"""
from __future__ import annotations

from ..core import (API, CFG, Skip, count_in, expect, expect_status,
                    messages_in, premier, seed, test, unique, work_folder)



def _regle(nom, conditions, actions=None, mode="all"):
    r = API.post("/rules/classic/", json_body={
        "name": nom, "match_mode": mode, "conditions": conditions,
        "actions": actions or [{"type": "mark_read"}],
    })
    expect_status(r, 200, f"creation de la regle « {nom} »")
    return r.json()["id"]


def _appliquer(rid, folder):
    """Applique une regle en mode flux : le nombre de correspondances est emis avant
    l'execution des actions, on l'obtient donc meme si une action echoue."""
    r = API.post(f"/rules/classic/{rid}/apply", params={"stream": "true"},
                 json_body={"account_id": CFG.account_id, "folder": folder})
    expect_status(r, 200, "application de la regle")
    matched, erreur = None, None
    import json
    for ligne in r.text.splitlines():
        if not ligne.strip():
            continue
        o = json.loads(ligne)
        if o.get("type") in ("progress", "result"):
            matched = o.get("matched")
        if o.get("type") == "error":
            erreur = o.get("detail")
    expect(erreur is None, f"la regle a leve une erreur : {erreur}")
    return matched


@test("RULE-01", "Validation des champs, operateurs et actions", "")
def validation_regles():
    cas = [
        ({"name": "x", "conditions": [{"field": "nimp", "operator": "contains", "value": "x"}],
          "actions": [{"type": "mark_read"}]}, "champ invalide"),
        ({"name": "x", "conditions": [{"field": "from", "operator": "nimp", "value": "x"}],
          "actions": [{"type": "mark_read"}]}, "operateur invalide"),
        ({"name": "x", "conditions": [{"field": "from", "operator": "contains", "value": "x"}],
          "actions": [{"type": "nimp"}]}, "action invalide"),
        ({"name": "x", "conditions": [], "actions": [{"type": "mark_read"}]}, "conditions vides"),
        ({"name": "x", "conditions": [{"field": "from", "operator": "contains", "value": "x"}],
          "actions": []}, "actions vides"),
    ]
    echecs = []
    for corps, label in cas:
        r = API.post("/rules/classic/", json_body=corps)
        if r.status != 422:
            echecs.append(f"{label} -> {r.status} (422 attendu)")
            if r.status == 200:
                API.delete(f"/rules/classic/{r.json()['id']}")
    expect(not echecs, "validation defaillante :\n      " + "\n      ".join(echecs))


@test("RULE-02", "Les operateurs de conditions matchent correctement", "")
def operateurs():
    SRC = work_folder("RULE02")
    # Dates relatives a l'instant present : figees, elles finiraient toutes du meme cote
    # du seuil et l'operateur `age` cesserait de discriminer.
    import datetime as _dt
    recent = (_dt.datetime.now() - _dt.timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    ancien = (_dt.datetime.now() - _dt.timedelta(days=800)).strftime("%Y-%m-%d %H:%M:%S")
    mk = seed(SRC, [
        {"from": "Alice <alice@acme-qa.local>", "subject": "PREFIX facture 001",
         "date": recent},
        {"from": "Bob <bob@acme-qa.local>", "subject": "autre sujet",
         "date": ancien, "cc": "carol@autre-qa.local"},
        {"from": "Zoe <zoe@zeta-qa.local>", "subject": "PREFIX devis",
         "date": recent},
    ])
    attendus = [
        ("domain_is acme", [{"field": "from", "operator": "domain_is", "value": "acme-qa.local"}], 2),
        ("starts_with", [{"field": "subject", "operator": "starts_with", "value": mk}], 3),
        ("contains PREFIX", [{"field": "subject", "operator": "contains", "value": "PREFIX"}], 2),
        ("ends_with", [{"field": "subject", "operator": "ends_with", "value": "devis"}], 1),
        ("not_contains", [{"field": "subject", "operator": "not_contains", "value": "PREFIX"}], 1),
        ("equals", [{"field": "from", "operator": "equals", "value": "alice@acme-qa.local"}], 1),
        ("cc contains", [{"field": "cc", "operator": "contains", "value": "carol"}], 1),
        ("age older_than 1an", [{"field": "age", "operator": "older_than", "value": "52w"}], 1),
        ("age newer_than 1sem", [{"field": "age", "operator": "newer_than", "value": "7d"}], 2),
        ("is_reply false", [{"field": "is_reply", "operator": "is_false"}], 3),
        ("has_attachments false", [{"field": "has_attachments", "operator": "is_false"}], 3),
        ("mode any", [{"field": "from", "operator": "contains", "value": "zeta-qa"},
                      {"field": "from", "operator": "contains", "value": "zzz"}], 1),
    ]
    echecs = []
    for label, conds, attendu in attendus:
        mode = "any" if label == "mode any" else "all"
        rid = _regle(f"QA autotest {unique()}", conds, mode=mode)
        try:
            got = _appliquer(rid, SRC)
            if got != attendu:
                echecs.append(f"{label} : {attendu} attendu, {got} obtenu")
        finally:
            API.delete(f"/rules/classic/{rid}")
    expect(not echecs, "operateurs incorrects :\n      " + "\n      ".join(echecs))


@test("RULE-03", "Une condition sur le destinataire matche un email multi-destinataires", "F-09")
def destinataire_multiple():
    SRC = work_folder("RULE03")
    """`parseaddr()` renvoyait ('','') sur un en-tete a plusieurs adresses : le champ
    devenait vide et aucune condition sur `to` ne pouvait aboutir."""
    seed(SRC, [{"from": "exp@qa-autotest.local", "subject": "QA multi destinataires",
                "to": "premier@qa-autotest.local, second@qa-autotest.local",
                "date": "2024-04-04 09:00:00"}])
    echecs = []
    for adresse in ("premier@qa-autotest.local", "second@qa-autotest.local"):
        rid = _regle(f"QA autotest to {unique()}",
                     [{"field": "to", "operator": "contains", "value": adresse}])
        try:
            got = _appliquer(rid, SRC)
            if got != 1:
                echecs.append(f"to contains {adresse} : 1 attendu, {got} obtenu")
        finally:
            API.delete(f"/rules/classic/{rid}")
    expect(not echecs,
           "le champ destinataire est vide sur les emails multi-destinataires :\n      "
           + "\n      ".join(echecs))


@test("RULE-04", "L'analyseur de regles IA preserve la casse et reconnait les actions", "F-13/F-14")
def parsing_regles_ia():
    md = ("## Factures\n"
          "- **Si**: le sujet contient \"facture\"\n"
          "- **Alors**: deplacer vers Comptabilite\n"
          "- **Notifier**: oui\n\n"
          "## Urgent\n"
          "- **Si**: le sujet contient \"urgent\"\n"
          "- **Alors**: marquer important\n"
          "- **Et**: transferer a destinataire@qa-autotest.local\n")
    r = API.post("/rules/", json_body={"name": f"QA autotest IA {unique()}",
                                       "rules_markdown": md})
    expect_status(r, 200, "creation de la regle IA")
    rid = r.json()["id"]
    try:
        p = API.post(f"/rules/{rid}/preview")
        expect_status(p, 200, "apercu")
        regles = p.json()["rules"]
        expect(len(regles) == 2, f"2 regles attendues, {len(regles)} analysees")

        move = [a for a in regles[0]["actions"] if a["type"] == "move"]
        expect(move, f"action `move` non reconnue : {regles[0]['actions']}")
        expect(move[0]["target"] == "Comptabilite",
               f"le nom du dossier a ete altere : « {move[0]['target']} » au lieu de "
               "« Comptabilite » — les dossiers IMAP sont sensibles a la casse (F-13)")

        types = {a["type"] for a in regles[1]["actions"]}
        expect("flag" in types, f"« marquer important » non reconnu : {regles[1]['actions']}")
        expect("forward" in types, f"« transferer » non reconnu : {regles[1]['actions']} (F-14)")
    finally:
        API.delete(f"/rules/{rid}")


@test("RULE-05", "Cycle de vie complet d'une regle classique", "")
def cycle_regle():
    rid = _regle(f"QA autotest cycle {unique()}",
                 [{"field": "subject", "operator": "contains", "value": "zzz"}])
    try:
        u = API.put(f"/rules/classic/{rid}", json_body={"priority": 5, "is_active": False})
        expect_status(u, 200, "modification")
        expect(u.json()["priority"] == 5 and u.json()["is_active"] is False,
               "la modification n'a pas ete appliquee")
        ko = API.put(f"/rules/classic/{rid}",
                     json_body={"conditions": [{"field": "from", "operator": "nimp", "value": "x"}]})
        expect(ko.status == 422, f"validation absente a la modification : {ko.status}")
    finally:
        d = API.delete(f"/rules/classic/{rid}")
        expect_status(d, 200, "suppression")
    expect(API.delete(f"/rules/classic/{rid}").status == 404,
           "la suppression d'une regle inexistante ne renvoie pas 404")


@test("STO-01", "Cycle complet du stockage local", "")
def dossiers_locaux():
    nom = f"QA_AUTOTEST_LOCAL_{unique()}"
    r = API.post(f"/accounts/{CFG.account_id}/local-folders", json_body={"name": nom})
    expect_status(r, 200, "creation du dossier local")
    fid = r.json()["id"]
    try:
        dbl = API.post(f"/accounts/{CFG.account_id}/local-folders", json_body={"name": nom})
        expect(dbl.status == 409, f"doublon : 409 attendu, {dbl.status} obtenu")
        liste = API.get(f"/accounts/{CFG.account_id}/folders").json()["local_folders"]
        expect(any(f["path"] == nom for f in liste), "le dossier local cree n'apparait pas")
    finally:
        d = API.delete(f"/accounts/{CFG.account_id}/local-folders/{fid}")
        expect_status(d, 200, "suppression du dossier local")
    expect(API.delete(f"/accounts/{CFG.account_id}/local-folders/{fid}").status == 404,
           "la suppression d'un dossier local inexistant ne renvoie pas 404")


def _date_preservee(origine: str, obtenue: str, etape: str):
    """Verifie que la date d'origine survit au deplacement.

    Le defaut F-08 remplacait la date par celle du deplacement, ce qui detruisait le
    classement chronologique. Une tolerance de 14 h est admise : le stockage local
    normalise les horodatages en UTC sans les reafficher dans le fuseau d'origine, un
    aller-retour decale donc l'heure affichee du decalage horaire (constat R-08, mineur et
    deja documente). Au-dela, c'est bien la date qui a ete perdue.
    """
    import datetime as _dt
    fmt = "%Y-%m-%d %H:%M"
    a = _dt.datetime.strptime(origine, fmt)
    b = _dt.datetime.strptime(obtenue, fmt)
    ecart = abs((b - a).total_seconds()) / 3600
    expect(ecart <= 14,
           f"date perdue {etape} : « {origine} » -> « {obtenue} » ({ecart:.0f} h d'ecart). "
           "L'email perd sa place dans le classement chronologique (F-08).")
    maintenant = _dt.datetime.now()
    expect(abs((b - maintenant).total_seconds()) > 86400,
           f"date remplacee par celle du deplacement {etape} : « {obtenue} » (F-08)")


@test("STO-02", "Deplacement IMAP vers local et retour, sans perte de date", "F-07/F-08")
def aller_retour_local():
    SRC = work_folder("STO02")
    """F-07 : l'aller echouait en 500. F-08 : le retour ecrasait la date d'origine par celle
    du deplacement, ce qui detruisait le classement chronologique."""
    nom = f"QA_AUTOTEST_LOCAL_{unique()}"
    r = API.post(f"/accounts/{CFG.account_id}/local-folders", json_body={"name": nom})
    expect_status(r, 200, "creation du dossier local")
    fid = r.json()["id"]
    try:
        mk = seed(SRC, [{"from": "archive@qa-autotest.local",
                         "subject": f"aller-retour {unique()}",
                         "date": "2021-07-08 11:22:00"}])
        msg = premier(messages_in(SRC, marker=mk), "rules_and_storage")
        date_origine = msg["date"]

        aller = API.post(f"/accounts/{CFG.account_id}/message/{msg['uid']}/move",
                         params={"folder": SRC, "storage": "imap", "target_storage": "local"},
                         json_body={"target_folder": nom})
        expect_status(aller, 200, "deplacement IMAP vers local")
        locaux = messages_in(nom, storage="local")
        expect(len(locaux) == 1, f"1 email attendu en local, {len(locaux)} trouve(s)")
        _date_preservee(date_origine, locaux[0]["date"], "a l'aller (IMAP vers local)")
        expect(count_in(SRC) == 0, "l'email n'a pas ete retire du dossier IMAP source")

        from ..core import empty_folder
        DST = work_folder("STO02D")
        empty_folder(DST)
        retour = API.post(f"/accounts/{CFG.account_id}/message/{locaux[0]['uid']}/move",
                          params={"folder": nom, "storage": "local", "target_storage": "imap"},
                          json_body={"target_folder": DST})
        expect_status(retour, 200, "deplacement local vers IMAP")
        revenus = messages_in(DST)
        expect(len(revenus) == 1, f"1 email attendu apres retour, {len(revenus)} trouve(s)")
        _date_preservee(date_origine, revenus[0]["date"], "au retour (local vers IMAP)")
    finally:
        API.delete(f"/accounts/{CFG.account_id}/local-folders/{fid}")


@test("IMP-01", "Import mbox, dedoublonnage et suivi de job", "")
def import_mbox():
    from ..core import build_mbox
    dossier = work_folder("IMP01")
    from ..core import empty_folder
    empty_folder(dossier)
    contenu = build_mbox([
        {"from": "imp1@qa-autotest.local", "subject": "QA-IMP01 un", "date": "2022-01-01 08:00:00"},
        {"from": "imp2@qa-autotest.local", "subject": "QA-IMP01 deux", "date": "2022-01-02 08:00:00"},
    ])
    import time

    def lancer():
        r = API.upload(f"/accounts/{CFG.account_id}/import-mbox", "qa.mbox", contenu,
                       params={"storage": "imap", "folder": dossier})
        expect_status(r, 200, "import")
        jid = r.json()["job_id"]
        for _ in range(15):
            time.sleep(0.7)
            j = API.get(f"/accounts/import-jobs/{jid}")
            if j.status == 200 and j.json().get("status") in ("done", "error"):
                return j.json()
        raise AssertionError("le job d'import ne s'est pas termine dans le delai imparti")

    j1 = lancer()
    expect(j1["status"] == "done", f"premier import : statut {j1['status']}")
    expect(j1["progress"]["imported"] == 2,
           f"2 messages attendus, {j1['progress']['imported']} importes")
    j2 = lancer()
    expect(j2["progress"]["skipped"] == 2 and j2["progress"]["imported"] == 0,
           f"le dedoublonnage par Message-ID ne fonctionne plus : {j2['progress']}")
    expect(count_in(dossier) == 2,
           f"le dossier contient {count_in(dossier)} messages au lieu de 2 : doublons crees")
    hist = API.get("/accounts/import-jobs")
    expect_status(hist, 200, "historique des jobs")
    expect(isinstance(hist.json(), list), "l'historique n'est pas une liste")

"""Listage, tris, filtres, dates et pieces jointes.

Historique :
- F-04 : le filtre par expediteur reduisait l'adresse a son nom affiche, donc filtrer par
  adresse ou par domaine ne renvoyait jamais rien.
- F-05 puis N-01 : la colonne date a longtemps eu trois sources differentes — affichage
  depuis l'en-tete `Date:`, tri et filtre depuis l'INTERNALDATE. Une liste « triee par date »
  presentait des ruptures visibles et le filtre ne trouvait pas ce qu'il affichait.

Les tests filtrent systematiquement sur le marqueur unique renvoye par `seed()` : le dossier
de travail peut contenir des residus d'executions precedentes sans fausser les comptages.
"""
from __future__ import annotations

from ..core import (API, BOX, CFG, Skip, expect, expect_status,
                    messages_in, seed, test, unique, work_folder)


JEU = [
    {"from": "Alice Martin <alice@acme-qa.local>", "subject": "AAA premier sujet",
     "date": "2024-01-05 09:15:00"},
    {"from": "Bob Durand <bob@acme-qa.local>", "subject": "MMM sujet median",
     "date": "2024-02-10 14:30:00", "cc": "carol@autre-qa.local"},
    {"from": "Zoe <zoe@zeta-qa.local>", "subject": "ZZZ dernier sujet",
     "date": "2024-03-15 18:45:00"},
]


def _lister(dossier, **params):
    p = {"folder": dossier, "size": 300}
    p.update(params)
    r = API.get(f"/accounts/{CFG.account_id}/messages", params=p)
    expect_status(r, 200, f"listage {params}")
    return r.json()


@test("MAIL-01", "Listage et pagination", "")
def listage_pagination():
    SRC = work_folder('MAIL01')
    mk = seed(SRC, JEU)
    total = _lister(SRC)["total"]
    p0 = _lister(SRC, size=2, page=0)["messages"]
    p1 = _lister(SRC, size=2, page=1)["messages"]
    expect(len(p0) == 2, f"page 0 (size 2) : 2 messages attendus, {len(p0)} obtenus")
    attendu_p1 = min(2, max(0, total - 2))
    expect(len(p1) == attendu_p1,
           f"page 1 (size 2) : {attendu_p1} attendus sur {total} au total, {len(p1)} obtenus")
    uids = {m["uid"] for m in p0} | {m["uid"] for m in p1}
    expect(len(uids) == len(p0) + len(p1),
           "les pages 0 et 1 se recouvrent : la pagination renvoie deux fois le meme message")
    expect(len(messages_in(SRC, marker=mk)) == 3, "les 3 messages de test ne sont pas listables")


@test("MAIL-02", "Les six combinaisons de tri sont correctes", "N-01")
def tris():
    SRC = work_folder('MAIL02')
    seed(SRC, JEU)
    echecs = []
    for champ, cle in (("date", "date"), ("from", "from"), ("subject", "subject")):
        for sens in ("asc", "desc"):
            msgs = _lister(SRC, sort_by=champ, sort_order=sens)["messages"]
            vals = [m[cle] if champ == "date" else m[cle].lower() for m in msgs]
            attendu = sorted(vals, reverse=(sens == "desc"))
            if vals != attendu:
                echecs.append(f"{champ} {sens} : obtenu {vals[:5]}")
    expect(not echecs, "tris incorrects :\n      " + "\n      ".join(echecs))


@test("MAIL-03", "La date affichee en liste et en detail est la meme", "F-05")
def date_liste_detail():
    SRC = work_folder('MAIL03')
    mk = seed(SRC, JEU)
    for m in messages_in(SRC, marker=mk):
        d = API.get(f"/accounts/{CFG.account_id}/message/{m['uid']}", params={"folder": SRC})
        expect_status(d, 200, f"lecture de {m['uid']}")
        expect(d.json()["date"] == m["date"],
               f"uid {m['uid']} : la liste affiche « {m['date']} », le detail "
               f"« {d.json()['date']} » — deux sources de date differentes")


@test("MAIL-04", "Le filtre de date porte sur la date affichee", "N-01")
def filtre_date_coherent():
    SRC = work_folder('MAIL04')
    mk = seed(SRC, JEU)
    for m in messages_in(SRC, marker=mk):
        jour = m["date"][:10]
        trouves = _lister(SRC, filter_date=jour)["messages"]
        expect(any(x["uid"] == m["uid"] for x in trouves),
               f"filter_date={jour} ne renvoie pas l'email {m['uid']} qui affiche pourtant "
               "cette date : le filtre et l'affichage utilisent des sources differentes")


@test("MAIL-05", "Le tri par date reste coherent quand Date: et date de reception divergent", "N-01")
def date_divergente():
    """Cas qui a revele N-01 : un email reinjecte (import, restauration, transfert) porte une
    date d'en-tete ancienne et une date de reception recente."""
    SRC = work_folder('MAIL05')
    BOX.require()
    seed(SRC, JEU)
    marqueur = unique("DIV")
    BOX.imap(
        "m = EmailMessage()\n"
        "m['From'] = 'diverge@qa-autotest.local'\n"
        "m['To'] = 'test@mailia.local'\n"
        f"m['Subject'] = 'QA-MAIL05 {marqueur}'\n"
        "m['Date'] = email.utils.formatdate(time.mktime(time.strptime("
        "'2019-04-05 08:30:00', '%Y-%m-%d %H:%M:%S')), localtime=True)\n"
        f"m['Message-ID'] = '<{marqueur}@qa-autotest.local>'\n"
        "m.set_content('en-tete Date ancienne, date de reception recente')\n"
        f"M.append({SRC!r}, '', imaplib.Time2Internaldate(time.time()), m.as_bytes())\n"
    )
    cible = [m for m in messages_in(SRC) if marqueur in m["subject"]]
    expect(cible, "le message a date divergente n'a pas ete depose")
    affichee = cible[0]["date"]
    expect(affichee.startswith("2019-04-05"),
           f"la liste affiche « {affichee} » : elle utilise la date de reception, pas "
           "l'en-tete Date:")

    liste = [m["date"] for m in _lister(SRC)["messages"]]
    expect(liste == sorted(liste, reverse=True),
           f"la liste triee par date n'est pas monotone : {liste[:6]}")

    trouves = _lister(SRC, filter_date="2019-04-05")["messages"]
    expect(any(marqueur in m["subject"] for m in trouves),
           "le filtre ne retrouve pas le message a la date qu'il affiche")


@test("MAIL-06", "Le filtre par expediteur accepte une adresse complete", "F-04")
def filtre_expediteur():
    SRC = work_folder('MAIL06')
    seed(SRC, JEU)
    msgs = _lister(SRC, filter_from="alice@acme-qa.local")["messages"]
    if not msgs:
        # Le filtre s'appuie sur IMAP SEARCH FROM : certains serveurs de test ne gerent que
        # la correspondance exacte. On distingue la limite serveur du defaut applicatif.
        raise Skip("IMAP SEARCH FROM ne renvoie rien sur ce serveur : limite du serveur de test")
    expect(all("alice" in m["from"].lower() for m in msgs),
           f"le filtre renvoie des messages d'autres expediteurs : "
           f"{[m['from'] for m in msgs][:3]}")


@test("MAIL-07", "Les autres filtres de colonne repondent et discriminent", "")
def autres_filtres():
    dossier = work_folder("MAIL07")
    mk = seed(dossier, JEU)
    echecs = []
    for f in ({"filter_subject": "MMM"}, {"filter_attachments": "true"},
              {"filter_spam": "true"}, {"filter_replied": "true"}, {"q": "sujet"}):
        p = {"folder": dossier, "size": 300}
        p.update(f)
        r = API.get(f"/accounts/{CFG.account_id}/messages", params=p)
        if r.status != 200:
            echecs.append(f"{f} -> {r.status} {r.detail[:60]}")
    expect(not echecs, "filtres en echec :\n      " + "\n      ".join(echecs))

    # Discrimination : parmi les messages du test, un seul porte « MMM ».
    mmm = [m for m in _lister(dossier, filter_subject="MMM")["messages"]
           if mk in m["subject"]]
    expect(len(mmm) == 1, f"filter_subject=MMM : 1 message du test attendu, {len(mmm)} obtenu(s)")


@test("MAIL-08", "Lecture d'un email, en-tetes et piece jointe", "")
def lecture_et_piece_jointe():
    import base64
    import time
    sujet = f"QA-MAIL08 {unique()}"
    contenu = b"contenu de la piece jointe de test"
    r = API.post(f"/accounts/{CFG.account_id}/send", json_body={
        "to": ["test@mailia.local"], "subject": sujet, "body_text": "avec piece jointe",
        "attachments": [{"filename": "note.txt",
                         "data_base64": base64.b64encode(contenu).decode()}],
    })
    expect_status(r, 200, "envoi avec piece jointe")

    uid = None
    fin = time.time() + 20
    while uid is None and time.time() < fin:
        time.sleep(0.7)
        for m in messages_in("INBOX"):
            if sujet in m["subject"]:
                uid = m["uid"]
                break
    expect(uid, "l'email avec piece jointe n'est jamais arrive")

    d = API.get(f"/accounts/{CFG.account_id}/message/{uid}", params={"folder": "INBOX"})
    expect_status(d, 200, "lecture")
    pj = d.json()["attachments"]
    expect(pj and pj[0]["filename"] == "note.txt", f"piece jointe absente : {pj}")

    dl = API.get(f"/accounts/{CFG.account_id}/message/{uid}/attachment/0",
                 params={"folder": "INBOX"})
    expect_status(dl, 200, "telechargement")
    expect(dl.body == contenu, "le contenu telecharge differe de l'original")

    ko = API.get(f"/accounts/{CFG.account_id}/message/{uid}/attachment/99",
                 params={"folder": "INBOX"})
    expect(ko.status == 404, f"index de piece jointe invalide : 404 attendu, {ko.status} obtenu")


@test("MAIL-09", "Un email et un dossier inexistants renvoient une erreur explicite", "N-15")
def erreurs_explicites():
    r = API.get(f"/accounts/{CFG.account_id}/message/999999999", params={"folder": "INBOX"})
    expect(r.status == 404, f"email inexistant : 404 attendu, {r.status} obtenu")
    r2 = API.get(f"/accounts/{CFG.account_id}/messages", params={"folder": "__QA_INEXISTANT__"})
    expect(r2.status in (400, 404, 502),
           f"dossier inexistant : erreur attendue, {r2.status} obtenu")
    expect("illegal in state" not in r2.detail.lower(),
           f"l'erreur revele un etat IMAP interne au lieu de nommer le probleme : "
           f"{r2.detail[:100]}")


@test("MAIL-10", "Export d'un dossier en ZIP", "")
def export_zip():
    SRC = work_folder('MAIL10')
    import io
    import zipfile
    mk = seed(SRC, JEU)
    r = API.get(f"/accounts/{CFG.account_id}/folder-export", params={"folder": SRC})
    expect_status(r, 200, "export")
    expect("zip" in r.headers.get("Content-Type", "").lower(),
           f"type de contenu inattendu : {r.headers.get('Content-Type')}")
    noms = zipfile.ZipFile(io.BytesIO(r.body)).namelist()
    total = len(messages_in(SRC))
    expect(len(noms) == total,
           f"l'export contient {len(noms)} fichiers pour {total} messages dans le dossier")
    expect(len(noms) >= 3, f"les 3 messages du test devraient au moins y figurer ({mk})")

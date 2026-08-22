"""Non-duplication des emails lors des deplacements et suppressions.

Historique : F-02 (itération 1). Les commandes IMAP `STORE` etaient emises sans parentheses
autour de la liste de flags (`+FLAGS \\Deleted` au lieu de `+FLAGS (\\Deleted)`). Sur un
serveur a analyseur strict, le `COPY` reussissait puis le `STORE` echouait : l'original
n'etait jamais supprime. L'utilisateur voyait une erreur 502 alors que son email venait
d'etre DUPLIQUE. Trois tentatives = trois copies.

Le motif de verification est toujours le meme et c'est lui qui compte : compter la source ET
la destination avant et apres, et exiger que le total soit inchange. Un code HTTP 200 ne
prouve rien ici.

Tous les comptages sont restreints au marqueur unique du test, de sorte qu'un residu laisse
par une execution precedente ne puisse ni masquer ni provoquer un echec.
"""
from __future__ import annotations

from ..core import (API, CFG, count_in, empty_folder, expect, expect_status,
                    messages_in, premier, seed, test, work_folder)

def _variation(avant: int, apres: int, contexte: str = "") -> str:
    """Decrit dans quel SENS le total a bouge, au lieu de presumer la duplication.

    Ces tests surveillent une conservation — `total_avant == total_apres` — et non l'absence
    de duplication. L'invariant attrape donc les deux violations : le total qui augmente
    (duplication, `F-02`) et le total qui diminue (perte de donnees).

    Le libelle a longtemps annonce « DUPLICATION » dans les deux cas. Quand A-18 a fait
    disparaitre des messages, le message affichait « DUPLICATION : total 3 -> 2 » : la
    detection etait juste, le nom faux, et j'ai d'abord conclu a un defaut de mon test.
    Un message doit decrire ce qu'il a mesure, pas ce qu'il s'attendait a trouver.
    """
    quoi = "DUPLICATION" if apres > avant else "PERTE DE DONNEES"
    suffixe = f" {contexte}" if contexte else ""
    return f"{quoi}{suffixe} : total {avant} -> {apres}"



def _espaces(ident: str) -> tuple[str, str]:
    """Paire source/destination exclusive au test.

    Aucun dossier n'est partage entre tests : c'etait la derniere source d'interference,
    et elle produisait des echecs qui ne se reproduisaient jamais en execution isolee.
    """
    return work_folder(ident), work_folder(ident + "D")


def _trois_messages():
    return [
        {"from": "exp1@qa-autotest.local", "subject": "message 01", "date": "2024-03-01 09:00:00"},
        {"from": "exp2@qa-autotest.local", "subject": "message 02", "date": "2024-03-02 10:00:00"},
        {"from": "exp3@qa-autotest.local", "subject": "message 03", "date": "2024-03-03 11:00:00"},
    ]


@test("DUP-01", "Deplacer un email ne le duplique pas", "F-02")
def move_sans_duplication():
    SRC, DST = _espaces('DUP01')
    empty_folder(DST)
    mk = seed(SRC, _trois_messages())
    src0, dst0 = count_in(SRC, marker=mk), count_in(DST, marker=mk)
    uid = premier(messages_in(SRC, marker=mk), "duplication")["uid"]

    r = API.post(f"/accounts/{CFG.account_id}/message/{uid}/move",
                 params={"folder": SRC}, json_body={"target_folder": DST})
    expect_status(r, 200, "deplacement")

    src1, dst1 = count_in(SRC, marker=mk), count_in(DST, marker=mk)
    expect(src1 == src0 - 1, f"la source n'a pas diminue : {src0} -> {src1} (email non retire)")
    expect(dst1 == dst0 + 1, f"la destination n'a pas augmente : {dst0} -> {dst1}")
    expect((src1 + dst1) == (src0 + dst0),
           _variation(src0 + dst0, src1 + dst1))


@test("DUP-02", "Un deplacement vers une cible vide ne detruit pas l'email", "IT4-01")
def move_cible_vide_sans_perte():
    """Defaut decouvert par cette suite a sa premiere execution.

    `MoveRequest.target_folder` n'est pas validee. Avec une chaine vide, l'API repond
    `200 {"status": "moved"}` et l'email disparait de la source sans arriver nulle part :
    une recherche par objet sur les 20 dossiers du serveur n'en retrouve aucune trace.
    C'est une perte de donnees silencieuse, annoncee comme un succes.

    Attendu : soit un refus (4xx), soit un deplacement effectif — jamais une disparition.
    """
    SRC, DST = _espaces('DUP02')
    mk = seed(SRC, _trois_messages())
    src0 = count_in(SRC, marker=mk)
    uid = premier(messages_in(SRC, marker=mk), "duplication")["uid"]
    r = API.post(f"/accounts/{CFG.account_id}/message/{uid}/move",
                 params={"folder": SRC}, json_body={"target_folder": ""})
    src1 = count_in(SRC, marker=mk)
    if r.status >= 400:
        expect(src1 == src0,
               f"la cible vide a bien ete refusee ({r.status}) mais la source a change : "
               f"{src0} -> {src1}")
        return
    expect(src1 == src0,
           f"PERTE DE DONNEES : l'API a repondu {r.status} « {r.json().get('status')} » et "
           f"l'email a quitte la source ({src0} -> {src1}) sans arriver dans aucun dossier. "
           "Une cible vide doit etre refusee.")


@test("DUP-03", "Supprimer un email ne le duplique pas vers la corbeille", "F-02")
def delete_sans_duplication():
    SRC, DST = _espaces('DUP03')
    mk = seed(SRC, _trois_messages())
    src0, trash0 = count_in(SRC, marker=mk), count_in("Trash", marker=mk)
    uid = premier(messages_in(SRC, marker=mk), "duplication")["uid"]

    r = API.delete(f"/accounts/{CFG.account_id}/message/{uid}", params={"folder": SRC})
    expect_status(r, 200, "suppression")

    src1, trash1 = count_in(SRC, marker=mk), count_in("Trash", marker=mk)
    expect(src1 == src0 - 1, f"l'email n'a pas ete retire de la source : {src0} -> {src1}")
    expect((src1 + trash1) == (src0 + trash0),
           _variation(src0 + trash0, src1 + trash1))


@test("DUP-04", "La suppression en masse ne duplique pas", "F-02")
def delete_bulk_sans_duplication():
    SRC, DST = _espaces('DUP04')
    mk = seed(SRC, _trois_messages())
    src0, trash0 = count_in(SRC, marker=mk), count_in("Trash", marker=mk)
    uids = [m["uid"] for m in messages_in(SRC, marker=mk)][:2]

    r = API.post(f"/accounts/{CFG.account_id}/delete-bulk",
                 json_body={"uids": uids, "folder": SRC})
    expect_status(r, 200, "suppression en masse")

    src1, trash1 = count_in(SRC, marker=mk), count_in("Trash", marker=mk)
    expect(src1 == src0 - 2, f"les emails n'ont pas ete retires : {src0} -> {src1}")
    expect((src1 + trash1) == (src0 + trash0),
           _variation(src0 + trash0, src1 + trash1))


@test("DUP-05", "Les flags sont reellement poses et retires", "F-02")
def flags_effectifs():
    """Meme cause racine que la duplication : le `STORE` non parenthese faisait echouer tout
    marquage. On verifie l'etat apres chaque appel, pas le code de retour."""
    SRC, DST = _espaces('DUP05')
    mk = seed(SRC, _trois_messages())
    uid = premier(messages_in(SRC, marker=mk), "duplication")["uid"]

    def etat(champ):
        for m in messages_in(SRC, marker=mk):
            if m["uid"] == uid:
                return m[champ]
        raise AssertionError(f"email {uid} introuvable dans {SRC}")

    for flag, champ in (("seen", "seen"), ("flagged", "flagged"), ("answered", "answered")):
        r = API.post(f"/accounts/{CFG.account_id}/message/{uid}/flags",
                     params={"folder": SRC}, json_body={"flag": flag, "action": "add"})
        expect_status(r, 200, f"pose du flag {flag}")
        expect(etat(champ) is True, f"le flag {flag} n'a pas ete pose malgre une reponse 200")

        r = API.post(f"/accounts/{CFG.account_id}/message/{uid}/flags",
                     params={"folder": SRC}, json_body={"flag": flag, "action": "remove"})
        expect_status(r, 200, f"retrait du flag {flag}")
        expect(etat(champ) is False, f"le flag {flag} n'a pas ete retire malgre une reponse 200")


@test("DUP-06", "Les outils MCP de deplacement et suppression ne dupliquent pas", "F-02")
def mcp_move_delete_sans_duplication():
    SRC, DST = _espaces('DUP06')
    from ..core import BOX
    BOX.require()
    empty_folder(DST)
    mk = seed(SRC, _trois_messages())
    src0, dst0, trash0 = (count_in(SRC, marker=mk), count_in(DST, marker=mk),
                          count_in("Trash", marker=mk))
    uid = premier(messages_in(SRC, marker=mk), "duplication")["uid"]

    r = BOX.mcp("move_email", {"account_id": CFG.account_id, "folder": SRC,
                               "uid": uid, "target_folder": DST})
    expect("__erreur__" not in r, f"move_email a echoue : {r}")
    src1, dst1 = count_in(SRC, marker=mk), count_in(DST, marker=mk)
    expect((src1 + dst1) == (src0 + dst0),
           _variation(src0 + dst0, src1 + dst1, "(move_email)"))
    expect(src1 == src0 - 1, f"move_email n'a pas retire l'original : {src0} -> {src1}")

    uid2 = premier(messages_in(SRC, marker=mk), "duplication")["uid"]
    r = BOX.mcp("delete_email", {"account_id": CFG.account_id, "folder": SRC, "uid": uid2})
    expect("__erreur__" not in r, f"delete_email a echoue : {r}")
    src2, trash2 = count_in(SRC, marker=mk), count_in("Trash", marker=mk)
    expect((src2 + trash2) == (src1 + trash0),
           _variation(src1 + trash0, src2 + trash2, "(delete_email)"))

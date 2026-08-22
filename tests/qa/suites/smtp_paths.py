"""Les six chemins d'envoi SMTP.

Historique : le meme defaut a resurgi trois fois — un `starttls()` inconditionnel, copie-colle
d'un appelant a l'autre, qui ignorait le drapeau `smtp_ssl` du compte. Un serveur SMTP en
clair devenait inutilisable, et seules certaines copies etaient corrigees a chaque passage
(F-03, N-02, N-03, puis auth.py au lot 3).

Les six appelants passent desormais par `src/smtp_client.py`. Le test le plus utile de cette
suite n'est donc pas fonctionnel mais structurel : SMTP-06 verifie qu'aucun `starttls()` ne
reapparait ailleurs. C'est ce qui empeche la quatrieme resurgence.

Le chemin « mot de passe oublie » n'est PAS declenche de bout en bout : `auth.py` choisit
comme expediteur le premier compte administrateur disposant d'un SMTP, sans `ORDER BY`. Sur
une instance de production, cela ferait envoyer un vrai message depuis la messagerie reelle
(constat IT3-02). On teste donc uniquement sa branche sans effet de bord.
"""
from __future__ import annotations

from pathlib import Path

from ..core import (API, BOX, CFG, Skip, count_in, expect, expect_status,
                    messages_in, premier, seed, test, work_folder)

REPO = Path(__file__).resolve().parents[3]


@test("SMTP-01", "test-credentials honore smtp_ssl=false", "N-02")
def credentials_smtp():
    r = API.post("/accounts/test-credentials", json_body={
        "imap_host": "greenmail", "imap_port": 3143, "imap_ssl": False,
        "imap_user": "test", "imap_password": "testpass123",
        "smtp_host": "greenmail", "smtp_port": 3025, "smtp_ssl": False,
        "test_type": "smtp",
    })
    expect_status(r, 200, "test-credentials")
    expect(r.json().get("status") == "ok",
           f"STARTTLS force malgre smtp_ssl=false : {r.json().get('message')}")


@test("SMTP-02", "test-smtp sur un compte existant", "F-03")
def smtp_du_compte():
    r = API.post(f"/accounts/{CFG.account_id}/test-smtp")
    expect_status(r, 200, "test-smtp")
    expect(r.json().get("status") == "ok", f"echec SMTP : {r.json().get('message')}")


@test("SMTP-03", "L'envoi via l'API aboutit et le message est livre", "")
def envoi_api():
    from ..core import unique
    sujet = f"QA-SMTP03 {unique()}"
    avant = count_in("INBOX")
    r = API.post(f"/accounts/{CFG.account_id}/send", json_body={
        "to": ["test@mailia.local"], "subject": sujet,
        "body_text": "corps de test", "body_html": "<p>corps</p>",
    })
    expect_status(r, 200, "envoi")
    import time
    for _ in range(10):
        time.sleep(0.8)
        if count_in("INBOX") > avant:
            break
    trouve = any(sujet in m["subject"] for m in messages_in("INBOX"))
    expect(trouve, "l'API a repondu « sent » mais le message n'est pas arrive")


@test("SMTP-04", "Une reponse marque l'original comme repondu", "F-11")
def reponse_marque_original():
    from ..core import unique
    dossier = work_folder("SMTP04")
    mk = seed(dossier, [{"from": "orig@qa-autotest.local",
                                "subject": f"QA-SMTP04 {unique()}",
                                "date": "2024-05-05 08:00:00"}])
    msg = premier(messages_in(dossier, marker=mk), "smtp_paths")
    uid = msg["uid"]
    detail = API.get(f"/accounts/{CFG.account_id}/message/{uid}",
                     params={"folder": dossier})
    expect_status(detail, 200, "lecture de l'original")
    mid = detail.json()["message_id"]

    r = API.post(f"/accounts/{CFG.account_id}/send", json_body={
        "to": ["test@mailia.local"], "subject": "Re: " + msg["subject"],
        "body_text": "ma reponse", "in_reply_to": mid, "references": mid,
        "reply_uid": uid, "reply_folder": dossier,
    })
    expect_status(r, 200, "envoi de la reponse")
    apres = [m for m in messages_in(dossier, marker=mk) if m["uid"] == uid]
    expect(apres, "l'original a disparu du dossier source")
    expect(apres[0]["answered"] is True,
           "l'original n'a pas ete marque comme repondu apres l'envoi de la reponse")


@test("SMTP-05", "L'action de regle `forward` transfere reellement", "F-10")
def action_forward():
    """`forward` figurait dans les actions valides mais n'etait pas implementee : la regle
    renvoyait `matched: 1, actions: []` sans rien faire."""
    from ..core import unique
    # Prerequis : l'envoi doit fonctionner. Sinon le moteur de regles rapporte `failed: 1`
    # sans en dire la cause, et le test accuse l'action `forward` d'un probleme de transport
    # — sur un serveur a certificat auto-signe, par exemple. Un test dont le prerequis n'est
    # pas rempli doit le dire, pas conclure.
    # `test-smtp` repond 200 meme en echec : c'est `status` qui porte le verdict, pas le code.
    sonde = (API.post(f"/accounts/{CFG.account_id}/test-smtp").json() or {})
    if sonde.get("status") == "error":
        expect(False, f"echec SMTP : {sonde.get('message', '')}")
    marqueur = unique("FWD")
    dossier = work_folder("SMTP05")
    seed(dossier, [{"from": "src@qa-autotest.local", "subject": f"QA-SMTP05 {marqueur}",
                    "date": "2024-06-06 08:00:00"}])
    # La regle ne peut matcher que ce message : `marqueur` est unique a cette execution.
    rule = API.post("/rules/classic/", json_body={
        "name": f"QA autotest forward {marqueur}",
        "conditions": [{"field": "subject", "operator": "contains", "value": marqueur}],
        "actions": [{"type": "forward", "target": "test@mailia.local"}],
    })
    expect_status(rule, 200, "creation de la regle")
    rid = rule.json()["id"]
    try:
        avant = count_in("INBOX")
        ap = API.post(f"/rules/classic/{rid}/apply",
                      json_body={"account_id": CFG.account_id, "folder": dossier})
        expect_status(ap, 200, "application de la regle")
        actions = ap.json().get("actions", [])
        expect(actions, "la regle a matche mais n'a execute aucune action (F-10)")
        fwd = [a for a in actions if a.get("type") == "forward"]
        expect(fwd and fwd[0].get("forwarded", 0) >= 1,
               f"l'action forward n'a rien transfere : {actions}")
        import time
        for _ in range(10):
            time.sleep(0.8)
            if count_in("INBOX") > avant:
                break
        expect(count_in("INBOX") > avant,
               "l'action forward annonce un transfert mais rien n'a ete livre")
    finally:
        API.delete(f"/rules/classic/{rid}")


@test("SMTP-06", "Aucun starttls() en dehors du client SMTP centralise", "F-03/N-02/N-03")
def starttls_centralise():
    """Test structurel. Le defaut a resurgi trois fois parce que le meme bloc etait
    copie-colle. Tant que `starttls()` n'existe qu'a un seul endroit, il ne peut plus
    diverger — et si quelqu'un le recopie, ce test tombe."""
    src = REPO / "src"
    if not src.exists():
        raise Skip("source indisponible depuis cette machine")
    coupables = []
    for f in src.rglob("*.py"):
        if f.name == "smtp_client.py":
            continue
        for i, ligne in enumerate(f.read_text(errors="replace").splitlines(), 1):
            if "starttls(" in ligne and not ligne.strip().startswith("#"):
                coupables.append(f"{f.relative_to(REPO)}:{i}")
    expect(not coupables,
           "un appel starttls() est reapparu hors de src/smtp_client.py — c'est exactement "
           "ainsi que le defaut a resurgi trois fois :\n      " + "\n      ".join(coupables))


@test("SMTP-07", "Tous les appelants SMTP transmettent le drapeau smtp_ssl", "F-03")
def appelants_transmettent_ssl():
    src = REPO / "src"
    if not src.exists():
        raise Skip("source indisponible depuis cette machine")
    import re
    # Le nom exact, sans capturer un eventuel wrapper local `_smtp_connect(` : c'est le
    # wrapper qui transmet le drapeau, pas ses appelants.
    appel = re.compile(r"(?<![_\w])smtp_connect\s*\(")
    manquants = []
    for f in src.rglob("*.py"):
        if f.name == "smtp_client.py":
            continue
        texte = f.read_text(errors="replace")
        for m in appel.finditer(texte):
            arg = texte[m.end():m.end() + 250]
            if "smtp_ssl" not in arg and "use_ssl" not in arg:
                ligne = texte[:m.start()].count("\n") + 1
                manquants.append(f"{f.relative_to(REPO)}:{ligne}")
    expect(not manquants,
           "des appelants de smtp_connect() ne transmettent pas le reglage TLS du compte, "
           "il retombera donc sur le defaut True :\n      " + "\n      ".join(manquants))


@test("SMTP-08", "Le mot de passe oublie ne divulgue pas l'existence d'un compte", "")
def forgot_password_anti_enumeration():
    """Seule branche testable sans effet de bord : une adresse inconnue sort avant tout acces
    SMTP. Le chemin d'envoi reel n'est volontairement pas declenche (voir l'en-tete)."""
    r = API.post("/auth/forgot-password", token="",
                 json_body={"email": "inconnu-qa-autotest@nulle.part"})
    expect_status(r, 200, "mot de passe oublie")
    expect(r.json().get("status") == "ok",
           "la reponse differe pour une adresse inconnue : enumeration de comptes possible")


@test("SMTP-09", "Les outils MCP d'envoi fonctionnent", "N-03")
def mcp_envois():
    BOX.require()
    r = BOX.mcp("test_smtp", {"account_id": CFG.account_id})
    expect("__erreur__" not in r and r.get("status") == "ok",
           f"MCP test_smtp en echec (STARTTLS force ?) : {r}")
    from ..core import unique
    s = BOX.mcp("send_email", {"account_id": CFG.account_id, "to": ["test@mailia.local"],
                               "subject": f"QA-SMTP09 {unique()}", "body": "corps mcp"})
    expect("__erreur__" not in s and s.get("status") == "sent",
           f"MCP send_email en echec : {s}")

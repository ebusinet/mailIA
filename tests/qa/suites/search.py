"""Recherche plein texte et recherche multi-dossiers.

Historique :
- F-01 : `indexer.py` impose un tri par date sans `track_scores`, donc Elasticsearch renvoie
  `_score: null`. Le modele Pydantic declarait `score: float` et `hit.get("_score", 0)`
  retournait `None` (la cle existe avec la valeur null) : toute recherche renvoyant au moins
  un resultat echouait en 500. Seule une recherche sans resultat repondait 200.
- F-06 : sur un index Elasticsearch absent (utilisateur jamais synchronise), la route levait
  une `NotFoundError` non interceptee, donc un 500 au lieu d'un resultat vide.
- FE-01 : les fragments de surlignage contiennent le corps de l'email et etaient injectes
  sans echappement dans la page. Le backend les renvoie toujours bruts : la defense est
  cote client. On verifie ici que l'API les expose bien tels quels, pour que la disparition
  de `escHighlight()` cote frontend ne passe pas inapercue.
"""
from __future__ import annotations

from ..core import API, CFG, Skip, expect, expect_status, seed, test



@test("SRCH-01", "Une recherche avec resultats renvoie 200", "F-01")
def recherche_avec_resultats():
    r = API.get("/search/", params={"q": ""})
    expect_status(r, 200, "recherche vide (tout l'index)")
    total = r.json()["total"]
    if total == 0:
        raise Skip("index Elasticsearch vide pour cet utilisateur : lancer une synchro d'abord")
    expect(len(r.json()["results"]) > 0, "total > 0 mais aucun resultat renvoye")


@test("SRCH-02", "Le champ `score` est toujours un nombre, jamais null", "F-01")
def score_jamais_null():
    """La cause racine de F-01. Un `score` null refait echouer la validation Pydantic."""
    r = API.get("/search/", params={"q": ""})
    expect_status(r, 200, "recherche")
    res = r.json()["results"]
    if not res:
        raise Skip("index vide")
    mauvais = [x for x in res if not isinstance(x.get("score"), (int, float))]
    expect(not mauvais,
           f"{len(mauvais)} resultat(s) ont un score non numerique : "
           f"{[x.get('score') for x in mauvais[:3]]}")


@test("SRCH-03", "Une recherche sans resultat renvoie 200 et une liste vide", "F-01")
def recherche_sans_resultat():
    r = API.get("/search/", params={"q": "zzz_terme_qui_n_existe_pas_qa_autotest"})
    expect_status(r, 200, "recherche sans resultat")
    expect(r.json()["total"] == 0, "un terme improbable a renvoye des resultats")


@test("SRCH-04", "Les filtres de recherche repondent tous en 200", "F-01")
def filtres_recherche():
    filtres = [
        {"q": "", "has_attachments": "true"},
        {"q": "", "date_from": "2020-01-01", "date_to": "2030-01-01"},
        {"q": "", "folder": "INBOX"},
        {"q": "", "size": 3, "page": 1},
        {"q": "", "account_id": CFG.account_id},
    ]
    echecs = []
    for f in filtres:
        r = API.get("/search/", params=f)
        if r.status != 200:
            echecs.append(f"{f} -> {r.status} {r.detail[:70]}")
    expect(not echecs, "filtres en echec :\n      " + "\n      ".join(echecs))


@test("SRCH-05", "Un index Elasticsearch absent renvoie un resultat vide, pas une erreur", "F-06")
def index_absent():
    """Impossible a provoquer sans creer un utilisateur jamais synchronise. On verifie a
    defaut que la route intercepte bien NotFoundError."""
    from pathlib import Path
    src = Path(__file__).resolve().parents[3] / "src" / "api" / "routes" / "search.py"
    if not src.exists():
        raise Skip("source indisponible depuis cette machine")
    texte = src.read_text()
    expect("NotFoundError" in texte and "except NotFoundError" in texte,
           "la route de recherche n'intercepte plus NotFoundError : un utilisateur jamais "
           "synchronise obtiendra un 500 (F-06)")


@test("SRCH-06", "La recherche multi-dossiers fonctionne et signale les dossiers en erreur", "")
def recherche_multi_dossiers():
    r = API.post(f"/accounts/{CFG.account_id}/search-multi",
                 json_body={"q": "a", "folders": ["INBOX", "__QA_INEXISTANT__"]})
    expect_status(r, 200, "recherche multi-dossiers")
    d = r.json()
    expect("messages" in d and "errors" in d, f"structure de reponse inattendue : {list(d)}")
    expect(any("__QA_INEXISTANT__" in str(e) for e in d["errors"]),
           "un dossier inexistant n'est pas signale dans `errors`")


@test("SRCH-07", "Les fragments de surlignage restent bruts cote API (defense cote client)", "FE-01")
def surlignage_brut_documente():
    """Le correctif de FE-01 est cote frontend (`escHighlight`). Si un jour le backend se met
    a echapper, tant mieux. Ce test verifie surtout que la fonction d'echappement existe
    toujours cote client : c'est elle qui protege."""
    from pathlib import Path
    front = Path(__file__).resolve().parents[3] / "src" / "web" / "static" / "index.html"
    if not front.exists():
        raise Skip("frontend indisponible depuis cette machine")
    texte = front.read_text(errors="replace")
    expect("function escHighlight" in texte,
           "la fonction escHighlight() a disparu du frontend : les fragments de surlignage, "
           "qui contiennent le corps des emails, redeviennent une XSS stockee (FE-01)")
    expect("highlight.body.map(escHighlight)" in texte or "escHighlight)" in texte,
           "escHighlight() existe mais n'est plus appliquee aux fragments de surlignage")

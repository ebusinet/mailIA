# Rapport de tests — Itération 2

**Date** : 2026-08-22
**Périmètre** : A. non-régression des 17 correctifs + des 58 PASS de l'itération 1 — B. surface MCP (67 outils) — C. vérification du correctif FE-01
**Utilisateur QA** : id=5 / `qa@mailia.local` — compte mail `account_id=3` (QA GreenMail)

## Intégrité du compte réel

Vérifié en base en fin de campagne : `account_id=1` (utilisateur 2) **inchangé** —
`sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51`, **1227** emails locaux (identique aux deux
relevés précédents). Aucun accès en écriture, aucune connexion IMAP vers ce compte cette fois-ci
(`/admin/status`, seul endpoint qui en ouvrait une en lecture seule, n'a pas été rejoué).

Garde-fou MCP vérifié avant tout test : `list_folders(account_id=1)` →
`Account 1 not found for user 5`, et `list_accounts` ne renvoie que le compte 3.

---

## Synthèse

| Catégorie | Résultat |
|---|---|
| Correctifs confirmés | **17 / 17** |
| Correctifs incomplets | **1** (F-05 — voir N-01) |
| Régressions sur les 58 PASS de l'itération 1 | **0** |
| Nouvelle régression introduite par un correctif | **1** (N-01) |
| Nouveaux bugs découverts sur la surface MCP | **16** (dont 1 faille de cloisonnement, 1 outil totalement inopérant) |
| Outils MCP : PASS / FAIL | **61 / 6** (couverture 67/67) |

**Les trois points à retenir** :

1. **Les 17 correctifs tiennent**, y compris les plus délicats — F-00 (cloisonnement), F-02
   (aucune duplication, vérifiée par comptage), FE-01 (XSS neutralisée).
2. **Deux familles de bugs n'ont été corrigées qu'à moitié.** Le bug `smtp_ssl` ignoré subsiste
   dans 2 des 4 points d'appel SMTP (N-02, N-03) — il rend inutilisables les 4 outils d'envoi MCP.
   Le cloisonnement F-00 a été corrigé côté API mais pas dans le serveur MCP (N-04).
3. **Le correctif F-05 a introduit une régression** : la colonne date est désormais *affichée*
   depuis l'en-tête `Date:` mais reste *triée* et *filtrée* sur l'INTERNALDATE (N-01).

---

# A. Non-régression

## A.1 — Les 17 correctifs

| # | Bug d'origine | Statut | Preuve |
|---|---|---|---|
| F-00 | IDOR sur `storage=local` | **CONFIRMÉ** | Sonde sur compte 99999 : les **8** endpoints renvoient désormais `Account not found` (contre `Local folder not found` / `Email not found` avant). Détail ci-dessous. |
| F-01 | Recherche 500 dès 1 résultat | **CONFIRMÉ** | `q=devis` → 200, `total=6` ; `q=` → 200, `total=65`. `score=hit.get("_score") or 0` (`search.py:70`). |
| F-02 | Flags KO, déplacement/suppression dupliquant | **CONFIRMÉ** | 6 combinaisons de flags OK ; 3 opérations destructives avec **delta total = 0**. Détail ci-dessous. |
| F-03 | `smtp_ssl` / `sync_enabled` ignorés à la création | **CONFIRMÉ** | Compte créé avec `smtp_ssl:false, sync_enabled:false` → renvoyés `false`/`false` ; `test-smtp` sur ce compte neuf → `ok`. |
| F-04 | `filter_from` ne cherchait que le nom affiché | **CONFIRMÉ** | `filter_from=alice@acme-corp.example` → 2 résultats (0 avant). Les valeurs partielles (`acme-corp`, `Alice`) renvoient 0 **à cause de GreenMail** — re-vérifié en imaplib brut : `SEARCH FROM "acme-corp"` → vide, `SEARCH FROM "alice@acme-corp.example"` → `17 33`. |
| F-05 | Date liste ≠ date détail | **CONFIRMÉ mais incomplet** | Liste et détail concordent (`2026-08-20 09:15`). Mais tri et filtre restent sur l'INTERNALDATE → **N-01**. |
| F-06 | Index ES absent → 500 | **CONFIRMÉ** | `except NotFoundError → SearchResponse(total=0, results=[])` (`search.py:74-76`). |
| F-07 | Déplacement IMAP→local en 500 | **CONFIRMÉ** | uid 38 déplacé vers `IT2-Local` : présent en local avec sa date d'origine `2026-08-17 11:00`, absent d'INBOX. |
| F-08 | Retour local→IMAP écrasant la date | **CONFIRMÉ** | `L7473` remonté vers `QAIter2Dst` : date affichée `2026-08-17 11:00` (date d'origine, pas celle du déplacement). |
| F-09 | Règles sur `to` multi-destinataires | **CONFIRMÉ** | `to contains premier@qa.example` → 1, `to contains second@qa.example` → 1 (0 et 0 avant). |
| F-10 | Action `forward` jamais exécutée | **CONFIRMÉ** | `{"type":"forward","forwarded":1,"failed":0}` **et effet réel** : INBOX 11 → 12, le message transféré est bien arrivé. |
| F-11 | Réponse ne marquant pas l'original | **CONFIRMÉ** | uid 41 : `answered=False` → envoi avec `reply_uid`/`reply_folder` → `answered=True`. |
| F-12 | `signature_id: null` sans effet | **CONFIRMÉ** | `PUT {"signature_id":null}` → `signature_id: None` (restait `6` avant). |
| F-13 | Dossier cible mis en minuscules | **CONFIRMÉ** | Preview : `{"type":"move","target":"Comptabilite"}` (casse préservée). |
| F-14 | `transferer` / `marquer important` non reconnus | **CONFIRMÉ** | Preview : `[{"type":"flag","target":"important"},{"type":"forward","target":"boss@qa.example"}]`. |
| F-15 | `/ai/chat` en 500 brut | **CONFIRMÉ** | Provider injoignable → `502 {"detail":"AI provider error: Connection error."}` ; `provider_id=999999` → `404 {"detail":"Provider 999999 not found"}`. |
| F-16 | `claude-native` envoyant `Bearer ` vide | **CONFIRMÉ** | Plus d'`Illegal header value`. La requête part et reçoit un `401 Invalid API key` du proxy — c'est le problème connu d'authentification du CLI, distinct et hors périmètre. |

### F-00 — détail

Sonde sur un compte inexistant (aucune donnée réelle sollicitée) :

```
GET    /accounts/99999/messages?folder=INBOX                          → 404 Account not found
GET    /accounts/99999/messages?folder=X&storage=local                → 404 Account not found
GET    /accounts/99999/message/L999999?folder=X&storage=local         → 404 Account not found
GET    /accounts/99999/message/1/attachment/0?folder=INBOX            → 404 Account not found
POST   /accounts/99999/message/L999999/flags?storage=local            → 404 Account not found
POST   /accounts/99999/message/L999999/move?storage=local&target_storage=local → 404 Account not found
DELETE /accounts/99999/message/L999999?folder=X&storage=local         → 404 Account not found
POST   /accounts/99999/delete-bulk?storage=local                      → 404 Account not found
```

Et sur le compte réel : `GET /accounts/1/messages?folder=Archives&storage=local` → `404 Account not found`.
Le message d'erreur est désormais uniforme : la vérification de propriété précède bien tout
embranchement `storage`.

### F-02 — détail : comptages avant / après

| Opération | Source avant → après | Destination avant → après | Δ total |
|---|---|---|---|
| `move` INBOX → QAIter2Dst | 16 → 15 | 0 → 1 | **0** |
| `DELETE /message/{uid}` | INBOX 15 → 14 | Trash 9 → 10 | **0** |
| `delete-bulk` (2 emails) | INBOX 14 → 12 | Trash 10 → 12 | **0** |

Flags : `seen` add/remove, `flagged` add/remove, `answered` add/remove — les 6 renvoient
`{"status":"ok"}` en 200 et l'état est vérifié dans la liste après chaque appel. Aucune duplication,
aucune perte.

## A.2 — Les 58 PASS de l'itération 1

**Aucune régression détectée.** Points re-vérifiés, tous PASS :

- **En-têtes de sécurité** : les 6 présents (HSTS, X-Frame-Options DENY, nosniff, XSS, Referrer-Policy, Permissions-Policy).
- **Comptes** : liste, `test-credentials` (IMAP), `test-imap`, `test-smtp`, création, modification, suppression.
- **Dossiers** : `folders`, `folders-raw`, `folders-counts`, création, création accentuée, renommage, refus sur dossier système, `empty-folder` (2 supprimés), refus de suppression d'un dossier système.
- **Emails** : liste, pagination, tris date/from/subject, filtres `subject`/`date`/`attachments`/`spam`/`replied`, recherche IMAP `q=`, lecture, `tech_headers`, téléchargement de PJ, 404 sur email et index de PJ inexistants.
- **Spam** : scan (14 scannés, 1 détecté, score 28.0 avec `x_spam_flag`, `lottery_scam`, `urgency`, `excessive_exclamation`), `filter_spam` (1), liste blanche masquant effectivement le spam (1 → 0), liste noire, suppressions par id et par expéditeur. *(Le premier scan avait renvoyé 0 : simple artefact — plus aucun spam en INBOX après mes tests de suppression. Confirmé par réinjection d'un spam frais.)*
- **Règles classiques** : CRUD, validation (champ / opérateur / conditions vides / actions vides → 422), et les 13 opérateurs re-testés : `contains` 2, `not_contains` 7, `equals` 2, `domain_is` 2, `starts_with` 5, `ends_with` 3, `has_attachments` 2, `is_spam` 1, `is_reply` 12, `size greater_than` 1, `age older_than` 1, `cc contains` 1, mode `any` 3. **Aucune erreur d'exécution d'action** — contrairement à l'itération 1 où toutes échouaient sur le `STORE`. *(`cc` avait d'abord renvoyé 0 : les emails porteurs d'un Cc avaient été consommés par mes tests destructifs. Confirmé par réinjection.)*
- **Règles IA** : CRUD, preview/parsing.
- **Contacts / groupes / signatures** : listes, autocomplétion, résolution de signature.
- **Administration** : `settings`, `users`, `info`, refus d'auto-retrait des droits admin.
- **Sécurité** : sans token → 403 sur 4 endpoints ; token invalide → 401 ; `register` sans admin → 403 ; cloisonnement compte 1 → 404 ; **rate limiting login** → 429 au 6ᵉ essai (5/min, conforme).
- **Import / export** : export ZIP (15 `.eml`), import mbox → local (3/3), import ZIP → IMAP avec dédoublonnage (5 skipped / 0 importés), suivi de job, historique.
- **`search-multi`** : 5 résultats sur 2 dossiers.
- **Digest** : 502 propre quand l'IA est indisponible, 422 sur `days=99`.

## A.3 — Vérification de FE-01 (XSS)

**Correctif confirmé, avec une réserve d'architecture.**

J'ai envoyé un email piégé (`IT2-09 XSSPROBE`, expéditeur `mallory@evil.example`) dont le corps
`text/plain` contient :
`Bonjour, voici le XSSPROBE contenu: <img src=x onerror="alert(document.domain)"> et <script>alert(1)</script> fin.`

**Réponse de l'API après synchronisation** — `GET /api/search/?q=XSSPROBE` :

```json
"highlight": {
  "body": ["Bonjour, voici le <em>XSSPROBE</em> contenu: <img src=x onerror=\"alert(document.domain)\"> et <script>alert(1)</script> fin."]
}
```

Le backend renvoie donc **toujours la charge brute** : l'option `encoder: "html"` n'a pas été
ajoutée à `indexer.py`. Toute la défense repose sur le frontend.

**Analyse du correctif frontend** — `index.html:7602` :

```javascript
function escHighlight(s) { return esc(s).replace(/&lt;em&gt;/g, '<em>').replace(/&lt;\/em&gt;/g, '</em>'); }
```

appliqué en `index.html:2183` via `r.highlight.body.map(escHighlight)`, et `r.date` / `r.folder`
du même bloc désormais passés par `esc()` (ligne 2181).

**Le correctif est correct.** J'ai cherché un contournement sans en trouver :

| Tentative | Résultat |
|---|---|
| `<img src=x onerror=...>` | `esc()` échappe le `<` → `&lt;img …&gt;`, inerte |
| `<script>alert(1)</script>` | idem, inerte |
| Injecter un `<em>` porteur d'attributs | Impossible : la restauration ne reconnaît que les séquences exactes `&lt;em&gt;` / `&lt;/em&gt;` et produit exactement `<em>` / `</em>`, sans attribut |
| Écrire littéralement `<em>` dans le corps | Produit un `<em>` réel dans la page — balise sans attribut, inerte |
| Double encodage : écrire littéralement `&lt;em&gt;` | `esc()` échappe d'abord le `&` → `&amp;lt;em&amp;gt;`, la regex ne matche plus |

Le fragment est rendu en contenu d'élément (et non dans un attribut), donc l'absence
d'échappement des guillemets par `esc()` est sans conséquence ici.

**Réserve** : la protection est purement côté client et repose sur un unique appel. Un futur
point de rendu qui oublierait `escHighlight` rouvrirait la faille — le champ
`attachment_content`, également surligné (`indexer.py:207`) et alimenté par le contenu de PDF
extrait par Tika, est le candidat le plus proche. Ajouter `"encoder": "html"` côté backend
donnerait une défense en profondeur pour un coût nul.

---

# B. Surface MCP — 67 outils

Testée intégralement via `/app/mcp_runner.py` (cadré `USER_ID=5`).
**61 PASS / 6 FAIL / 0 SKIP.** Couverture 67/67.

## B.1 — Aucune duplication sur les opérations destructives

Le correctif F-02 tient aussi dans le MCP. `move_email` (`imap/manager.py:241-263`) et
`move_emails_bulk` (l. 265-305) implémentent en plus un **rollback de la copie** via COPYUID si le
`STORE` échoue, ce qui rend une copie orpheline impossible par construction.

| Opération | Source avant → après | Destination avant → après | Δ |
|---|---|---|---|
| `move_email` | MCPSrc 6 → 5 | MCPDst 0 → 1 | 0 |
| `move_emails_bulk` (2 msgs, 2 cibles) | MCPSrc 5 → 3 | MCPDst 1→2, MCPArch 0→1 | 0 |
| `search_and_move_emails` (4 msgs) | MCPSrc 7 → 3 | MCPDst 2 → 6 | 0 |
| `organize_emails` (1 msg) | MCPSrc 2 → 1 | MCPArch 1 → 2 | 0 |
| `archive_email` | MCPSrc 3 → 2 | Archive 0 → 1 | 0 |
| `delete_email` | MCPSrc 2 → 1 | Trash 9 → 10 | 0 |
| `delete_emails_bulk` | MCPSrc 1 → 0 | Trash 10 → 11 | 0 |
| `search_and_delete_emails` | MCPSrc 3 → 2 | Trash 11 → 12 | 0 |
| `update_draft` | Drafts UID 7 supprimé | UID 8 créé (2 → 2) | 0 |

Bilan de contrôle : 18 messages injectés, 2 détruits, 3 envoyés en Trash → 13 attendus, **13
constatés**. Zéro duplication, zéro perte. Cas limite de l'auto-déplacement (MCPSrc → MCPSrc)
testé : recopie puis suppression de l'original, total inchangé.

## B.2 — Les 6 FAIL

### N-03 — MAJEUR : STARTTLS forcé dans le MCP, les 4 outils d'envoi sont inutilisables

`src/mcp/server.py:2159-2168` :

```python
def _smtp_connect(account):
    port = account.smtp_port or 465
    use_implicit_ssl = port == 465 or (account.smtp_ssl and port != 587)
    if use_implicit_ssl:
        return smtplib.SMTP_SSL(account.smtp_host, port, timeout=30)
    else:
        server = smtplib.SMTP(account.smtp_host, port, timeout=30)
        server.starttls(context=ssl_mod.create_default_context())   # ligne 2167 — inconditionnel
        return server
```

**C'est exactement le bug corrigé dans l'API, non répercuté dans le MCP.** Comparaison
côte à côte — `src/api/routes/accounts.py:298` et `:1865` sont bien gardés :

```python
server = smtplib.SMTP(...)
if smtp_ssl:                                        # <-- la garde manquante côté MCP
    server.starttls(context=ssl_mod.create_default_context())
```

Reproduction :
```
{"tool":"test_smtp","args":{"account_id":3}}
→ {"status": "error", "message": "STARTTLS extension not supported by server."}
```
alors que `POST /api/accounts/3/test-smtp` sur le **même compte** renvoie
`{"status":"ok","message":"Connexion SMTP reussie (greenmail:3025)"}`.

Impact : `test_smtp`, `send_email`, `reply_to_email` et `forward_email` sont inopérants pour tout
compte en SMTP clair. **Correctif attendu** : reprendre la garde `if account.smtp_ssl:`, et
idéalement tester `server.has_extn('starttls')`.

### N-02 — MOYEN : même bug dans `POST /accounts/test-credentials`

`src/api/routes/accounts.py:212`, branche `test_type="smtp"` :

```python
if req.smtp_port in (465,):
    server = smtplib.SMTP_SSL(...)
else:
    server = smtplib.SMTP(...)
    server.starttls(context=ssl_mod.create_default_context())   # inconditionnel
```

Aggravant : le schéma `TestCredentials` (`accounts.py:180-189`) **ne comporte aucun champ
`smtp_ssl`** — l'information n'est même pas transmissible. Reproduction :

```
POST /api/accounts/test-credentials  {"…","smtp_port":3025,"smtp_ssl":false,"test_type":"smtp"}
→ {"status":"error","message":"STARTTLS extension not supported by server."}
```

Conséquence concrète : au moment d'ajouter un compte, l'utilisateur qui teste ses identifiants SMTP
obtient un échec, alors que le compte fonctionnera une fois créé (`send` étant corrigé). Le
diagnostic contredit la réalité.

**Bilan de la famille `smtp_ssl` : 2 des 4 points d'appel corrigés.** Le grep de contrôle est
simple : `grep -n "starttls" src/api/routes/accounts.py src/mcp/server.py` → 4 occurrences, 2
gardées par `if smtp_ssl:`, 2 non gardées.

### N-04 — MAJEUR : cloisonnement absent dans `move_local_email` (famille F-00, MCP)

`src/mcp/server.py:3054-3058` :

```python
await get_account(db, user_id, account_id)                    # le COMPTE est bien validé
email = (await db.execute(
    select(LocalEmail).where(LocalEmail.id == email_id)       # l'EMAIL ne l'est pas
)).scalar_one_or_none()
target = (await db.execute(
    select(LocalFolder).where(LocalFolder.account_id == account_id, ...)
)).scalar_one_or_none()
email.folder_id = target.id
```

La vérification de propriété porte sur le compte **et sur le dossier cible**, tous deux fournis par
l'appelant et donc légitimement les siens. Mais `email_id` est repris tel quel, sans jointure
`LocalEmail → LocalFolder → MailAccount`. Un utilisateur peut donc déplacer l'email local d'un
**autre utilisateur** vers son propre dossier — puis le lire normalement, `read_local_email`
résolvant les droits par le dossier, désormais le sien.

C'est exactement la famille F-00, corrigée côté API mais pas dans le serveur MCP.
`read_local_email` (`server.py:2895-2907`) fait au contraire la résolution complète et correcte —
l'asymétrie confirme l'oubli.

**Non exploité** : la démonstration aurait exigé de manipuler les données locales de l'utilisateur 2
(1227 emails). Constat par lecture de code, corroboré par le contraste avec `read_local_email`.

### N-05 — MAJEUR : `semantic_search` ne peut structurellement pas fonctionner

```
{"tool":"semantic_search","args":{"query":"reunion avec le client","size":3}}
→ BadRequestError(400, 'search_phase_execution_exception',
   'the query vector has a different dimension [384] than the index vectors [768]')
```

Origine du vecteur — c'est un **vrai modèle**, pas un vecteur factice. `src/mcp/server.py:281-285` :

```python
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")
embedding = model.encode(query).tolist()
```

Trois défauts cumulés :

1. **Dimensions incompatibles** — `all-MiniLM-L6-v2` produit 384 dimensions ; le mapping ES déclare
   `"dims": 768` (`src/search/indexer.py:57-59`). Toute requête est rejetée en 400.
2. **Aucun document n'a jamais d'embedding — le point réellement bloquant.** `index_email()` accepte
   un paramètre `embedding` (`indexer.py:76`, appliqué l. 100-101) mais **aucun appelant ne le
   renseigne** ; le chemin réellement utilisé, `bulk_index_emails()` (`indexer.py:105-140`), ne
   construit même pas le champ. Vérifié côté serveur :
   `POST /mailia-5/_count {"query":{"exists":{"field":"embedding"}}}` → `{"count":0}`.
   Même en alignant les dimensions, le kNN renverrait systématiquement 0 résultat.
3. **Modèle rechargé à chaque appel** — `SentenceTransformer(...)` est instancié dans le corps de
   l'outil, sans cache : chargement complet du modèle à chaque requête.

En l'état, `semantic_search` est du code mort exposé comme un outil fonctionnel. La documentation
(§4) le présente pourtant comme une fonctionnalité disponible.

### N-06 — MOYEN : `reply_to_email` renseigne `In-Reply-To` avec l'UID IMAP

`src/mcp/server.py:2256` : `msg["In-Reply-To"] = uid`

Attendu : le `Message-ID` de l'email d'origine, pas l'UID IMAP (`"1"`), qui n'a aucune signification
hors du dossier courant. Aucun en-tête `References` n'est posé non plus. Les réponses émises par
MailIA ne se rattacheront donc jamais au fil chez le destinataire — et `get_thread`
(`server.py:1346-1374`), qui s'appuie précisément sur ces en-têtes, ne les retrouvera pas.

Non vérifiable par envoi réel (bloqué par N-03) ; lisible sans ambiguïté dans le code.

### N-07 — MINEUR : `reply_to_email` et `forward_email` remontent des exceptions brutes

`send_email` enveloppe correctement (`server.py:2217-2218` : `except Exception as e: raise ToolError(f"SMTP error: {e}")`).
`reply_to_email` (`server.py:2262-2270`) et `forward_email` n'ont qu'un `try/finally` sans `except` :
l'exception SMTP remonte brute (`SMTPNotSupportedError`) au client MCP. Trois outils du même groupe,
trois comportements différents.

### N-08 — MINEUR : `contact_from_email` prend les dates pour des numéros de téléphone

`src/mcp/server.py:2139` : `phones = re.findall(r'[\+]?[\d\s\-\.]{8,15}', sig_text)`

Sur un corps contenant « Rendez-vous le 2026-09-15 a 14h00 au bureau. » et **aucun numéro** :
```
→ {"contact": {..., "phones": ["2026-09-15"], "urls": []}}
```
La classe de caractères accepte toute suite de 8 à 15 chiffres/espaces/tirets/points : dates ISO,
montants, numéros de facture, références. Attendu : au minimum une frontière de mot et un seuil de
chiffres (≥ 9).

## B.3 — Défauts secondaires relevés sur des outils PASS

| # | Constat | Emplacement |
|---|---|---|
| N-09 | **`delete_folder` ne purge pas Elasticsearch.** Contrairement à `delete_email` / `delete_emails_bulk` / `search_and_delete_emails` qui appellent tous `_es_delete_docs`, il expurge en IMAP sans toucher l'index. Constaté : `get_folders_stats` annonce encore `MCPDst: 6, MCPArch: 2, MCPSrc: 1` sur des dossiers vides. Les documents fantômes polluent `search_emails`, `count_emails` et `get_folders_stats`. | `server.py:1086-1128` |
| N-10 | **`copy_local_to_imap` n'indexe pas dans ES** : après upload, `count_emails(folder=MCPDst)` = 6 contre 7 en IMAP. | — |
| N-11 | **Scores anti-spam contradictoires** : sur le même UID, `spam_analysis` renvoie `1/4` et `scan_for_spam` `0/4`. Le premier accorde +1 si `warnings` est vide, le second si `reasons` est vide — mais y a préalablement poussé « No DKIM signature ». | `server.py:1903-1908` vs `2011-2013` |
| N-12 | **`scan_for_spam` trie sur une chaîne** : `sort(key=lambda x: x["trust_score"])` trie `"0/4"`, `"1/4"`… lexicographiquement. Fonctionne par accident sur un barème mono-chiffre. | `server.py:2028` |
| N-13 | **`save_draft` / `update_draft` ne posent pas d'en-tête `Date`** : `list_drafts` renvoie `"date": ""` pour les brouillons créés par MCP. | — |
| N-14 | **Les 5 outils IA remontent une exception brute** (`RuntimeError: No AI provider configured…`) sans enveloppe `ToolError`. Message intelligible, mais type incohérent avec le reste du serveur. | — |
| N-15 | **`read_email` sur dossier inexistant** : `command FETCH illegal in state AUTH` au lieu d'un « dossier introuvable ». Le `SELECT` échoué n'est pas testé avant le `FETCH`. | — |
| N-16 | **`extract_calendar_events` : docstring trompeur** — annonce « from attachments **or body** », mais l'implémentation ne parcourt que les parties MIME `text/calendar` / `application/ics`. | `server.py:2067-2088` |
| N-17 | **`list_folders` : séparateur incohérent** — renvoie `"separator": "."` alors que la hiérarchie réelle utilise `/` (`Archives/Clients`, `MCPArch/Sub`). | — |

## B.4 — Comblement des SKIP de l'itération 1

| SKIP iter. 1 | Résultat |
|---|---|
| S-05 outils MCP | **Comblé** — 67/67 testés. |
| S-06 recherche sémantique | **Comblé** — testée, **inopérante** (N-05). |
| S-07 brouillons update/list/delete | **Comblé** — `save_draft`, `list_drafts`, `update_draft`, `delete_draft` : cycle complet PASS, `update_draft` sans doublon (UID 7 supprimé, UID 8 créé). Reste le défaut N-13 (en-tête `Date` absent). Ces opérations n'existent **que** dans le MCP, pas dans l'API HTTP. |

---

# C. Régression introduite par un correctif

## N-01 — MAJEUR : le correctif F-05 désolidarise l'affichage du tri et du filtre

Le correctif change **uniquement l'affichage** (`accounts.py:1358-1373`, « Prefer the Date: header —
same source as the message detail ») et laisse inchangés :

- le **tri** — `accounts.py:1289` : `uid_dates.sort(key=lambda x: _parse_idate(x[1]), reverse=True)` sur l'INTERNALDATE ;
- le **filtre** — `accounts.py:1292-1296` : `if fd_lower in _parse_idate(idate).strftime(...)` sur l'INTERNALDATE.

La colonne « date » a donc désormais **trois sources différentes selon l'opération** :
affichage = en-tête `Date:`, tri = INTERNALDATE, filtre = INTERNALDATE.

### Preuve 1 — rupture visible du tri

Message uid 42, produit par le test de l'action `forward` : en-tête `Date: Sat, 15 Aug 2026 09:00:00`,
INTERNALDATE `22-Aug-2026 01:14:51` (instant de la livraison), les deux relevés en IMAP direct.

Liste INBOX triée par date décroissante :

```
  42  2026-08-15 09:00  IT2-08 Multi destinataires
  30  2026-08-22 00:23  Re: QA01 Devis annuel Acme      <-- plus récent que la ligne au-dessus
  32  2026-08-21 23:51  Rapport avec piece jointe
  ...
```

La première ligne affiche une date plus ancienne que la deuxième. Pour l'utilisateur, la liste
« triée par date » ne l'est visiblement pas.

### Preuve 2 — le filtre ne trouve pas ce qu'il affiche

```
filter_date=2026-08-15  → total=1  [('40', '2026-08-15 09:00')]        # uid 42 ABSENT
filter_date=2026-08-22  → total=2  [('42', '2026-08-15 09:00'), ('30', '2026-08-22 00:23')]
```

Filtrer sur la date affichée pour uid 42 (`2026-08-15`) ne le renvoie pas ; il n'apparaît qu'en
filtrant sur `2026-08-22`, une date qui n'est affichée nulle part. Le filtre de colonne est
inutilisable dès que les deux dates divergent.

### Portée

Tout email dont l'INTERNALDATE diverge de l'en-tête `Date:` : import mbox, restauration depuis le
stockage local, copie inter-comptes, transfert automatique par une règle, et tout message dont
l'expéditeur a posé une date incorrecte. Ce sont précisément les cas que F-05 visait à corriger.

**Correctif attendu** : aligner les trois sur l'en-tête `Date:`. Cela impose de récupérer l'en-tête
`DATE` pour l'ensemble des UID avant de trier — c'est déjà ce que fait le tri par `from`/`subject`
(`accounts.py:1300-1323`, `FETCH BODY.PEEK[HEADER.FIELDS (…)]` par tranches de 200), le mécanisme
existe donc et peut être réutilisé. À défaut, replacer l'affichage sur l'INTERNALDATE et vivre avec
l'écart liste/détail — mais alors F-05 n'est pas corrigé.

---

# D. Précisions et corrections apportées à mes rapports précédents

- **Hypothèse de l'itération 1 invalidée** : j'avais supposé que l'échec `DELETE => socket error: EOF`
  de GreenMail était lié au **renommage** du dossier. C'est faux : `IT2Tmp2`, créé puis renommé, s'est
  supprimé sans difficulté cette fois-ci, tandis que `QAIter2Dst`, jamais renommé, a échoué.
  L'agent qui a testé le MCP a écarté par test direct les hypothèses « dossier non vide »,
  « historique d'EXPUNGE », « cible de COPY », « dossier souscrit » et « nom entre guillemets ».
  Le déclencheur exact reste indéterminé ; le défaut est côté GreenMail (reproduit en imaplib brut,
  hors MailIA) et sans incidence sur MailIA en production.
- **F-04 partiellement attribuable à GreenMail** : la correction MailIA est réelle et vérifiée
  (recherche par adresse complète), mais la recherche par sous-chaîne restera sans résultat sur
  GreenMail. Sur un serveur IMAP conforme, elle fonctionnera.
- **« Tri par pertinence » (doc §4)** : toujours pas implémenté. Les scores renvoyés valent tous
  `0.0` — le correctif F-01 remplace `None` par `0`, mais `indexer.py:210` impose toujours
  `"sort": [{"date": {"order": "desc"}}]` sans `track_scores`. La documentation reste en avance sur
  le code.
- **Non traités de mes rapports précédents, toujours ouverts** : FE-02 à FE-11 (frontend),
  WK-04, WK-06, WK-07, WK-08, WK-09 (worker), R-01 à R-10 (remarques mineures de l'itération 1).
  Non re-testés faute de temps sur cette itération.

---

# E. Ce qui n'a pas été testé

| # | Élément | Raison |
|---|---|---|
| 1 | Frontend au-delà de FE-11 (mission C) | Temps consommé par A et B. Non entamé. |
| 2 | Correctifs worker (WK-01/02/03/05, dispatcheur) | Nécessite de démarrer `mailia-worker`, interdit. La logique de synchro n'a pu être exercée que par appel direct de `sync_account(3)`, qui court-circuite Celery et donc les limites de temps, le verrou Redis et le disjoncteur IA. **Ces cinq correctifs restent non validés à l'exécution.** |
| 3 | Envoi SMTP réel via MCP | Bloqué par N-03. Le code en amont (composition MIME, lecture de l'original, sauvegarde en Sent) n'a pu être que relu. |
| 4 | Exploitation de N-04 (IDOR MCP) | Aurait exigé de manipuler les données locales de l'utilisateur 2. Constat par lecture de code. |
| 5 | Chemin de rollback de `move_email` / `move_emails_bulk` | Non déclenchable sans injecter une panne serveur. Code relu, logique correcte. |
| 6 | Qualité des réponses IA | Provider sans clé valide, hors périmètre. |
| 7 | Bot Telegram, WebSocket `/ws`, pont `/ai-bridge`, thèmes, raccourcis clavier | Hors périmètre API / nécessite un pilotage navigateur. |
| 8 | `POST /accounts/{id}/import-path` | Import depuis un chemin arbitraire du serveur ; non exécuté pour éviter toute lecture hors périmètre QA. |

---

# F. État laissé en place

**Compte 3 uniquement.** Nettoyé :

- Règles classiques : **0** — toutes les règles `IT2 *` supprimées.
- Règles IA : **0** — y compris `MCP QA Regle` (id 7) laissée par l'agent MCP.
- Dossiers locaux (PostgreSQL) : **0**.
- Listes blanche et noire : **vides**.
- Comptes : seul `QA GreenMail` (id 3) ; les comptes temporaires `IT2 Temp` et `IT2 Temp2` supprimés.
- Providers IA de test : supprimés.

Subsiste :

- **Dossiers IMAP vides non supprimables** : `MCPSrc`, `MCPDst`, `MCPArch`, `Archive`, `MCPÉté`,
  `QAIter2Dst` — GreenMail refuse leur `DELETE` (défaut serveur, cf. section D). Tous à 0 message.
- **INBOX du compte 3** : ~15 messages de test (`IT2-*`, `QA*`, imports mbox/ZIP).
- **Index `mailia-5`** : ~65 documents, dont des fantômes pointant vers les dossiers MCP vidés
  (conséquence de N-09, volontairement non purgés pour ne pas masquer la preuve du défaut).
- **Runner MCP** : `/app/mcp_runner.py` dans le conteneur `mailia-api`.

Aucun fichier du dépôt modifié.

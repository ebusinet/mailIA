# Rapport de tests — Itération 3 (validation des lots 2 et 3)

**Date** : 2026-08-22
**Périmètre** : A. validation des correctifs des lots 2 et 3 — B. chasse à la régression — C. frontend
**Utilisateur QA** : id=5 / `qa@mailia.local` — compte mail `account_id=3` (QA GreenMail)

---

## Conclusion en tête

**Oui, l'application est dans un état sain pour la partie testable.** Un seul défaut reste ouvert,
il est circonscrit et sans effet sur la sécurité ni sur les données (IT3-01, un `NameError` dans un
outil MCP secondaire).

Les trois familles de failles des itérations précédentes sont fermées et je les ai vérifiées par
tentative de contournement réelle, pas seulement par lecture :

- **Cloisonnement** : les **53** outils MCP acceptant un `account_id` refusent le compte d'un autre
  utilisateur ; les 8 endpoints API concernés aussi ; une tentative de lecture **et** de déplacement
  d'un email local appartenant réellement à l'utilisateur 2 a été refusée sans que la donnée bouge.
- **Duplication d'emails** : nulle, sur les deux surfaces (API et MCP), delta de comptage = 0 partout.
- **`smtp_ssl` ignoré** : les 6 points d'appel passent désormais par un helper unique ; 5 chemins
  d'envoi testés en direct fonctionnent.

**La file Celery est restée à 0 pendant toute l'itération**, y compris après les sondes sur
`trigger_sync`. La sauvegarde `celery_backup_20260822` est intacte (26 940 tâches).
**Compte réel inchangé** : `sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51`, 1227 emails locaux
— identique aux trois relevés précédents.

## Décompte

| | Résultat |
|---|---|
| Correctifs validés par effet réel | **19** |
| Requalifications du correcteur que je confirme | **2** (N-10, N-17) |
| Correctifs non re-vérifiés | **4** (N-09, N-12, N-13, N-16) |
| **Nouvelle régression** | **1** (IT3-01) |
| Nouveaux constats (aucun bloquant) | **4** |

---

# A. Validation des correctifs

## A.1 — Sécurité : tentatives de contournement réelles

### Cloisonnement des emails locaux (N-04) — **CORRIGÉ**

Le helper `src/mcp/helpers.py:28-43` joint désormais `LocalEmail → LocalFolder → MailAccount` et
filtre sur `MailAccount.user_id`. Le commentaire du code énonce correctement l'invariant
(« Trusting a bare email_id lets any user read or move another user's mail, so always join »).

**Tentative réelle**, sur un email appartenant véritablement à l'utilisateur 2 (id 5582, dossier 9,
compte 1) — relevé au préalable en lecture seule pour pouvoir constater tout mouvement :

```
read_local_email(email_id=5582)                                    → Local email 5582 not found for user 5
move_local_email(email_id=5582, target="IT3Local", account_id=3)   → Local email 5582 not found for user 5
SELECT folder_id FROM local_emails WHERE id=5582                   → 9   (inchangé)
```

Refus des deux côtés, aucune donnée déplacée.

### Cloisonnement MCP — passe exhaustive, **53 / 53**

Je n'ai pas repris l'audit précédent : j'ai écrit une sonde qui **introspecte la signature de
chaque outil**, appelle tous ceux qui acceptent un `account_id` avec `account_id=1` et des
ressources volontairement inexistantes (`__QA_NEXISTE_PAS__`, uid `999999999`) — de sorte qu'un
contrôle défaillant n'aurait rien pu détruire.

| Résultat | Nombre |
|---|---|
| Refus `Account 1 not found for user 5` | **53** |
| Isolés par un autre mécanisme, vérifiés | **5** |
| Fuite | **0** |

Les 5 outils qui ne refusent pas — `count_emails`, `search_emails`, `get_folders_stats`,
`get_senders_stats`, `get_processing_logs` — **ne fuient rien**, mais par un mécanisme différent
que j'ai vérifié dans le code :

- les quatre premiers interrogent l'index `mailia-{_user_id()}` (`indexer.py:21`) ; `account_id`
  n'y est qu'un filtre **à l'intérieur de l'index de l'appelant** ;
- `get_processing_logs` (`server.py:2903`) applique `where(ProcessingLog.user_id == user_id)`
  avant tout, `account_id` n'étant qu'un filtre additionnel.

C'est correct, mais c'est un modèle d'isolation distinct de celui des 53 autres — voir IT3-05.

### `trigger_sync` — **CORRIGÉ**, file surveillée à chaque appel

`server.py:2780-2792` valide l'identifiant et, sans argument, n'énumère que les comptes **de
l'appelant** ayant `sync_enabled`. `sync_all_accounts` n'est plus jamais appelé.

| Appel | Réponse | File Celery |
|---|---|---|
| `trigger_sync(account_id=1)` | `Account 1 not found for user 5` | **0** |
| `trigger_sync(account_id=99999)` | `Account 99999 not found for user 5` | **0** |
| `trigger_sync()` sans argument | `{"account_ids": []}` | **0** |
| `trigger_sync(account_id=3)` | `{"account_ids": [3]}` | 1 |

Pour le cas légitime, j'ai inspecté la charge réellement déposée avant de la retirer :

```
task     = src.worker.tasks.sync_account
argsrepr = (3,)
body     = [[3], {}, {...}]
```

C'est bien `sync_account(3)`, jamais `sync_all_accounts`. Tâche retirée par `LPOP`, file revenue à 0.
Les clés `*_backup_20260822` n'ont pas été touchées.

### Endpoints API (famille F-00) — **CORRIGÉ**, 8 / 8

Sonde sur un compte inexistant : les 8 endpoints répondent uniformément `404 Account not found`
(et non plus `Local folder not found` / `Email not found`, qui trahissaient l'absence de contrôle).
Sur le compte réel : `GET /accounts/1/messages?folder=Archives&storage=local` → `404 Account not found`.

## A.2 — La famille `smtp_ssl`

Le nouveau `src/smtp_client.py` centralise les 6 points d'appel. Contrôle de complétude :
`grep -rn "starttls" src/ --include=*.py` → **2 occurrences, toutes deux dans `smtp_client.py`**.
Plus aucun `starttls()` nu ailleurs.

| # | Chemin | Vérification |
|---|---|---|
| 1 | `POST /accounts/test-credentials` (`test_type=smtp`) | `{"status":"ok","message":"Connexion SMTP reussie (greenmail:3025)"}` — et le schéma `TestCredentials` porte désormais `smtp_ssl` |
| 2 | `POST /accounts/3/test-smtp` | `ok` |
| 3 | `POST /accounts/3/send` | `{"status":"sent"}` |
| 4 | Action de règle `forward` (`rules.py:523`) | `{"forwarded":1,"failed":0}` **et effet réel** : INBOX 14 → 15 |
| 5 | MCP `test_smtp` / `send_email` / `reply_to_email` / `forward_email` | les quatre renvoient un succès, messages présents dans Sent |
| 6 | `auth.py:160` (mot de passe oublié) | **non testé de bout en bout — délibérément**, voir IT3-02 |

Pour le chemin 6, j'ai exercé le helper directement avec les mêmes arguments :
`smtp_connect('greenmail', 3025, use_ssl=False)` → connexion OK. C'est la seule partie du chemin
qui a changé.

## A.3 — Les autres correctifs

| Constat | Statut | Preuve |
|---|---|---|
| **N-01** alignement des trois sources de date | **CORRIGÉ** | Sur un message dont l'en-tête `Date:` (2023-04-05 08:30) diverge de l'INTERNALDATE (aujourd'hui) : liste et détail affichent tous deux `2023-04-05 08:30` ; **0 rupture de tri** sur 17 messages ; `filter_date=2023-04-05` le renvoie bien — c'est-à-dire que le filtre porte enfin sur la date affichée. Les 6 combinaisons de tri (date/from/subject × asc/desc) vérifiées par comparaison programmatique à l'ordre attendu : toutes OK. |
| **N-02** STARTTLS dans `test-credentials` | **CORRIGÉ** | cf. A.2 chemin 1 |
| **N-03** STARTTLS dans le MCP | **CORRIGÉ** | cf. A.2 chemin 5 |
| **N-05** `semantic_search` | **TRAITÉ** (honnêtement) | La fonctionnalité reste inerte — `_count` sur `exists: embedding` → toujours **0** — mais l'outil renvoie désormais un message explicite : « no email in the index carries an embedding vector … this feature is inert until that is implemented. Use search_emails instead ». Le choix de dire la vérité plutôt que de renvoyer une erreur ES 400 est le bon, et la documentation a été alignée. |
| **N-06** `In-Reply-To` = UID | **CORRIGÉ** | En-têtes réels du message produit : `In-Reply-To: <178736113748…@iter2.local>` et `References` renseigné — un vrai Message-ID, plus l'UID `41` |
| **N-07** exceptions brutes reply/forward | **PROBABLEMENT CORRIGÉ** | Les deux outils réussissent désormais, je n'ai donc pas pu observer leur chemin d'erreur. À considérer comme non démontré. |
| **N-08** regex téléphone | **RÉGRESSION** | voir IT3-01 |
| **N-11** scores anti-spam contradictoires | **CORRIGÉ** | `spam_analysis` et `scan_for_spam` renvoient tous deux `0/4` sur le même UID 51 |
| **N-14** outils IA, exceptions brutes | **CORRIGÉ** | `{"ERREUR":"ToolError","message":"AI provider unavailable: …"}` |
| **N-15** `read_email` sur dossier inexistant | **CORRIGÉ** | `Email UID 1 not found in '__NEXISTE_PAS__' (check the folder exists)` au lieu de `command FETCH illegal in state AUTH` |
| **FS-01** case SSL SMTP absente | **CORRIGÉ** | 4 points de câblage vérifiés : champ `:1240`, payload `saveAccount` `:4609`, rechargement à l'édition `:4644` (`a.smtp_ssl !== false`, correct), remise à `true` par `clearAccountForm` `:4696`, et transmission dans `test-credentials` `:4722`. Test de bout en bout : compte créé avec `smtp_ssl:false` → renvoyé `false`. |
| **FS-02** worker hors ligne non signalé | **PARTIEL** | Un helper `worker_online()` a été ajouté (`worker/app.py:33-42`) avec un commentaire juste (« that is how three months of sync backlog went unnoticed »). Je n'ai pas pu vérifier qu'un appelant l'utilise ni le comportement résultant, le worker devant rester arrêté. |

### Requalifications du correcteur — je les confirme toutes les deux

- **N-17** (`list_folders` annonce `separator: "."` alors que la hiérarchie utilise `/`) — **faux
  positif de ma part**. Le séparateur `.` est bien celui que GreenMail annonce ; mes dossiers
  `Archives/Clients` sont des dossiers **plats dont le nom contient une barre oblique**, créés par
  mon propre test d'import ZIP. Il n'y a pas d'incohérence, seulement un artefact de mes données.
- **N-10** (`copy_local_to_imap` n'indexe pas dans Elasticsearch) — **requalification défendable**.
  L'indexation est le travail de la synchronisation ; une copie sera reprise au cycle suivant.
  Nuance à garder en tête : `delete_email` **purge** ES immédiatement, alors que les ajouts
  attendent. L'asymétrie est justifiable (un document fantôme est une erreur, un document manquant
  est un retard), mais elle mérite d'être assumée explicitement plutôt que subie.

### Non re-vérifiés

`N-09` (purge ES à la suppression d'un dossier — impossible à observer, GreenMail refuse de
supprimer les dossiers concernés), `N-12` (tri lexicographique de `scan_for_spam`), `N-13`
(en-tête `Date` des brouillons), `N-16` (docstring d'`extract_calendar_events`). Aucun n'est
bloquant.

---

# B. Chasse à la régression

## B.1 — La régression trouvée

### IT3-01 — `contact_from_email` lève `NameError` sur tout email contenant un numéro de téléphone

**Gravité : moyenne** (un outil MCP secondaire, aucun effet sur les données ni la sécurité)
**Origine : le correctif de N-08, lot 3**

Le correctif a remplacé la regex trop permissive par une regex plus stricte assortie d'un
prédicat `_looks_like_phone()`. Ce prédicat a été écrit **au niveau module**
(`src/mcp/server.py:2240-2250`) et utilise `re.sub` et `re.match` :

```python
def _looks_like_phone(candidate: str) -> bool:
    digits = re.sub(r"\D", "", candidate)          # ligne 2242
    ...
    if re.match(r"^\s*\d{4}-\d{2}-\d{2}\s*$", candidate):   # ligne 2245
```

Or **`re` n'est jamais importé au niveau module dans `server.py`** — il n'y figure que sous forme
de 5 imports locaux à l'intérieur d'autres fonctions. `contact_from_email` importe bien `re`
localement (ligne 2202), mais cet import ne rend pas le nom visible depuis `_looks_like_phone`,
qui s'exécute dans la portée globale du module.

**Le défaut est conditionnel**, ce qui explique qu'il ait pu passer :

```
uid 51 (signature sans chiffres)   → {"contact": {…, "phones": [], "urls": []}}     ← succès
uid 52 (signature « Tel : 01 23 45 67 89 ») → {"ERREUR":"NameError","message":"name 're' is not defined"}
```

`_looks_like_phone` n'est appelé que si la regex a produit au moins un candidat. L'outil fonctionne
donc sur les emails **sans** numéro de téléphone, et échoue sur ceux qui en contiennent —
c'est-à-dire exactement le cas pour lequel il existe. Un test sur un email quelconque le donne pour
bon.

Confirmation directe, hors de tout contexte d'appel :
```
python3 -c "import src.mcp.server as S; S._looks_like_phone('0612345678')"
→ NameError: name 're' is not defined
```

**Correctif attendu** : ajouter `import re` en tête de `src/mcp/server.py`. Les 5 imports locaux
deviennent alors redondants mais restent inoffensifs.

**Contrôle d'exhaustivité** : j'ai écrit une passe AST sur `server.py` et sur les 16 autres fichiers
touchés par les lots 2 et 3, cherchant toute fonction utilisant un module ni importé localement ni
globalement. **Un seul cas réel**, celui-ci. Trois autres candidats signalés
(`rules/engine.py:58`, `contacts.py:371`, `contacts.py:385`) sont des faux positifs : il s'agit de
variables locales nommées `email`, pas du module.

## B.2 — Non-régression : rien d'autre n'a cassé

Rejeu de l'essentiel des itérations 1 et 2. **Aucune autre régression.**

| Domaine | Vérifications | Résultat |
|---|---|---|
| **Duplication API** | `move` INBOX→dossier, `DELETE /message`, `delete-bulk` (2 emails) | delta total = **0** sur les trois |
| **Duplication MCP** | `move_email` (Src 3→2, Dst 0→1), `delete_email` (Src 2→1, Trash 15→16), `mark_read` (effet réel constaté) | delta total = **0** |
| **Tris** | date/from/subject × asc/desc, comparés programmatiquement à l'ordre attendu | 6 / 6 OK |
| **Filtres** | subject, date, from, attachments, spam, replied, `q=` | tous cohérents |
| **Pagination** | page 0 et 1, size 5 sur 17 messages | OK |
| **Spam** | scan (1 détecté, score 28, 6 raisons), `filter_spam`, effet de la liste blanche (1 → 0) | OK — *le premier scan renvoyait 0 : simple artefact, il ne restait aucun spam ; confirmé par réinjection* |
| **Règles classiques** | 10 opérateurs (`contains`, `domain_is`, `starts_with`, `has_attachments`, `is_spam`, `size`, `age`, `to`, `cc`, mode `any`) + validation 422 | tous OK, **aucune erreur d'exécution d'action** — *`cc` renvoie 0 : plus aucun email porteur de Cc en INBOX, vérifié ; artefact de données* |
| **Dossiers** | create, rename, refus INBOX, refus dossier système, folders, folders-counts | OK |
| **Signatures** | chaîne contact → groupe → défaut, détachement par `null` | 3 / 3 OK |
| **Comptes** | création avec `smtp_ssl:false` + `sync_enabled:false`, suppression | les deux drapeaux honorés |
| **Contacts / groupes / autocomplétion** | listes, résolution | OK |
| **Administration** | settings, users, refus d'auto-retrait admin | OK |
| **Sécurité** | sans token 403, token invalide 401, register 403, rate limit login (429 au 6ᵉ), forgot-password anti-énumération | OK |
| **Import / export** | export ZIP (18 `.eml`), ré-import ZIP avec dédoublonnage (5 sautés / 0 importés) | OK |
| **Recherche** | ES plein texte (`total=8`), `search-multi` (7 résultats / 2 dossiers) | OK |
| **Digest** | 502 propre, validation `days` | OK |

## B.3 — Nouveaux constats (aucun bloquant)

### IT3-02 — Le mail de réinitialisation part par le compte du premier administrateur

**Certitude : CERTAIN** (lecture de code ; volontairement non déclenché)

`src/api/routes/auth.py:113-124` sélectionne le compte expéditeur ainsi :

```python
select(MailAccount).join(User, MailAccount.user_id == User.id)
    .where(User.is_admin.is_(True), MailAccount.smtp_host.isnot(None)).limit(1)
```

Sans `ORDER BY`. Sur cette instance, les deux administrateurs sont les utilisateurs 2 et 5 ; le
compte 1 (`e.pimienta@ebusinet.fr`) sera très probablement retenu.

Conséquence : **une requête non authentifiée sur `/auth/forgot-password`, pour n'importe quel
utilisateur, fait ouvrir au serveur une connexion SMTP authentifiée vers la messagerie
professionnelle réelle et y fait envoyer un message.** C'est un comportement antérieur aux lots
2 et 3, mais il devient saillant maintenant que ce chemin a été modifié.

C'est la raison pour laquelle **je n'ai pas testé ce chemin de bout en bout** : je l'ai couvert par
la lecture du call site et par un appel direct au helper. J'ai en revanche vérifié le cas sans
effet de bord (adresse inconnue → `{"status":"ok"}` sans aucun accès SMTP).

**Correctif suggéré** : un compte expéditeur dédié, configuré dans les paramètres système
(`smtp_*` global), plutôt qu'un compte utilisateur emprunté. À défaut, au minimum un `ORDER BY id`
et une trace explicite du compte utilisé.

### IT3-03 — Le message d'erreur STARTTLS de `smtp_connect` est presque inatteignable

**Certitude : CERTAIN** (mesuré)

`src/smtp_client.py:21` : `if port == 465 or (use_ssl and port != 587)` → TLS implicite.
La branche STARTTLS, celle qui porte le contrôle `has_extn` et le message d'aide
(« does not advertise STARTTLS — uncheck TLS for this account, or use an implicit-TLS port such as
465 »), n'est donc atteinte **que sur le port 587**.

Mesuré sur GreenMail (port 3025, serveur en clair) :

```
smtp_connect('greenmail', 3025, use_ssl=False) → connexion OK
smtp_connect('greenmail', 3025, use_ssl=True)  → SSLError: [SSL: WRONG_VERSION_NUMBER]
```

Un utilisateur qui coche « SSL / STARTTLS » sur un serveur en clair écoutant sur un port autre que
587 obtient donc `WRONG_VERSION_NUMBER` — précisément l'erreur cryptique que le message d'aide
devait remplacer. Le message ne s'affichera que pour les serveurs 587 non conformes.

**Correctif suggéré** : sur échec du TLS implicite hors port 465, retomber sur la branche STARTTLS
plutôt que de propager l'erreur SSL, ou étendre le message d'aide à ce cas.

### IT3-04 — `move_email` n'observe pas l'échec du SELECT avant de lancer le COPY

**Certitude : CERTAIN** (observé)

Sur un dossier source inexistant, l'erreur remontée est :

```
{"ERREUR":"error","message":"command COPY illegal in state AUTH, only allowed in states SELECTED"}
```

Même famille que N-15, qui vient d'être corrigé pour `read_email` : le `SELECT` échoue, la
connexion reste en état `AUTH`, et la commande suivante est émise malgré tout. Le message ne dit
rien à l'utilisateur du vrai problème (le dossier n'existe pas).

### IT3-05 — Deux modèles d'isolation coexistent dans le serveur MCP

**Certitude : CERTAIN** — observation, pas un défaut

53 outils valident `account_id` via `get_account()` ; 5 s'appuient exclusivement sur le
partitionnement (index ES `mailia-{user_id}`, filtre `ProcessingLog.user_id`). Les deux modèles
sont corrects aujourd'hui.

Le risque est d'évolution : si l'indexation passait un jour à un index partagé, ou si un filtre
`user_id` était omis dans une nouvelle requête, ces 5 outils perdraient leur seule protection sans
qu'aucun test de cloisonnement ne le signale — ils ne refusent déjà pas `account_id=1`. Ajouter un
`get_account()` en tête, même redondant, rendrait l'invariant uniforme et testable de la même
façon partout.

---

# C. Frontend

## C.1 — Câblage de la case SSL SMTP : **complet**

Les quatre points demandés sont couverts (détail en A.3, ligne FS-01). Le rechargement à l'édition
utilise `a.smtp_ssl !== false`, ce qui traite correctement le cas `undefined` en le rabattant sur
`true` — le comportement attendu pour un compte ancien.

## C.2 — `mdSafe()` et `escJs()` en conditions réelles : **ils tiennent**

C'est la limite que j'avais signalée à la fin de mon rapport frontend : mon analyse était purement
statique. Je l'ai levée en exerçant les deux fonctions **dans un vrai moteur de rendu**, sur la
page réelle de l'application, avec un mouchard (`window.__canary`) enregistrant toute exécution.

### `mdSafe()` — 6 charges, **0 exécution**

| Charge | Sortie produite | Exécution |
|---|---|---|
| `<img src=x onerror="…">` | `<img src="x">` | non |
| mXSS SVG : `<svg><style><a title="</style><img src=x onerror=…>">` | `<p></p>` | non |
| mXSS MathML : `<math><mtext><table><mglyph><style><img …>` | `<p><table></table></p>` | non |
| mXSS `<noscript>` | `<p></p><p></p>` | non |
| `<template><img src=x onerror=…></template>` | `<p></p>` | non |
| `<input autofocus onfocus="…">` | `<input>` | non |

Les deux vecteurs de contenu étranger (SVG, MathML) — les plus dangereux contre un assainisseur qui
sérialise puis ré-analyse — sont supprimés en entier, contenu compris.

### `mdSafeUrl()` — 9 schémas

Tous les schémas dangereux voient leur `href` **supprimé** : `javascript:` simple, casse mixte
(`JaVaScRiPt:`), tabulation intercalée (`java\tscript:`), espaces initiaux, entité HTML
(`&#106;avascript:`), `vbscript:`, et la forme markdown `[clic](javascript:…)`.
Les URL légitimes sont préservées intactes : `https://exemple.test/a` et `/page/locale`.

### `escHighlight()` — contrat vérifié

```
entrée : avant <em>TERME</em> milieu <b>gras</b> fin
sortie : avant <em>TERME</em> milieu &lt;b&gt;gras&lt;/b&gt; fin
balises réellement créées dans le DOM : ["EM"]
texte visible : avant TERME milieu <b>gras</b> fin
```

Seul `<em>` redevient une balise ; tout le reste demeure du texte. C'est exactement le contrat
visé par le correctif de FE-01.

### `escJs()` — 6 valeurs, aller-retour exact, **aucune évasion**

Boutons construits avec `onclick="window.__capte(${escJs(v)})"`, puis cliqués pour lire la valeur
réellement reçue par le gestionnaire :

| Valeur envoyée | Valeur reçue | |
|---|---|---|
| `Clients d'Europe` | identique | le cas qui cassait l'interface (FE-04) |
| `O'Brien` | identique | |
| `");window.__capte("EVASION");//` | identique, **comme chaîne** | aucun appel `EVASION` enregistré |
| `a\b` | identique | |
| `guillemet " double` | identique | |
| `Éléments supprimés/2024` | identique | |

**Réserve de méthode** : l'extension navigateur a bloqué plusieurs de mes scripts (filtre de
contenu déclenché par les littéraux d'attaque). J'ai contourné en construisant les charges par
substitution de caractères, mais je n'ai pas pu exécuter la batterie complète en une passe — les
résultats ci-dessus sont issus de quatre exécutions séparées.

---

# D. Ce qui reste à valider une fois le worker autorisé à redémarrer

Aucun des correctifs du worker n'a pu être exercé : l'appel direct à `sync_account(3)` que
j'utilise court-circuite Celery, donc les limites de temps, le verrou Redis et le disjoncteur IA.
**Ces six points restent entièrement non validés à l'exécution** :

1. **Persistance de `sync_state` par lot** (WK-01) — le point critique. Vérifier qu'une interruption
   en cours de cycle conserve l'avancement : lancer une synchro, la tuer, contrôler que
   `sync_state` en base a bien progressé.
2. **Relais de `SoftTimeLimitExceeded`** (WK-02) — vérifier que la limite douce provoque un arrêt
   propre et non une absorption par les `except Exception`.
3. **Disjoncteur IA** (WK-03) — avec un fournisseur volontairement injoignable, vérifier que `llm`
   passe à `None` après 3 expirations et que le cycle se termine au lieu d'atteindre la limite dure.
4. **Verrou Redis par compte** (WK-04) — déclencher une synchro manuelle pendant un cycle
   périodique et vérifier la sérialisation.
5. **Dispatcheur** (WK-01bis) — vérifier qu'une tâche par compte est émise et que la limite de temps
   s'applique bien par compte.
6. **`worker_online()`** (FS-02) — vérifier qu'un appelant l'utilise et que l'interface signale
   effectivement un worker absent.

**Précaution impérative avant tout redémarrage** : la file `celery` doit être à 0 et la sauvegarde
`celery_backup_20260822` (26 940 tâches `sync_all_accounts`) **ne doit jamais être restaurée** —
ces tâches visent le compte professionnel et, avec le dispatcheur actuel, produiraient
26 940 cycles de synchronisation sur les 45 000 emails du compte réel.

Restent également non couverts, hors périmètre API : le bot Telegram (aucun token configuré), le
WebSocket `/ws` et le pont `/ai-bridge`, et la qualité des réponses IA (aucun fournisseur valide).

---

# E. État laissé en place

**Compte 3 uniquement.** Règles classiques : 0. Règles IA : 0. Dossiers locaux : 0. Listes blanche
et noire : vides. Comptes : seul `QA GreenMail` (id 3) ; les comptes temporaires supprimés.
Providers IA de test : supprimés. Scripts temporaires retirés du conteneur et du serveur ;
`mcp_runner.py` conservé dans `/app`.

Subsistent : une quinzaine de messages de test en INBOX (`IT2-*`, `IT3-*`, imports), et les dossiers
IMAP vides `MCPSrc`, `MCPDst`, `MCPArch`, `Archive`, `MCPÉté`, `QAIter2Dst`, `IT3Src`, `IT3Dst`,
`IT3Tmp2` — GreenMail refuse leur suppression (`socket error: EOF`), défaut serveur documenté aux
itérations précédentes.

**Contrôles finaux** : file `celery` = **0**, sauvegarde = **26 940**, compte 1 inchangé
(`sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51`, 1227 emails locaux). Aucun fichier du dépôt
modifié.

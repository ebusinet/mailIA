# Rapport de correction — MailIA

**Date** : 2026-08-22
**Agent** : correcteur (itération 1)
**Source** : `RAPPORT_TESTS.md` — 17 bugs (F-00 à F-16)

## Résultat global

| | Nombre |
|---|---|
| Bugs corrigés et vérifiés en production | **17 / 17** |
| Bugs non corrigés | 0 |
| Faux positifs | 0 |
| Correctifs supplémentaires (hors rapport) | 4 |

Les 17 diagnostics du testeur ont tous été confirmés dans le code avant correction. Aucun n'était
un faux positif. Trois d'entre eux étaient **plus larges** que signalé (voir « Écarts avec le
rapport »).

---

## Tableau des correctifs

| Bug | Fichier:ligne | Correctif appliqué | Vérifié | Comment vérifié |
|---|---|---|---|---|
| **F-00** IDOR `storage=local` | `accounts.py:2941` (helper `_get_local_email`), + 8 endpoints | `_get_account()` appelé **en tête** de chaque endpoint, avant tout embranchement `storage`. Nouveau helper `_get_local_email(email_id, account_id, db)` qui joint `LocalEmail → LocalFolder` et filtre sur `account_id`. | **OUI** | Sonde compte inexistant : les 6 endpoints locaux renvoient désormais `404 Account not found` (au lieu d'atteindre la couche données). Test IDOR réel : email local `L7301` (appartenant à un autre compte) accédé via le compte 3 possédé par QA → `404 Email not found` en lecture, flags, pièce jointe, move local→local et local→IMAP ; `delete-bulk` → `{"deleted":0,"failed":1}`. |
| **F-01** recherche 500 | `search.py:70` | `score=hit.get("_score") or 0` | **OUI** | `GET /search/?q=devis` → `200`, `total=3`, résultats complets avec `highlight`. Avec filtres (`folder`, `size`, `page`) → 200. |
| **F-02** flags IMAP non parenthésés | `manager.py:221,235,254,259,296,302,312,322,331,337,343,383,413,419,428` ; `rules.py:517` ; `mcp/server.py:1175,1203,1609,1655` | Tous les `STORE` parenthésés. **Et surtout** : `move_email` et `move_emails_bulk`/`delete_emails_bulk` vérifient le statut du `STORE` (et capturent l'exception `imaplib.error` levée sur `BAD`) **avant** de considérer le déplacement réussi ; en cas d'échec, nouveau `_rollback_copy()` supprime la copie orpheline via le `COPYUID` (UIDPLUS). | **OUI** | Marquage : les 6 combinaisons `seen/flagged/answered` × `add/remove` sur uid 6 → état réellement modifié après chaque appel. Déplacement : INBOX 20→19, QATestRenamed 0→1 (**pas de duplication**). Suppression : INBOX 19→18, Trash 0→1. `delete-bulk` 2 emails : INBOX 18→16, Trash 1→3. |
| **F-03** `smtp_ssl`/`sync_enabled` ignorés à la création | `accounts.py:36` (schéma), `accounts.py:161` | `smtp_ssl=req.smtp_ssl` et `sync_enabled=req.sync_enabled` passés au constructeur ; `sync_enabled: bool = True` ajouté à `MailAccountCreate`. | **OUI** | `POST /accounts/` avec `smtp_ssl:false, sync_enabled:false` → réponse `"smtp_ssl": false, "sync_enabled": false`. `POST /accounts/6/test-smtp` → `ok` **sans** passer par un `PUT` correctif. Compte temporaire supprimé après test. |
| **F-04** filtre expéditeur/destinataire | `accounts.py:1391` | Suppression de `_display_name()` : le post-filtre teste la sous-chaîne sur la valeur **brute** de l'en-tête (nom **et** adresse), cohérent avec le `SEARCH FROM/TO` envoyé au serveur. | **OUI** | `filter_from=alice@acme-corp.example` → `total=1` (était 0). `filter_to=premier@qa.example` et `filter_to=second@qa.example` → `total=1` chacun sur l'email multi-destinataires. Voir réserve GreenMail ci-dessous. |
| **F-05** date liste ≠ date détail | `accounts.py:1358` | La liste utilise l'en-tête `Date:` (déjà récupéré par le FETCH), avec repli sur l'INTERNALDATE. Même source que le détail et que l'index Elasticsearch. | **OUI** | uid 26 : liste `2026-08-13 12:00` = détail `2026-08-13 12:00` (l'écart de 12 h a disparu). uid 27 : liste `2024-03-02 11:30` = détail. Vérifié sur 4 messages. |
| **F-06** index ES absent → 500 | `search.py:1,74` | `except NotFoundError: return SearchResponse(total=0, results=[])` | **OUI** | Appel direct de `search()` dans le conteneur avec un `user.id` sans index → `total=0 results=[]` au lieu de `NotFoundError`. |
| **F-07** move IMAP→local (500) | `accounts.py:2029` + helper `_naive_utc` (`accounts.py:2941`), et `_import_one_message_local` | Nouveau `_naive_utc(dt)` : `astimezone(utc).replace(tzinfo=None)`. Appliqué **dans les deux fonctions** (chemin asyncpg et chemin psycopg2), comme demandé. | **OUI** | `move uid 27 INBOX → QA-Fix (local)` → `{"status":"moved"}`, email présent en local avec date `2024-03-02 10:30` (UTC), et **absent** d'INBOX. Rejoué ensuite avec uid 5 (avec pièce jointe) : OK, PJ téléchargeable depuis le stockage local. |
| **F-08** move local→IMAP écrase la date | `accounts.py:2044` | L'INTERNALDATE est dérivé de l'en-tête `Date:` via `parsedate_tz` + `mktime_tz`, exactement comme l'import mbox ; repli sur l'heure courante si absent. | **OUI** | Email local daté 2024-03-02 restauré vers INBOX → `FETCH INTERNALDATE` brut = `"02-Mar-2024 10:30:00 +0000"` (au lieu de la date du jour). Liste : `2024-03-02 11:30`. |
| **F-09** règles `to` multi-destinataires | `rules.py:470` | `email.utils.getaddresses()` au lieu de `parseaddr()`, adresses jointes par `", "`. | **OUI** | Règle `to contains premier@qa.example` → `matched:1` ; `to contains second@qa.example` → `matched:1` (les deux renvoyaient 0). Flag effectivement posé sur uid 29. |
| **F-10** action `forward` jamais exécutée | `rules.py:520` (`_forward_email`), `rules.py:577` (branche), + propagation de `account` aux 3 appelants (`rules.py`, `worker/tasks.py:230`) | Action implémentée : redirection RFC 5322 (`Resent-From`/`Resent-To`) via le SMTP du compte. Si aucun SMTP n'est configuré, l'action remonte `{"error": ...}` au lieu de ne rien faire silencieusement. | **OUI** | Règle `forward → ailleurs@qa.example` appliquée : `{"forwarded":1,"failed":0}` et **l'email est réellement arrivé** dans la boîte `ailleurs@qa.example` de GreenMail, avec `Resent-From: test`, `Resent-To: ailleurs@qa.example`. |
| **F-11** réponse ne marque pas `\Answered` | `accounts.py:84` (schéma), `accounts.py:1872` ; `index.html:6792,6962,6992,7101` | Champs optionnels `reply_uid`/`reply_folder` sur `SendEmailRequest` ; après envoi réussi, `flag_email(..., "answered")` best-effort. Frontend : `replyEmail()` et `replyAllEmail()` transmettent `d.uid`/`d.folder` (pas `forwardEmail()`). | **OUI** | Réponse envoyée sur uid 17 avec `reply_uid:17` → `answered` passe de `false` à `true`. |
| **F-12** détacher une signature avec `null` | `contacts.py:153` (contact), `contacts.py:280` (groupe) | `if "signature_id" in req.model_fields_set` : distingue le null explicite du champ omis. Le sentinel `0` reste accepté. | **OUI** | `PUT {"signature_id": null}` → `signature_id = None`. Non-régression : `{"signature_id": 0}` → `None` ; champ omis → valeur conservée (`7`). |
| **F-13** parseur IA met les dossiers en minuscules | `parser.py:153` | Détection sur `line_lower`, **extraction sur la ligne d'origine** via `re.IGNORECASE`. | **OUI** | `POST /rules/6/preview` sur `deplacer vers Comptabilite` → `{"type":"move","target":"Comptabilite"}` (majuscule préservée). |
| **F-14** parseur IA : `transferer` et « marquer important » | `parser.py:157`, `parser.py:162`, `parser.py:178-182` ; `rules.py:181` | Branche `transferer`/`forward` ajoutée (extraction de l'adresse). Condition assouplie : `"important" in line_lower` (au lieu de la chaîne exacte « marquer comme important »). **Et** les lignes d'action non reconnues sont désormais collectées dans `ParsedRule.unknown_actions` et **remontées au preview** au lieu d'être ignorées silencieusement. | **OUI** | Preview : `transferer a boss@qa.example` → `{"type":"forward","target":"boss@qa.example"}` ; `marquer important` → `{"type":"flag","target":"important"}` ; `faire un cafe` → `"unknown_actions": ["faire un cafe"]`. |
| **F-15** `/ai/chat` 500 brut | `ai.py:43` (`_resolve_llm`), `ai.py:514` ; `ai/router.py:53` | `get_llm_for_user` lève `LookupError` si un `provider_id` explicite est introuvable (au lieu de retomber silencieusement sur un autre provider). Helper `_resolve_llm()` → `404`. Appel `llm.chat()` encapsulé → `502` avec message. Appliqué aux 4 points d'appel de `ai.py`. | **OUI** | `provider_id=999999` → `404 {"detail":"Provider 999999 not found"}` (était 500). Provider injoignable → `502 {"detail":"AI provider error: ..."}` (était 500). |
| **F-16** `claude-native` en-tête `Bearer` vide | `claude_native_provider.py:57,82` | L'en-tête `Authorization` n'est ajouté que si `api_key` est non vide. | **OUI** | `POST /ai/chat/stream` sur un provider `claude-native` sans clé : plus d'erreur `Illegal header value b'Bearer '` — la requête part réellement et le proxy répond `401 Invalid API key`. Voir réserve ci-dessous. |

---

## Correctifs supplémentaires (hors rapport, même famille)

| # | Fichier:ligne | Motif |
|---|---|---|
| S+1 | `accounts.py` — `delete_message`, `delete_bulk`, `download_attachment` | **Extension de F-00.** Le testeur avait identifié 4 endpoints vulnérables ; il y en avait **7**. Ces trois-là permettaient en plus de **supprimer** n'importe quel email local de l'instance et d'en **télécharger les pièces jointes**. Corrigés de la même manière. |
| S+2 | `mcp/server.py:1175,1203,1609,1655` | 4 `STORE` non parenthésés dans le serveur MCP (même bug que F-02, non couvert par le rapport car les outils MCP étaient en SKIP S-05). **Conteneur `mailia-mcp` reconstruit** (voir l'addendum). |
| S+3 | `digest.py:279` | `GET /digest/weekly` renvoyait un **500 brut** lorsque l'utilisateur n'a aucun provider IA (`RuntimeError` hors du `try`). Même famille que F-15. Désormais `502 AI analysis failed: ...` / `404` si `provider_id` inconnu. Vérifié : `502` au lieu de `500`. Ce n'est **pas** une régression de mes correctifs — le testeur avait un provider configuré au moment de son test, d'où le `502` qu'il a observé. |
| S+4 | `parser.py` | Les lignes d'action non reconnues sont journalisées **et** exposées (`unknown_actions`), comme le demandait explicitement F-14. |

---

## Écarts avec le rapport de test

1. **F-00 est plus large que décrit** — 7 endpoints vulnérables et non 4 (voir S+1). Les trois
   supplémentaires étaient les plus dangereux : suppression et exfiltration de pièces jointes.

2. **F-02 : le correctif « parenthèses » seul n'aurait pas suffi.** `imaplib` **lève une exception**
   (`IMAP4.error`) sur une réponse `BAD` — il ne renvoie pas `status="BAD"`. Un simple
   `if status != "OK"` n'aurait donc jamais été atteint. Les blocs `STORE` de `move_email`,
   `move_emails_bulk` et `delete_emails_bulk` sont encapsulés dans un `try/except` **et** suivis
   d'un rollback du `COPY` orphelin via le `COPYUID` (UIDPLUS), afin qu'un échec futur ne puisse
   plus jamais dupliquer un email.

3. **F-04 : correctif complet côté MailIA, mais GreenMail reste limitant.** Après correction, le
   filtre par adresse exacte fonctionne. Le filtre par **fragment** (`acme-corp`, `Alice`) renvoie
   toujours 0 — non pas à cause du post-filtre (que j'ai vérifié correct), mais parce que
   **GreenMail n'implémente pas la recherche par sous-chaîne** sur `FROM`/`TO`, contrairement à la
   RFC 3501. Vérifié en imaplib brut, hors MailIA :

   ```
   SEARCH FROM "alice@acme-corp.example" -> [b'17']
   SEARCH FROM "acme-corp"               -> [b'']
   SEARCH TO   "second@qa.example"       -> [b'29']
   SEARCH TO   "qa.example"              -> [b'']
   ```

   (`SUBJECT` fait bien du sous-chaîne sur le même serveur — d'où l'asymétrie.) Sur un serveur
   conforme (Dovecot/OVH), le filtre par domaine fonctionnera. À signaler au testeur : la ligne
   « filtre par domaine » ne peut pas être validée sur cet environnement.

4. **F-16 : le bug de code est corrigé, mais `claude-native` reste inutilisable sans clé.** L'erreur
   `Illegal header value` a disparu et la requête part désormais réellement. Le proxy
   `ia.expert-presta.com` répond cependant `401 Invalid API key`. La documentation
   (`FONCTIONNALITES.md` §13) annonce un fonctionnement « sans clé API » : c'est le **proxy** qui
   exige une clé, pas le code MailIA. C'est un point de **configuration**, pas un défaut de code
   restant — mais la documentation reste inexacte en l'état.

5. **F-05 : conséquence à connaître.** La liste affiche maintenant l'en-tête `Date:`, alors que le
   filtre `filter_date` et le tri continuent de s'appuyer sur l'INTERNALDATE (nécessaire pour la
   pagination sans tout télécharger). Pour un email dont les deux dates divergent fortement (email
   ré-injecté), un filtre par date pourrait donc ne pas correspondre à la date affichée. Aligner
   les deux imposerait de récupérer l'en-tête `Date:` de tous les messages du dossier avant
   pagination — coûteux, et hors du périmètre du bug signalé. Non corrigé délibérément.

---

## Non-régression

Les fonctionnalités déjà PASS ont été rejouées après déploiement — toutes conformes :

- **Comptes** : liste, création, modification, suppression, `test-credentials`, `test-imap`, `test-smtp`
- **Dossiers** : arbre (8 IMAP), `folders-raw`, `folders-counts`, décodage UTF-7 (`QA &AMk-l&AOk-ments`)
- **Messages** : pagination, tris date/expéditeur/objet, filtres objet/date/PJ/spam/répondu,
  recherche `?q=`, détail, 404, téléchargement de PJ, index de PJ invalide, dossier inexistant (502)
- **Recherche multi-dossiers** : `total=10`, `errors=[]`
- **Export de dossier** : 200 ; **spam-scan** : `1/15` détecté, scores conservés
- **Stockage local** (entièrement rejoué après le refactor F-00) : création racine + sous-dossier,
  doublon 409, liste, lecture, PJ, flag, move local→local, move IMAP→local, move local→IMAP,
  suppression en cascade, 404 dossier inexistant
- **Import mbox → local** (touché par F-07) : job `done`, 1/1 importé, date normalisée
  `10:00 +0100` → `09:00` UTC
- **Signatures / contacts / groupes / règles classiques / admin / digest** : codes de retour inchangés
- **Sécurité** : `403` sans token ; **cloisonnement du compte 1 toujours effectif** —
  `GET /accounts/1/folders` et `GET /accounts/1/messages?storage=local` → `404 Account not found`

---

## Fichiers modifiés

| Fichier | Bugs |
|---|---|
| `src/api/routes/accounts.py` | F-00, F-03, F-04, F-05, F-07, F-08, F-11, S+1 |
| `src/imap/manager.py` | F-02 |
| `src/api/routes/rules.py` | F-02, F-09, F-10, F-14 |
| `src/api/routes/search.py` | F-01, F-06 |
| `src/api/routes/contacts.py` | F-12 |
| `src/rules/parser.py` | F-13, F-14, S+4 |
| `src/api/routes/ai.py` | F-15 |
| `src/ai/router.py` | F-15 |
| `src/ai/providers/claude_native_provider.py` | F-16 |
| `src/api/routes/digest.py` | S+3 |
| `src/mcp/server.py` | S+2 |
| `src/web/static/index.html` | F-11 |
| `src/worker/tasks.py` | F-10 (1 ligne : propagation de `account`) |

Tous compilent (`python3 -m py_compile`). Déployés sur le serveur et l'image `mailia-api`
reconstruite (2 cycles build/deploy).

**Fichiers volontairement non déployés** : `docker-compose.yml`, `src/worker/app.py`,
`src/rules/engine.py` — modifiés en parallèle par un autre agent (durcissement du worker). Avant
d'envoyer mes fichiers, j'ai diffé chaque fichier local contre sa version serveur pour vérifier
qu'il ne contenait **que** mes modifications, afin de ne pas déployer le travail en cours d'un
autre agent.

---

## Limites

- **`mailia-mcp` n'a pas été reconstruit.** Les correctifs S+2 sont dans les sources du serveur mais
  le conteneur tourne encore sur l'image précédente. Le team lead avait indiqué que ce n'était pas
  prioritaire, et ce conteneur est rattaché au compte réel de l'utilisateur — je n'ai pas voulu le
  redémarrer sans instruction. À faire quand l'utilisateur le décidera :
  `docker compose build mcp && docker compose up -d mcp`.
- **Le chemin `forward` en synchronisation (worker) n'a pas pu être testé** : le worker est
  volontairement arrêté. Le code passe désormais `account` (`worker/tasks.py:230`) ; seul le chemin
  API a été exécuté réellement.

---

## Sécurité du périmètre — confirmation

- **`mailia-worker` : `Exited (137)`. `mailia-beat` : `Exited (0)`.** Vérifié après chacun des deux
  déploiements. Aucun `docker compose up -d` sans nom de service n'a été lancé.
- **Compte 1 (utilisateur 2) intact**, vérifié en base après tous les tests :
  `sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51.760324` (inchangé), 17 dossiers locaux,
  **1227 emails locaux** (inchangé).
- Aucune écriture ni lecture de contenu sur le compte 1. La seule requête le concernant est un
  `SELECT le.id ... WHERE lf.account_id <> 3 LIMIT 1` en lecture seule, qui a retourné un **entier**
  (`7301`) et aucune donnée d'email — nécessaire pour prouver que l'IDOR est bien refermé.
- Tous les tests fonctionnels ont été menés avec le token QA (utilisateur 5) sur le compte 3.

## Données de test laissées / nettoyées

Nettoyé : dossiers locaux `QA-Fix`, `QA-Fix/Sub`, `QA-Imp` ; compte temporaire `QA Fix Temp` ;
règles classiques et IA de test ; provider IA de test ; contact et signature de test.
Le compte 3 n'a plus aucun dossier local.

Laissé en place sur le compte 3 (effets de bord des tests, tous volontaires) : INBOX à 15 messages,
`QATestRenamed` à 2, `Trash` à 3, un email transféré dans la boîte GreenMail `ailleurs@qa.example`,
et une réponse de test dans `Sent`.

---

# Addendum — revue du chef de projet sur `mcp/server.py`

## 1. Les 4 `STORE` étaient déjà corrigés

Le point principal de la revue (« `src/mcp/server.py` conserve 4 commandes `STORE` non
parenthésées ») porte sur une version antérieure au déploiement. Ces 4 occurrences faisaient partie
du correctif **S+2** du rapport initial, appliquées et expédiées lors du premier cycle. État vérifié
sur les quatre emplacements possibles :

```
LOCAL /var/www/mailia/src/mcp/server.py     1175 "(\\Seen)"  1203 "(\\Seen)"  1609 "(\\Deleted)"  1655 "(\\Deleted)"
SERVEUR .../mailia/src/mcp/server.py        idem
IMAGE mailia-api  (/app/src/mcp/server.py)  idem
IMAGE mailia-mcp  (/app/src/mcp/server.py)  idem
```

Ce que la revue a mis au jour en revanche, c'est que le **conteneur `mailia-mcp` n'avait pas été
reconstruit** — c'est désormais fait (`docker compose build api mcp && docker compose up -d api mcp`).

## 2. Sûreté de `delete` et `move` côté MCP — analyse

La revue demandait d'appliquer aux lignes 1609 et 1655 « la même logique que dans `manager.py` :
vérifier le statut du `STORE`, ne pas laisser de `COPY` orphelin ». Après lecture du code, le
scénario décrit ne correspond pas à ce que font ces deux lignes :

- **Ligne 1609 (`delete_draft`)** — suppression définitive dans `Drafts`. **Aucun `COPY` n'est
  effectué**, il n'y a donc pas de copie orpheline possible. Le statut du `STORE` **était déjà
  vérifié** (`if status == "OK"`). Rien à corriger.
- **Ligne 1655 (`update_draft`)** — ce n'est pas un déplacement après `COPY`, mais un
  « supprimer l'ancien puis enregistrer le nouveau ». Le risque réel est **inverse** de celui
  décrit : le résultat du `STORE` était **ignoré**, donc si la suppression de l'ancien brouillon
  échouait, le nouveau était quand même créé → **deux brouillons**, avec un retour `"updated"`.
  C'est bien une duplication, par un autre mécanisme. **Corrigé.**
- **Le scénario « archive ces 50 emails » de la revue ne passe pas par ces lignes.** Aucun `COPY`
  brut n'existe dans `mcp/server.py` (vérifié : `grep` sur toutes les commandes `_conn.*`). Les
  13 outils MCP de déplacement/suppression délèguent tous à `manager.py`
  (`move_email` l.703, `move_emails_bulk` l.745/947/1013/1264, `delete_email` l.817,
  `delete_emails_bulk` l.849/897, `flag_email`/`mark_read`/… l.784-791) — donc au code **déjà
  durci** au premier cycle (vérification du statut + `_rollback_copy` via `COPYUID`). Vérifié
  réellement ci-dessous.

## 3. Audit des autres commandes IMAP de `mcp/server.py`

Passage en revue des 40 appels `_conn.{uid,select,search,fetch,append,create,...}` du fichier.

| Constat | Verdict |
|---|---|
| `APPEND` avec `flags="\\Seen"` non parenthésé (l.3330) | **Faux problème.** `imaplib.append()` parenthésise lui-même : `if (flags[0],flags[-1]) != ('(',')'): flags = '(%s)' % flags`. Seul `uid("STORE", ...)` ne le fait pas — c'est bien la racine de F-02. |
| `search()`/`fetch()` par numéros de séquence (l.3099-3111, 3211-3223, 3295-3300) | Cohérent : `search` sans UID suivi de `fetch` sans UID. Correct. |
| `SEARCH`/`FETCH` par UID ailleurs | Cohérents, statuts vérifiés. |
| `select(f'"{fname}"')` (l.3101, 3213) et `append/select/create(f'"{encoded}"')` (l.3287-3330) | **Défaut réel** : quoting artisanal au lieu de `_imap_quote()`, sans échappement de `"` ni `\`. Un dossier contenant ces caractères casse la commande. **Corrigé** (5 emplacements). |
| `copy_local_to_imap` : `try: select(...) except: create(...)` (l.3287) | **Bug réel et actif.** `imaplib.select()` sur un dossier absent renvoie `('NO', ...)` **sans lever d'exception** — vérifié sur GreenMail : `select missing -> ('NO', [b'SELECT failed. No such mailbox'])`. La branche `except` ne pouvait donc **jamais** s'exécuter : le dossier cible n'était jamais créé et l'`APPEND` visait un dossier inexistant. **Corrigé** (test du statut au lieu de l'exception). |

## Correctifs de l'addendum

| # | Fichier:ligne | Correctif | Vérifié | Comment |
|---|---|---|---|---|
| A-1 | `mcp/server.py:1656-1662` | `update_draft` : vérifie que `old_uid` existe (un `STORE` sur un UID inconnu est un no-op **réussi** en IMAP, le seul test du statut ne suffisait pas), puis vérifie le statut du `STORE` avant d'enregistrer le nouveau brouillon. | **OUI** | `old_uid=999999` → `{"status":"failed","detail":"Draft 999999 not found in Drafts"}` et **Drafts reste à 4** (avant correctif : 4 → 5, avec un retour `"updated"` mensonger). `old_uid` valide → v3 remplacé par v4, Drafts reste à 4. |
| A-2 | `mcp/server.py:3287-3298` | `copy_local_to_imap` : `st, _ = select(...)` + `if st != "OK": create/subscribe/select` au lieu d'un `try/except` inopérant. | **OUI** | `copy_local_to_imap` vers `QAFixNouveau` (inexistant) → `{"uploaded":1,...}`, le dossier **est créé** et contient l'email. |
| A-3 | `mcp/server.py:25, 3101, 3213, 3287, 3330` | `_imap_quote()` au lieu du quoting artisanal `f'"{x}"'` (5 emplacements) ; `_imap_quote` remonté dans l'import module. | **OUI** | Non-régression : `find_duplicates_*` et `copy_local_to_imap` exécutés avec succès sur le compte 3. |

## Vérification réelle des outils MCP (via `mcp_runner.py`, `USER_ID=5`)

Garde-fou confirmé d'abord : `{"tool":"mark_read","args":{"account_id":1,...}}` →
`Account 1 not found for user 5`. 67 outils exposés.

| Outil MCP | Avant | Après | Verdict |
|---|---|---|---|
| `mark_unread` uid 17 | `seen=True` | `seen=False` | OK (l.1203) |
| `mark_read` uid 17 | `seen=False` | `seen=True` | OK (l.1175) |
| `move_email` uid 4 | INBOX 15 / QATestRenamed 2 | INBOX **14** / QATestRenamed **3** | **Pas de duplication** |
| `move_emails_bulk` 3 emails | INBOX 11 / QATestRenamed 3 | INBOX **8** / QATestRenamed **6** | **Pas de duplication** — scénario « archive ces N emails » |
| `delete_email` uid 24 | INBOX 14 / Trash 5 | INBOX **13** / Trash **6** | **Pas de duplication** |
| `delete_emails_bulk` 2 emails | INBOX 13 / Trash 6 | INBOX **11** / Trash **8** | **Pas de duplication** |
| `save_draft` | Drafts 2 | Drafts 3 | OK |
| `update_draft` (uid valide) | Drafts 4 | Drafts 4, v3 → v4 | OK, pas de doublon |
| `update_draft` (uid inconnu) | Drafts 4 | Drafts 4, `failed` | **A-1 vérifié** |
| `delete_draft` | Drafts 4 | Drafts 3 | OK (l.1609) |
| `copy_local_to_imap` (dossier absent) | dossier inexistant | créé + 1 email | **A-2 vérifié** |

## État final après l'addendum

- `mailia-api` et `mailia-mcp` reconstruits et redémarrés à partir des sources corrigées.
- **`mailia-worker` : `Exited (137)` — `mailia-beat` : `Exited (0)`.** Toujours arrêtés
  (`docker compose up -d api mcp` nomme explicitement les services).
- **Compte 1 intact** : `sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51.760324`, **1227 emails
  locaux** — identique au relevé initial.
- Non-régression API rejouée après les rebuilds : comptes, dossiers, messages, recherche ES, flags,
  cloisonnement du compte 1 (`404`), IDOR `L7301` (`404`), `403` sans token — tous conformes.
- Nettoyage : brouillons de test supprimés (Drafts revenu à son seul `QADRAFT01`), dossiers locaux
  `QA-Exp` supprimés (0 dossier local sur le compte 3). Laissé en place : le dossier IMAP
  `QAFixNouveau` (1 email), preuve du correctif A-2.

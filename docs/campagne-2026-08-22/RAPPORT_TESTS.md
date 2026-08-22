# Rapport de tests exhaustifs — MailIA

**Date** : 2026-08-22
**Itération** : 1 (première campagne — aucun rapport précédent trouvé)
**Testeur** : agent QA (aucune correction de code effectuée)

## Périmètre et sécurité du test

- Utilisateur QA : `id=5` / `qa@mailia.local` (admin)
- Compte mail de test : `account_id=3` « QA GreenMail » (serveur jetable)
- **Compte réel `account_id=1` (utilisateur 2) : jamais modifié.** Vérification finale en base :
  `sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51` (inchangé), 1227 emails locaux (inchangé).
- **Divulgation** : l'appel à `GET /api/admin/status` (endpoint admin, testé au titre de la
  section 18) parcourt **tous** les comptes de l'instance et ouvre une connexion IMAP
  **en lecture seule** (commandes `STATUS (MESSAGES)`) vers chacun, donc aussi vers le compte 1.
  Aucun contenu de message n'a été lu, aucune écriture effectuée. C'est le comportement normal
  de cet endpoint, signalé ici par transparence.
- Le worker et le beat Celery sont restés arrêtés. La logique de synchronisation a été testée en
  exécutant `sync_account(3)` **sur le seul compte 3**, sans démarrer de démon.

## Décompte final

| | Nombre |
|---|---|
| **PASS** | 58 |
| **FAIL** | 17 |
| **SKIP** | 9 |

Dont **1 faille de sécurité critique** (IDOR), **2 régressions bloquantes** (recherche plein
texte, déplacement/suppression d'emails) et **1 bug de la même famille que celui déjà corrigé**
(flag de configuration ignoré à la création de compte).

---

## Tableau récapitulatif

### 2. Comptes et authentification

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Lister les comptes | `GET /accounts/` | PASS | 1 compte QA retourné |
| Créer un compte | `POST /accounts/` | **FAIL** | `smtp_ssl` et `sync_enabled` ignorés → voir F-03 |
| Modifier un compte | `PUT /accounts/{id}` | PASS | `smtp_ssl`, `sync_enabled`, port, nom appliqués |
| Supprimer un compte | `DELETE /accounts/{id}` | PASS | Compte temporaire créé puis supprimé |
| Test identifiants (succès) | `POST /accounts/test-credentials` | PASS | « Connexion IMAP reussie — 5 dossiers trouves » |
| Test identifiants (mauvais mdp) | idem | PASS | `status=error` + message IMAP |
| Test identifiants (hôte inexistant) | idem | PASS | `status=error`, pas de 500 |
| Test IMAP | `POST /accounts/3/test-imap` | PASS | |
| Test SMTP (non-SSL) | `POST /accounts/3/test-smtp` | PASS | **Non-régression du correctif `smtp_ssl` confirmée** |
| Inscription sans token admin | `POST /auth/register` | PASS | 403 `Not authenticated` |
| Connexion mauvais identifiants | `POST /auth/login` | PASS | 401 `Invalid credentials` |
| Mot de passe oublié (anti-énumération) | `POST /auth/forgot-password` | PASS | `{"status":"ok"}` sur adresse inexistante |
| Réinitialisation token invalide | `POST /auth/reset-password` | PASS | 400 `Lien invalide ou expire` |

### 3. Dossiers

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Arbre des dossiers | `GET /accounts/3/folders` | PASS | Décodage UTF-7 (`QA &AMk-l&AOk-ments` → `QA Éléments`), hiérarchie construite |
| Dossiers bruts | `GET /accounts/3/folders-raw` | PASS | `name` + `display_name` |
| Compteurs | `GET /accounts/3/folders-counts` | PASS | `{"INBOX":6,"Sent":5}` — le `count:0` de `/folders` est volontaire (endpoint rapide sans `STATUS`) |
| Créer un dossier | `POST /accounts/3/create-folder` | PASS | Simple, sous-dossier (`QATest.Sub`), accentué |
| Renommer | `POST /accounts/3/rename-folder` | PASS | Sous-dossier suivi automatiquement |
| Renommer un dossier système | idem | PASS | 400 `Cannot move system folder: INBOX` |
| Vider un dossier | `POST /accounts/3/empty-folder` | PASS | 3 messages supprimés (utilise `(\Deleted)` parenthésé) |
| Supprimer dossier système | `POST /accounts/3/delete-folder` | PASS | 400 refusé |
| Supprimer dossier non vide sans `force` | idem | PASS | 409 + inventaire JSON |
| Supprimer dossier vide | idem | PASS | |
| Supprimer avec `force` | idem | SKIP | Défaut GreenMail (voir S-01) |

### 3bis. Consultation des emails

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Lister + pagination | `GET /accounts/3/messages` | PASS | `total=16`, page 3/size 5 → 1 message |
| Tri date asc/desc | idem | PASS | |
| Tri expéditeur asc | idem | PASS | Aaron → Zoe |
| Tri objet asc | idem | PASS | AAA → ZZZ |
| Filtre objet | `filter_subject` | PASS | Insensible à la casse |
| Filtre date | `filter_date` | PASS | |
| Filtre pièce jointe | `filter_attachments` | PASS | 3/16 |
| Filtre spam | `filter_spam` | PASS | 3/16 |
| Filtre répondu | `filter_replied` | PASS | 0 |
| **Filtre expéditeur** | `filter_from` | **FAIL** | Voir F-04 |
| **Filtre destinataire** | `filter_to` | **FAIL** | Même cause (F-04) |
| Recherche texte IMAP | `?q=budget` | PASS | |
| Dossier inexistant | idem | PASS | 502 avec message explicite |
| Lire un email | `GET /accounts/3/message/{uid}` | PASS | En-têtes, corps texte+HTML, PJ, `tech_headers`, score spam |
| Email inexistant | idem | PASS | 404 |
| Télécharger une PJ | `.../attachment/{i}` | PASS | `Content-Disposition`, `nosniff`, contenu exact |
| PJ index invalide | idem | PASS | 404 |
| **Date affichée** | liste vs détail | **FAIL** | Voir F-05 |
| Export d'un dossier | `GET /accounts/3/folder-export` | PASS | ZIP de 19 `.eml` |

### 3ter. Actions sur les emails

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| **Marquer lu / non lu** | `POST .../message/{uid}/flags` | **FAIL** | Voir F-02 |
| **Marquer important / retirer** | idem | **FAIL** | Voir F-02 |
| **Marquer répondu** | idem | **FAIL** | Voir F-02 |
| **Déplacer IMAP→IMAP** | `POST .../message/{uid}/move` | **FAIL** | Duplication — voir F-02 |
| **Supprimer un email** | `DELETE .../message/{uid}` | **FAIL** | Duplication — voir F-02 |
| **Suppression en masse** | `POST /accounts/3/delete-bulk` | **FAIL** | Duplication — voir F-02 |
| Déplacement en masse | — | SKIP | Aucun endpoint HTTP (MCP uniquement) |

### 4. Recherche

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| **Recherche plein texte ES** | `GET /search/` | **FAIL** | 500 dès qu'il y a ≥1 résultat — voir F-01 |
| Recherche sans résultat | idem | PASS | 200, `total=0` |
| **Recherche, index absent** | idem | **FAIL** | 500 au lieu de résultat vide — voir F-06 |
| Filtres ES (dossier/PJ/expéditeur/dates/pagination) | idem | **FAIL** | Bloqués par F-01 |
| Recherche multi-dossiers IMAP | `POST /accounts/3/search-multi` | PASS | 3 résultats sur 2 dossiers, champ `spam` présent |
| Multi-dossiers, dossier invalide | idem | PASS | Erreur remontée par dossier dans `errors[]` |
| Recherche sémantique | — | SKIP | Aucun endpoint HTTP (MCP uniquement) |

### 5. Rédaction et envoi

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Envoi complet | `POST /accounts/3/send` | PASS | To/Cc/Bcc, texte+HTML, PJ ; Bcc absent des en-têtes livrés |
| Priorité | idem | PASS | `X-Priority: 1`, `Importance: High`, `X-MSMail-Priority` |
| Accusé de lecture | idem | PASS | `Disposition-Notification-To` |
| Accusé de réception | idem | PASS | `Return-Receipt-To` |
| Copie dans Sent | idem | PASS | |
| Réponse (fil de discussion) | idem | PASS | `In-Reply-To` + `References` corrects |
| **Marquage « répondu » de l'original** | — | **FAIL** | Voir F-11 |
| Sauvegarde de brouillon | `POST /accounts/3/save-draft` | PASS | Dossier `Drafts` créé, brouillon visible |
| Modification / suppression de brouillon | — | SKIP | Aucun endpoint HTTP (MCP uniquement) |

### 6. Stockage local

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Créer dossier local | `POST /accounts/3/local-folders` | PASS | Racine + sous-dossier |
| Doublon | idem | PASS | 409 |
| Lister emails locaux | `GET .../messages?storage=local` | PASS | UID préfixés `L` |
| Lire un email local | `GET .../message/L{id}?storage=local` | PASS | |
| Flag sur email local | `POST .../flags?storage=local` | PASS | |
| Déplacer local→local | `POST .../move` | PASS | Vérifié dans les deux dossiers |
| **Déplacer IMAP→local** | idem | **FAIL** | 500 — voir F-07 |
| Déplacer local→IMAP | idem | **FAIL** | Fonctionne mais perd la date — voir F-08 |
| Supprimer dossier local | `DELETE .../local-folders/{id}` | PASS | Cascade OK, 0 email orphelin |
| Dossier local inexistant | idem | PASS | 404 |
| **Cloisonnement `storage=local`** | plusieurs | **FAIL** | **Faille critique — voir F-00** |
| Détection de doublons IMAP↔local | — | SKIP | MCP uniquement |
| Purge des dossiers locaux vides | — | SKIP | MCP uniquement |

### 7. Import

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Import mbox → local | `POST /accounts/3/import-mbox?storage=local` | PASS | 3/3 importés |
| Import ZIP → IMAP | idem `?storage=imap` | PASS | 5/5, 2 dossiers créés |
| Dates préservées | idem | PASS | `INTERNALDATE` posé depuis l'en-tête `Date:` |
| Dédoublonnage (ré-import) | idem | PASS | 5 skipped, 0 importés |
| Suivi de job | `GET /accounts/import-jobs/{id}` | PASS | Progression détaillée + `folders_done` |
| Historique des jobs | `GET /accounts/import-jobs` | PASS | |
| Reprise d'un job interrompu | `POST .../import-resume/{id}` | PASS | Dossier déjà traité correctement sauté |
| Reprise sur job terminé | idem | PASS | 400 refusé |
| Reprise job inexistant | idem | PASS | 404 |
| Import depuis chemin serveur | `POST .../import-path` | SKIP | Non testé (voir S-04) |

### 8. Signatures

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| CRUD signatures | `/signatures/` | PASS | |
| Défaut exclusif | `PUT /signatures/{id}` | PASS | Un seul `is_default=true` à la fois |
| Résolution : contact | `GET /signatures/resolve?emails=` | PASS | Priorité contact respectée |
| Résolution : groupe | idem | PASS | |
| Résolution : défaut | idem | PASS | Destinataire inconnu → signature par défaut |
| Résolution multi-destinataires | idem | PASS | |
| **Détacher via `signature_id: null`** | `PUT /contacts/{id}` | **FAIL** | Voir F-12 |

### 9. Contacts et groupes

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| CRUD contacts (multi-emails) | `/contacts/` | PASS | |
| CRUD groupes | `/contacts/groups` | PASS | |
| Ajout / retrait de membres | `.../members` | PASS | `member_count` cohérent |
| Autocomplétion (nom, email) | `/contacts/autocomplete` | PASS | Dédoublonnage par email, `q` vide → `[]` |
| Contact inexistant | `DELETE /contacts/{id}` | PASS | 404 |

### 10. Détection de spam

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Scan (non-streaming) | `POST /accounts/3/spam-scan` | PASS | 3/18, scores et raisons détaillés |
| Scan (streaming ndjson) | `?stream=true` | PASS | `folder_start`/`progress`/`folder_done`/`result` |
| Détection en-têtes SpamAssassin | idem | PASS | `x_spam_flag`, `spamassassin_yes`, `spam_score_12.5` |
| Détection `Authentication-Results` | idem | PASS | `spf_fail, dkim_fail, dmarc_fail, multi_auth_fail` → score 8.0 |
| Heuristiques objet | idem | PASS | `lottery_scam`, `urgency`, `excessive_exclamation` |
| Liste blanche (email + domaine) | `/spam-whitelist` | PASS | Effet vérifié : 3 spams → 1 |
| Liste noire (email + domaine) | `/spam-blacklist` | PASS | Effet vérifié : force le classement spam |
| Suppression par id / par expéditeur | idem | PASS | |

### 11. Règles automatiques

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| CRUD règles classiques | `/rules/classic/` | PASS | Création, modification, suppression, 404 |
| Validation champ/opérateur/action | idem | PASS | 422 sur chacun |
| Validation conditions/actions vides | idem | PASS | 422 |
| Tri par priorité | `GET /rules/classic/` | PASS | |
| Opérateur `contains` | apply | PASS | 2/19 attendus |
| Opérateur `not_contains` | apply | PASS | 7 |
| Opérateur `equals` | apply | PASS | 1 (sur adresse nue) |
| Opérateur `domain_is` | apply | PASS | 2 |
| Opérateur `starts_with` | apply | PASS | 7 |
| Opérateur `ends_with` | apply | PASS | 1 |
| `has_attachments is_true/is_false` | apply | PASS | 4 / 14 |
| `is_spam greater_than` | apply | PASS | 3 |
| `is_reply is_false` | apply | PASS | 18 |
| `size greater_than / less_than` | apply | PASS | 1 / 16 |
| `age older_than / newer_than` | apply | PASS | 3 / 11, cohérents avec les dates injectées |
| Champ `cc` | apply | PASS | 1 |
| **Champ `to` multi-destinataires** | apply | **FAIL** | Voir F-09 |
| Mode `all` / `any` | apply | PASS | 0 / 2 |
| Valeurs invalides (`age`, `size`) | apply | PASS | 0 correspondance, pas d'erreur |
| **Action `forward`** | apply | **FAIL** | Voir F-10 |
| **Actions `mark_read`/`mark_flagged`/`delete`** | apply | **FAIL** | Bloquées par F-02 |
| **Action `move`** | apply | **FAIL** | Duplication (F-02) |
| CRUD règles IA | `/rules/` | PASS | |
| Preview / parsing markdown | `POST /rules/{id}/preview` | PASS | Conditions, mots-clés, `needs_ai`, `notify` corrects |
| **Parsing : nom de dossier** | idem | **FAIL** | Mis en minuscules — voir F-13 |
| **Parsing : action `transferer`** | idem | **FAIL** | Non reconnue — voir F-14 |
| Aperçu des règles classiques | — | SKIP | Aucun endpoint « preview » côté classique |

### 12. Synchronisation

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Déclenchement manuel | `POST /accounts/3/sync` | PASS | 200 `sync_started` (tâche mise en file) |
| Exécution de la synchro | `sync_account(3)` direct | PASS | 26 documents indexés dans `mailia-5` |
| Indexation ES | vérif. directe ES | PASS | Dates issues de l'en-tête `Date:` |
| Synchro automatique (beat) | — | SKIP | Worker/beat volontairement arrêtés |

### 13/14. IA et digest

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Lister les providers | `GET /ai/providers` | PASS | Clé API jamais exposée |
| CRUD providers | `/ai/providers` | PASS | |
| Test de connexion | `POST .../{id}/test` | PASS | Erreur structurée + latence |
| Diagnostic | `POST .../{id}/diagnose` | PASS | 5 vérifications (voir remarque R-03) |
| Chat streaming (erreur propre) | `POST /ai/chat/stream` | PASS | `data: {"error": ...}` puis `[DONE]` |
| **Chat non-streaming** | `POST /ai/chat` | **FAIL** | 500 brut — voir F-15 |
| **Chat, provider_id inexistant** | idem | **FAIL** | 500 au lieu de 404 (F-15) |
| **Provider `claude-native`** | `/ai/chat/stream` | **FAIL** | `Bearer ` vide — voir F-16 |
| Digest hebdomadaire (IA HS) | `GET /digest/weekly` | PASS | 502 propre `AI analysis failed` |
| Validation `days` | idem | PASS | 422 au-delà de 30 |

### 18. Administration

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| Lister les paramètres | `GET /admin/settings` | PASS | |
| Modifier les paramètres | `PUT /admin/settings` | PASS | Format liste ; valeurs restaurées après test |
| Clé inconnue | idem | PASS | 400 |
| Masquage des secrets | idem | PASS | `sk-ant-QATEST123` relu `sk-***123` |
| Lister les utilisateurs | `GET /admin/users` | PASS | |
| Modifier un utilisateur | `PUT /admin/users/{id}` | PASS | |
| Auto-retrait des droits admin | idem | PASS | 400 `Cannot remove your own admin rights` |
| Utilisateur inexistant | idem | PASS | 404 |
| Informations globales | `GET /admin/info` | PASS | |
| Tableau de statut | `GET /admin/status` | PASS | Comptes, ES, worker `offline`, logs |

### 19. Sécurité

| Fonctionnalité | Endpoint | Résultat | Détail |
|---|---|---|---|
| En-têtes de sécurité | tous | PASS | HSTS, X-Frame-Options DENY, nosniff, XSS, Referrer-Policy, Permissions-Policy |
| Accès sans token | 8 endpoints | PASS | 403 partout |
| Token invalide | `GET /accounts/` | PASS | 401 |
| Endpoints publics | `/health` | PASS | |
| Cloisonnement compte 1 (IMAP) | 6 endpoints | PASS | 404 `Account not found` |
| **Cloisonnement `storage=local`** | 4 endpoints | **FAIL** | **F-00 — critique** |
| Rate limit connexion | `POST /auth/login` | PASS | 429 au 6ᵉ essai (5/min) |
| Rate limit mot de passe oublié | `POST /auth/forgot-password` | PASS | 429 au 4ᵉ essai (3/5 min) |
| Inscription réservée admin | `POST /auth/register` | PASS | 403 |

---

## Détail des échecs

### F-00 — CRITIQUE : cloisonnement absent sur `storage=local` (IDOR)

**Fichier** : `src/api/routes/accounts.py`

Toutes les branches `storage == "local"` retournent **avant** l'appel à `_get_account()`, la seule
fonction qui vérifie `MailAccount.user_id == user.id` :

| Endpoint | Ligne | Requête effectuée |
|---|---|---|
| `list_messages` | `accounts.py:1121-1176` | `LocalFolder.account_id == account_id` — `account_id` vient de l'URL, jamais validé |
| `get_message` | branche locale de `accounts.py:1601` | `LocalEmail.id == email_id` seul |
| `update_flags` | `accounts.py:1932-1944` | `LocalEmail.id == email_id` seul |
| `move_message` (local→local) | `accounts.py:1980-1996` | `LocalEmail.id == email_id` seul |

`_get_account()` n'est appelé qu'à la ligne 1178, après le `return` de la branche locale.

**Preuve (sans lire aucune donnée réelle)** — sonde sur un compte inexistant :

```
GET /api/accounts/99999/messages?folder=INBOX               → 404 {"detail":"Account not found"}
GET /api/accounts/99999/messages?folder=X&storage=local     → 404 {"detail":"Local folder not found"}
POST /api/accounts/99999/message/L999999/flags?storage=local → 404 {"detail":"Email not found"}
GET /api/accounts/99999/message/L999999?storage=local        → 404 {"detail":"Email not found"}
```

Le message d'erreur diffère : en mode `local`, la requête atteint la couche données sans jamais
passer par la vérification de propriété.

**Exploitabilité réelle** (constat en base, lecture seule) : le compte 1 (utilisateur 2) possède
17 dossiers locaux contenant **1227 emails**. N'importe quel utilisateur authentifié — y compris
l'utilisateur non-admin `id=4` `x@x.com` — peut les lire intégralement via
`GET /api/accounts/1/messages?folder=<chemin>&storage=local`, et modifier les flags ou déplacer
n'importe quel email local de l'instance en devinant son `id` numérique.

**Je n'ai délibérément pas exécuté cette requête** : la preuve par le code et par la sonde
ci-dessus est concluante.

**Attendu** : appeler `await _get_account(account_id, user, db)` en tête de chaque endpoint, avant
tout embranchement `storage`, et joindre `LocalEmail → LocalFolder → MailAccount` pour filtrer par
`user_id`.

---

### F-01 — BLOQUANT : la recherche plein texte renvoie 500 dès qu'il y a un résultat

**Fichiers** : `src/api/routes/search.py:68` et `src/search/indexer.py:210`

`indexer.py:210` impose un tri explicite :

```python
"sort": [{"date": {"order": "desc"}}],
```

Avec un `sort` explicite et sans `track_scores`, Elasticsearch renvoie systématiquement
`_score: null`. Or `search.py:68` fait :

```python
score=hit.get("_score", 0),
```

`.get()` retourne `None` (la clé **existe** avec la valeur `null`), et le modèle `SearchResult`
déclare `score: float = 0` → `ValidationError`, transformée en 500.

**Requête** : `GET /api/search/?q=devis`
**Réponse** : `HTTP 500 Internal Server Error`
**Trace** :
```
pydantic_core._pydantic_core.ValidationError: 1 validation error for SearchResult
score
  Input should be a valid number [type=float_type, input_value=None, input_type=NoneType]
```

**Confirmation côté ES** (26 documents indexés) :
```
_score= None | Relance devis projet site web | INBOX | 2026-08-21T23:50:00
_score= None | QA01 Devis annuel Acme        | INBOX | 2026-08-20T09:15:00
```

Toute recherche retournant au moins un résultat échoue. Seule `?q=inexistantxyz` (0 résultat)
répond 200. **La fonctionnalité de recherche est donc totalement inutilisable.**

**Attendu** : `score=hit.get("_score") or 0`.

**Remarque associée** : `docs/FONCTIONNALITES.md` §4 annonce un « tri par pertinence ». Le code
trie exclusivement par date décroissante et le score n'est jamais calculé.

---

### F-02 — BLOQUANT : flags IMAP non parenthésés → marquage impossible, déplacement et suppression dupliquent les emails

**Fichier** : `src/imap/manager.py` lignes 228, 262, 271, 281, 290, 296, 302, 342, 369, 377
et `src/api/routes/rules.py:515`

Les commandes `STORE` sont envoyées sans parenthèses autour de la liste de flags :

```python
self._conn.uid("STORE", uid, "+FLAGS", "\\Deleted")   # manager.py:228
status, _ = self._conn.uid("STORE", uid, "+FLAGS", imap_flag)  # manager.py:271
```

alors qu'ailleurs dans le code la forme parenthésée est bien utilisée
(`accounts.py:910`, `accounts.py:1017`, `mcp/server.py:1120` → `"(\\Deleted)"`), ce qui montre
qu'il s'agit d'un oubli et non d'un choix.

Un serveur IMAP à parseur strict rejette la forme non parenthésée :

```
BAD [b"Expected:'(' found:'\\' Command should be '<tag> UID <fetch-command>|
<store-command>|<copy-command>|<search-command>|<expunge-command>|<move-command>'"]
```

**Conséquences constatées** :

1. **Marquage impossible** — `POST /api/accounts/3/message/17/flags?folder=INBOX`
   avec `{"flag":"seen","action":"add"}` → 502, et l'état reste `seen=False`.
   Idem pour `flagged` et `answered`, en ajout comme en retrait.

2. **Déplacement : duplication de l'email** (`manager.py:220-233`). Le `COPY` réussit, puis le
   `STORE +FLAGS \Deleted` lève une exception → l'original n'est **jamais** supprimé.

   ```
   POST /api/accounts/3/message/24/move?folder=INBOX  {"target_folder":"QATestRenamed"}
   → 502 IMAP error
   INBOX : 17 messages (uid 24 toujours présent)
   QATestRenamed : 1 message (copie de uid 24)
   ```

   L'utilisateur voit une erreur alors que l'email a été **dupliqué**.

3. **Suppression : duplication vers la corbeille** (`manager.py:335-345`). Même mécanisme.

   ```
   DELETE /api/accounts/3/message/26?folder=INBOX → 502
   INBOX : 17 (inchangé)   Trash : 1 (copie)
   ```

4. **Suppression en masse** (`manager.py:355-380`) : `delete-bulk` sur `["22","23"]` → 502,
   INBOX inchangé (17), Trash passé de 1 à 3.

5. **Actions de règles** : `mark_read`, `mark_flagged` et `delete` (`rules.py:510-547`)
   échouent de la même façon ; `move` duplique.

`empty-folder`, qui utilise la forme parenthésée, fonctionne parfaitement (3 messages supprimés).

**Attendu** : parenthéser systématiquement (`"(\\Seen)"`, `"(\\Deleted)"`, `f"({imap_flag})"`)
et, pour `move_email`/`delete_email`, vérifier le statut du `STORE` avant de considérer
l'opération réussie — sinon le `COPY` doit être annulé.

---

### F-03 — `smtp_ssl` et `sync_enabled` ignorés à la création d'un compte (même famille que le bug déjà corrigé)

**Fichier** : `src/api/routes/accounts.py:141-165`

Le constructeur `MailAccount(...)` de `create_account` ne transmet **ni** `smtp_ssl` **ni**
`sync_enabled`, alors que `smtp_ssl` figure bien dans le schéma `MailAccountCreate`
(`accounts.py:32`). `sync_enabled` est carrément absent du schéma. Les deux retombent donc sur les
valeurs par défaut du modèle (`True`).

`update_account` (`accounts.py:227-238`) les gère correctement — l'asymétrie confirme l'oubli.

**Requête** :
```json
POST /api/accounts/
{"name":"QA Temp2", ..., "smtp_host":"greenmail","smtp_port":3025,"smtp_ssl":false, ...}
```
**Réponse** : `"smtp_ssl": true, "sync_enabled": true`

**Impact démontré de bout en bout** :
```
POST /accounts/5/test-smtp  → {"status":"error","message":"[SSL: WRONG_VERSION_NUMBER] ..."}
PUT  /accounts/5  {"smtp_ssl":false}   → smtp_ssl=false
POST /accounts/5/test-smtp  → {"status":"ok","message":"Connexion SMTP reussie (greenmail:3025)"}
```

Un utilisateur qui déclare un serveur SMTP en clair obtient un compte inutilisable à l'envoi tant
qu'il ne repasse pas par une modification. Par ailleurs, tout nouveau compte démarre avec la
synchronisation **activée**, sans possibilité de la désactiver à la création.

---

### F-04 — Le filtre par expéditeur ne cherche que dans le nom affiché

**Fichier** : `src/api/routes/accounts.py:1384-1396`

Le post-filtre appliqué après la recherche IMAP réduit l'adresse à son nom affiché :

```python
def _display_name(addr):
    if '<' in addr:
        name = addr[:addr.index('<')].strip().strip('"').strip("'")
        if name:
            return name
    return addr

_post_filters.append(lambda m, _f=_ff: _f in _display_name(m["from"]).lower())
```

Or la recherche IMAP construite juste avant (`accounts.py:1229`) porte, elle, sur l'en-tête `FROM`
complet. Les deux étages sont donc incohérents : ce que le serveur trouve, le post-filtre le jette.

**Preuve isolée** (recherche IMAP brute vs API, même valeur) :
```
imaplib : SEARCH FROM "alice@acme-corp.example"                    → uid 17
API     : GET /accounts/3/messages?filter_from=alice@acme-corp.example → total=0
```

Filtrer par adresse ou par domaine (`acme-corp`) renvoie donc toujours 0 résultat, alors que
c'est l'usage le plus naturel de ce champ. Le même code s'applique à `filter_to`.

**Attendu** : tester la sous-chaîne sur la valeur brute de l'en-tête (nom **et** adresse).

---

### F-05 — La date affichée dans la liste et dans le détail proviennent de sources différentes

**Fichiers** : `src/api/routes/accounts.py:1350-1356` (liste) vs branche de `get_message`

La liste construit `date_str` à partir de l'**INTERNALDATE** (date d'arrivée sur le serveur), alors
que le `FETCH` récupère pourtant déjà l'en-tête `DATE` (`accounts.py:1334`) sans jamais l'utiliser.
Le détail d'un message, lui, affiche l'en-tête `Date:`.

**Constat sur le message uid 26** (en-tête `Date: Thu, 13 Aug 2026 12:00:00 +0000`) :
```
GET /accounts/3/messages?folder=INBOX     → "date": "2026-08-13 00:00"
GET /accounts/3/message/26?folder=INBOX   → "date": "2026-08-13 12:00"
```

*(Sur ce message précis, l'écart de 12 h vient d'un défaut de GreenMail lui-même — voir S-02 —
mais il rend visible l'incohérence de conception : les deux vues ne s'accordent pas, et
l'indexation Elasticsearch utilise pour sa part l'en-tête `Date:`. Trois sources pour une même
information.)*

Cas d'usage réellement impacté : tout email ré-injecté sur le serveur (restauration depuis le
stockage local, migration, copie inter-comptes) apparaît dans la liste à la date de l'opération,
et dans le détail à sa date réelle.

---

### F-06 — Recherche sur un index Elasticsearch absent : 500 au lieu d'un résultat vide

**Fichier** : `src/api/routes/search.py:41-73`

Le bloc est un `try: ... finally: await es.close()` **sans aucun `except`**. Pour un utilisateur
qui n'a jamais synchronisé, l'index `mailia-{user_id}` n'existe pas :

```
GET /api/search/?q=devis  → HTTP 500
elasticsearch.NotFoundError: NotFoundError(404, 'index_not_found_exception',
  'no such index [mailia-5]', mailia-5, index_or_alias)
```

**Attendu** : intercepter `NotFoundError` et renvoyer `SearchResponse(total=0, results=[])`.

---

### F-07 — Déplacer un email IMAP vers le stockage local échoue systématiquement (500)

**Fichier** : `src/api/routes/accounts.py:2016-2032`

```python
date_val = email.utils.parsedate_to_datetime(msg.get("Date", ""))
...
local_email = LocalEmail(..., date=date_val, ...)
```

`parsedate_to_datetime` renvoie un `datetime` **timezone-aware**, alors que la colonne
`local_emails.date` est un `TIMESTAMP WITHOUT TIME ZONE`. Le pilote **asyncpg** (chemin async)
refuse la conversion :

```
asyncpg.exceptions.DataError: invalid input for query argument $7:
  datetime.datetime(2025, 1, 15, 10, 0, tz... (can't subtract offset-naive and offset-aware datetimes)
```

**Requête** :
```
POST /api/accounts/3/message/20/move?folder=INBOX&storage=imap&target_storage=local
{"target_folder":"QA-Archive"}
```
**Réponse** : `500 Internal Server Error`. L'email reste en INBOX et n'est pas créé en local
(le `commit` échoue avant le `delete_email`) — **pas de perte de données**, mais la fonctionnalité
est inopérante pour tout email possédant un en-tête `Date:` avec fuseau, c'est-à-dire tous.

**Pourquoi l'import local fonctionne malgré un code identique** : `_import_one_message_local`
(`accounts.py:3261`) fait exactement la même chose, mais s'exécute sur la session **synchrone**
(`get_sync_session()` → `psycopg2`, `src/db/session.py:25`), qui accepte le datetime aware et le
convertit silencieusement en UTC. Seul le chemin asyncpg casse.

**Attendu** : normaliser (`date_val.astimezone(timezone.utc).replace(tzinfo=None)`) avant
l'insertion, dans les deux fonctions.

---

### F-08 — Restaurer un email du stockage local vers IMAP écrase sa date

**Fichier** : `src/api/routes/accounts.py:2049-2053`

```python
status, _ = imap._conn.append(
    _imap_quote(req.target_folder), "\\Seen",
    imaplib.Time2Internaldate(time.time()), em.raw_message
)
```

L'INTERNALDATE est fixé à l'instant du déplacement au lieu d'être dérivé de l'en-tête `Date:` —
alors que l'import mbox, lui, le fait correctement (`accounts.py:2817-2822`).

**Constat** : email local `MBOX02` daté du `2024-03-02` déplacé vers INBOX
```
POST .../message/L7464/move?folder=QA-Archive&storage=local&target_storage=imap
→ {"status":"moved"}
INBOX : uid 27 | date affichée 2026-08-22 00:06 | MBOX02 Deuxieme email importe
```

Combiné à F-05 (la liste affiche l'INTERNALDATE), un aller-retour IMAP → local → IMAP fait perdre
le classement chronologique de l'email.

---

### F-09 — Les règles sur le champ `to` ne matchent jamais un email multi-destinataires

**Fichier** : `src/api/routes/rules.py:468-469`

```python
from_addr = str(email.utils.parseaddr(str(msg.get("From", "") or ""))[1])
to_addr   = str(email.utils.parseaddr(str(msg.get("To", "") or ""))[1])
```

`email.utils.parseaddr()` n'accepte qu'**une seule** adresse. Sur un en-tête multi-destinataires,
Python 3.12 renvoie `('', '')` :

```python
>>> email.utils.parseaddr('premier@qa.example, second@qa.example')
('', '')
>>> email.utils.getaddresses(['premier@qa.example, second@qa.example'])
[('', 'premier@qa.example'), ('', 'second@qa.example')]
```

**Preuve** — email `QA11` avec `To: premier@qa.example, second@qa.example` :

| Règle | Attendu | Obtenu |
|---|---|---|
| `to contains premier@qa.example` | 1 | **0** |
| `to contains second@qa.example` | 1 | **0** |

Le champ devient vide : **aucune** condition sur `to` ne peut matcher, même pas sur le premier
destinataire. Incohérence supplémentaire : le champ `cc` (`rules.py:470`) utilise l'en-tête brut
et fonctionne correctement (vérifié : 1 correspondance).

**Attendu** : `", ".join(a[1] for a in email.utils.getaddresses([...]))`.

---

### F-10 — L'action de règle `forward` est validée mais jamais exécutée

**Fichiers** : `src/api/routes/rules.py:215` (validation) et `rules.py:518-547` (exécution)

`forward` figure dans `_VALID_ACTIONS` :
```python
_VALID_ACTIONS = {"move", "mark_read", "mark_flagged", "mark_spam", "delete", "forward"}
```
mais `_execute_actions()` ne comporte aucune branche `elif atype == "forward"`.

**Requête** :
```json
POST /api/rules/classic/          {"name":"QA forward",
  "conditions":[{"field":"subject","operator":"contains","value":"QA11"}],
  "actions":[{"type":"forward","target":"ailleurs@qa.example"}]}   → 200, règle créée

POST /api/rules/classic/32/apply  {"account_id":3,"folder":"INBOX"}
```
**Réponse** : `{"matched":1,"actions":[]}` — HTTP 200.

La règle déclare une correspondance, ne signale aucune erreur, et **ne fait rien**.
`docs/FONCTIONNALITES.md` §11 liste pourtant « transférer » parmi les actions possibles.

**Attendu** : implémenter l'action, ou la retirer de `_VALID_ACTIONS` et de la documentation.

---

### F-11 — Répondre à un email ne le marque jamais comme « répondu »

**Fichiers** : `src/api/routes/accounts.py:1818-1882` et `src/web/static/index.html`

`send_email` ne pose jamais le flag `\Answered` sur le message d'origine, et le frontend ne fait
qu'**afficher** l'indicateur (`index.html:5589` et `:5759`, aucune écriture) — un `grep answered`
sur l'interface ne retourne que ces deux lignes de rendu.

**Constat** : réponse envoyée avec `In-Reply-To` correct sur l'email uid 17 ; celui-ci reste
`answered=false`.

Conséquences : le filtre de colonne « email déjà répondu » (§3 de la documentation), l'icône ↩
et la condition de règle `is_reply` ne reflètent jamais l'activité de MailIA lui-même — seulement
celle d'autres clients mail.

*(À noter : même si le flag était posé, F-02 en empêcherait aujourd'hui l'écriture.)*

---

### F-12 — Impossible de détacher une signature d'un contact ou d'un groupe avec `null`

**Fichier** : `src/api/routes/contacts.py:151-152`

```python
if req.signature_id is not None:
    contact.signature_id = req.signature_id if req.signature_id != 0 else None
```

Le sentinel de détachement est `0`, pas `null`. Envoyer `null` — l'idiome REST naturel, et la
valeur que renvoie l'API en lecture — est traité comme « champ non fourni ».

**Requête** : `PUT /api/contacts/7  {"signature_id": null}`
**Réponse** : `200` avec `"signature_id": 6` (inchangé) — aucune erreur, aucun avertissement.
`{"signature_id": 0}` fonctionne correctement.

L'API répond succès en ayant ignoré la demande : la résolution de signature continue de renvoyer
l'ancienne signature.

---

### F-13 — Le parseur de règles IA met les noms de dossiers en minuscules

**Fichier** : `src/rules/parser.py:142-145`

```python
line_lower = line.lower().strip()
if "deplacer" in line_lower or "move" in line_lower:
    folder = re.search(r"(?:vers|to)\s+(.+)", line_lower)
    if folder:
        actions.append(RuleAction("move", folder.group(1).strip()))
```

La cible est extraite de `line_lower`, donc toujours en minuscules.

**Requête** : règle contenant `- **Alors**: deplacer vers Comptabilite`
**Réponse de `POST /api/rules/5/preview`** :
```json
"actions": [{"type": "move", "target": "comptabilite"}]
```

Les noms de dossiers IMAP sont sensibles à la casse : la règle visera `comptabilite` et non
`Comptabilite` — au mieux échec, au pire création d'un dossier parasite.

**Attendu** : détecter sur `line_lower` mais extraire la cible depuis la ligne d'origine.

---

### F-14 — Le parseur de règles IA ne reconnaît ni `transferer` ni « marquer important »

**Fichier** : `src/rules/parser.py:131-159`

`_parse_actions()` ne gère que `deplacer/move`, `marquer comme lu/mark as read`,
`marquer comme important/mark as important`, `flag`, `extraire/extract`. Il n'existe **aucune**
branche `transferer`/`forward`, alors que la dataclass `RuleAction` documente `forward` comme
type valide (`parser.py:32`).

**Requête** : règle contenant
```
- **Alors**: marquer important
- **Et**: transferer a boss@qa.example
```
**Réponse de preview** : `"actions": []`

Aucune des deux actions n'est retenue, et le preview ne signale rien : la règle est enregistrée,
comptée dans `parsed_count`, affichée comme active, et ne fera jamais rien. La formulation
« marquer important » échoue simplement parce que le parseur exige la chaîne exacte
« marquer comme important ».

**Attendu** : ajouter la branche `forward` et, surtout, remonter au preview les lignes d'action
non reconnues plutôt que de les ignorer silencieusement.

---

### F-15 — `POST /ai/chat` renvoie un 500 brut sur toute erreur

**Fichier** : `src/api/routes/ai.py:493-503`

L'endpoint n'a aucune gestion d'erreur, contrairement à `/ai/chat/stream` qui émet proprement
`data: {"error": ...}` puis `data: [DONE]`.

| Requête | Réponse |
|---|---|
| `POST /api/ai/chat  {"message":"bonjour"}` (provider injoignable) | `500 Internal Server Error` |
| `POST /api/ai/chat  {"message":"x","provider_id":999999}` | `500 Internal Server Error` |

Le second cas devrait être un `404 Provider not found` : `get_llm_for_user()` n'effectue aucune
validation de l'identifiant fourni.

---

### F-16 — Le provider `claude-native` envoie un en-tête `Bearer` vide et plante

**Fichier** : `src/ai/providers/claude_native_provider.py:58` et `:84`

```python
"Authorization": f"Bearer {self.api_key}",
```

Ce provider est décrit dans `docs/FONCTIONNALITES.md` §13 comme fonctionnant « sans clé API ».
Sans clé, l'en-tête devient `Bearer ` (avec espace final), rejeté par httpx.

**Requête** : `POST /api/ai/chat/stream  {"message":"Dis juste OK","provider_id":6}`
**Réponse** : `data: {"error": "Illegal header value b'Bearer '"}`

C'est un défaut **distinct** du problème connu d'authentification du CLI : ici la requête n'est
même jamais émise. Un provider `claude-native` ne peut pas être configuré tel que documenté.

**Attendu** : n'ajouter l'en-tête `Authorization` que si `api_key` est non vide.

---

## SKIP — non testé et pourquoi

| # | Élément | Raison |
|---|---|---|
| S-01 | `delete-folder?force=true` sur `QATestRenamed` | Échec reproduit **en imaplib brut** hors MailIA : `M.delete('"QATestRenamed"')` → `abort command: DELETE => socket error: EOF`. Défaut du serveur GreenMail (le dossier avait été renommé), pas de MailIA. La suppression d'un dossier vide fraîchement créé fonctionne parfaitement. |
| S-02 | Écart de 12 h sur l'INTERNALDATE d'un message | GreenMail stocke `13-Aug-2026 00:00:00` pour un APPEND à 12:00 (horloge 12 h côté serveur), vérifié par FETCH direct. Défaut GreenMail. A néanmoins servi à révéler F-05. |
| S-03 | Synchronisation automatique (Celery beat, cycle 5 min) | Worker et beat volontairement arrêtés pour protéger le compte réel. Seule la logique de synchro a été exécutée manuellement sur le compte 3. `/admin/status` confirme `worker: offline`. |
| S-04 | `POST /accounts/{id}/import-path` | Import depuis un chemin arbitraire du serveur ; réservé aux admins. Non exécuté pour éviter toute lecture de fichier hors périmètre QA. |
| S-05 | Outils MCP (~74 outils `mcp__mailia__*`) | Le serveur MCP configuré dans cet environnement est rattaché au compte réel de l'utilisateur. Les invoquer aurait agi sur `account_id=1`. Interdit par le périmètre. |
| S-06 | Recherche sémantique (kNN / `dense_vector`) | Aucun endpoint HTTP : `semantic_search` est importé dans `search.py` mais jamais exposé ; accessible uniquement via MCP (S-05). |
| S-07 | Brouillons : mise à jour, liste, suppression | Seul `save-draft` existe côté API. `update_draft`, `delete_draft`, `list_drafts` sont des outils MCP (S-05). La documentation §5 les présente pourtant comme des fonctionnalités de l'application. |
| S-08 | Bot Telegram et notifications | Nécessite un token de bot Telegram et un chat lié. Aucun token configuré (`telegram_bot_token` vide). |
| S-09 | Interface web, WebSocket `/ws` et pont `/ai-bridge`, thèmes, raccourcis clavier | Hors du périmètre API demandé ; nécessite un pilotage navigateur. |

---

## Remarques mineures (sans statut PASS/FAIL)

- **R-01** — `create_folder` lors d'un import ZIP crée des dossiers dont le nom contient
  littéralement `/` (`Archives/Clients`) alors que le séparateur annoncé par le serveur est `.`.
  Le résultat n'est pas imbriqué dans l'arbre. Fonctionnel sur GreenMail, à surveiller ailleurs.
- **R-02** — `GET /accounts/3/folders` retourne les dossiers locaux dans une liste plate
  (`children` toujours vide) ; la hiérarchie n'est reconstruite que pour les dossiers IMAP.
- **R-03** — Dans `POST /ai/providers/{id}/diagnose`, la vérification « MCP Server » est
  rapportée `ok` alors que le détail indique `HTTP 404 a https://mailia.expert-presta.com/mcp/sse`.
  Un 404 ne devrait pas être un succès.
- **R-04** — La protection des dossiers système (`rename-folder`, `delete-folder`) compare le
  dernier segment du chemin à `{INBOX, Sent, Drafts, Trash, Junk, Spam}` : un dossier
  utilisateur nommé `Archives.Sent` serait refusé à tort.
- **R-05** — Aucun endpoint d'aperçu (« preview ») pour les règles **classiques** : `apply`
  exécute toujours les actions. La documentation §11 annonce cet aperçu.
- **R-06** — `_build_mime_message` ne devine pas le type MIME des pièces jointes : un
  `note.txt` est envoyé en `application/octet-stream`.
- **R-07** — L'en-tête `MIME-Version` est émis deux fois dans les messages envoyés.
- **R-08** — Le stockage local convertit les dates en UTC sans les réafficher dans le fuseau
  d'origine : un email daté `10:00 +0100` s'affiche `09:00`.
- **R-09** — `POST /accounts/{id}/sync` répond `200 {"status":"sync_started"}` même lorsqu'aucun
  worker Celery n'est disponible ; la tâche reste en file indéfiniment sans retour à l'utilisateur.
- **R-10** — Plusieurs erreurs IMAP brutes sont renvoyées telles quelles au client
  (`502 {"detail":"IMAP error: UID command error: BAD [...]"}`), exposant des détails
  d'implémentation serveur.

---

## Données de test laissées en place

Sur le compte QA (`account_id=3`) uniquement :
- INBOX : 20 messages (6 préexistants + QA01→QA11 + envois de test)
- Dossiers IMAP créés : `QA Éléments`, `QATestRenamed`, `Trash`, `Drafts`,
  `Archives/Clients`, `Archives/Fournisseurs`
- Index Elasticsearch `mailia-5` : 26 documents
- Contacts : `Alice Martin` ; groupes : `QA Clients Acme`, `Tous`
- Signatures : `QA Defaut`, `QA Groupe devenu defaut`, `QA Contact`
- Jobs d'import : 4 (dont `qaresume`)

Nettoyés : toutes les règles classiques et IA de test, les providers IA de test, les dossiers
locaux `QA-Archive` et `QA-Archive/2026`, les entrées de liste blanche/noire, le compte temporaire
`QA Temp2`, et les paramètres système modifiés (`app_name`, `anthropic_api_key`) restaurés à vide.

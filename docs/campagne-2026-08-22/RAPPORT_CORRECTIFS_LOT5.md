# Rapport de correction — Lot 5 (final) + récapitulatif de remise

**Date** : 2026-08-22
**Source** : `RAPPORT_TESTS_ITER3.md`
**Déploiement** : effectué (lots 4 **et** 5). `api` et `mcp` reconstruits ; `worker` et `beat`
jamais démarrés. **File Celery vérifiée à 0 après déploiement**, sauvegarde intacte à 26 940.

## Résultat

| | |
|---|---|
| Constats du lot 5 traités | **5** (IT3-01 à IT3-05) |
| Correctifs non re-vérifiés désormais vérifiés | **3 / 4** (N-12, N-13, N-16 ; N-09 reste bloqué par GreenMail) |
| Correctifs du lot 4 vérifiés après déploiement | **tous** |
| Bug de la même classe qu'IT3-01 trouvé pendant l'audit | **1** (latent) |

---

## Tableau

| Bug | Fichier:ligne | Correctif | Vérifié | Comment |
|---|---|---|---|---|
| **IT3-01** `NameError` sur `re` | `mcp/server.py:11` | `import re` remonté au niveau module, **les 5 imports locaux supprimés**. | **OUI** | `S._looks_like_phone('0612345678')` → `True` hors de tout contexte d'appel ; `'2026-09-15'` → `False`. `contact_from_email` sur l'email uid 52 (celui qui échouait) → `"phones": ["01 23 45 67 89"]`. |
| **IT3-01 bis** même piège, latent | `mcp/server.py:310` | `_encode_query()` (niveau module) utilisait `EMBEDDING_MODEL_NAME`, importé **localement** dans `semantic_search`. Import déplacé dans la fonction qui s'en sert. | Statique | Trouvé par l'audit AST ci-dessous, pas signalé par le testeur. Latent : le chemin n'est atteint qu'une fois des embeddings présents — donc jamais aujourd'hui. Aurait mordu le jour où N-05 serait implémenté. |
| **IT3-04** `SELECT` non vérifié | `imap/manager.py:104` (`_select`), 14 sites ; `mcp/server.py`, 17 sites ; middleware | Nouvelle exception `FolderNotSelectable` et méthode `IMAPManager._select()` qui teste le statut. Les 14 `SELECT` de `manager.py` et les 17 du serveur MCP y passent. Le middleware MCP convertit `FolderNotSelectable` et `imaplib.IMAP4.error` en `ToolError` — un point de passage unique pour les 67 outils. | **OUI** | `move_email`, `delete_email`, `mark_read`, `mark_unread`, `list_emails` sur un dossier inexistant → `Cannot open folder '__NX__' — check that it exists`. `read_email` → son message dédié. Avant : `command COPY illegal in state AUTH` / `command STORE illegal in state AUTH`. |
| **IT3-02** expéditeur du mail de réinitialisation | `auth.py:113-131` | `ORDER BY id` sur les deux requêtes + trace explicite du compte retenu. **Je n'ai pas changé qui envoie** — voir ci-dessous. | Partiel | `forgot-password` sur une adresse inconnue → `{"status":"ok"}` sans aucun accès SMTP (anti-énumération préservée). Le chemin avec effet de bord n'est **délibérément pas déclenché** : il enverrait un mail depuis la messagerie professionnelle réelle. |
| **IT3-03** message STARTTLS inatteignable | `smtp_client.py:21-31` | L'échec du TLS implicite hors port 465 est capturé et re-levé avec un message actionnable, au lieu de propager `WRONG_VERSION_NUMBER`. | Partiel | `py_compile` OK ; non déclenchable sur GreenMail sans casser la configuration du compte QA. Non-régression vérifiée : les chemins nominaux (`test-smtp`, `test-credentials` en clair, envoi API) répondent tous `ok`. |
| **IT3-05** deux modèles d'isolation | `mcp/server.py:43` (`_assert_account`) + 5 outils | Je partage la lecture — **ce n'était pas un défaut**. J'ai quand même aligné : les 5 outils valident `account_id` quand il est fourni. La redondance rend l'invariant **uniforme et testable de la même façon partout**, ce qui est sa vraie valeur. | **OUI** | `count_emails`, `search_emails`, `get_folders_stats`, `get_senders_stats`, `get_processing_logs` avec `account_id=1` → tous `Account 1 not found for user 5`. Le prochain audit de cloisonnement couvrira 58/58 au lieu de 53 + 5 raisonnés à la main. |

---

## IT3-01 — l'audit d'exhaustivité que tu demandais

J'ai écrit une passe AST sur les 19 fichiers touchés par la campagne, cherchant tout nom chargé
dans une fonction sans être lié ni localement ni au niveau module.

Première passe : 38 signalements, **tous des fermetures** (fonctions imbriquées capturant les
locales de leur parent : `_fetch_messages` sur `conn`, `_flush_non_ascii` sur `non_ascii`…). Le cas
dangereux est différent : une fonction **de niveau module**, qui n'a aucun parent d'où capturer.
En restreignant aux fonctions de niveau module :

```
19 fichiers, fonctions de niveau module uniquement
  src/mcp/server.py:311  _encode_query()  -> EMBEDDING_MODEL_NAME
  1 cas
```

Un seul, et c'était encore un des miens (N-05). Corrigé. Après correction, la passe ne signale plus
rien. Les trois faux positifs que le testeur mentionnait (`engine.py:58`, `contacts.py:371/385`) ne
ressortent pas non plus : ce sont bien des variables locales nommées `email`.

**La leçon structurelle** : 5 imports locaux du même module standard donnaient l'illusion que `re`
était disponible partout. C'est exactement le motif que tu demandais de supprimer, pas seulement de
corriger — c'est fait, il n'en reste aucun.

## IT3-04 — pourquoi la famille est plus large, et une nuance importante

Le recensement a trouvé **~45 `SELECT` non vérifiés** : 14 dans `manager.py`, 19 dans
`mcp/server.py`, 7 dans `rules.py`, 5 dans `accounts.py`.

**Nuance qui change la gravité** : `imaplib.select()` remet l'état de la connexion à `AUTH` quand il
échoue (`self.state = 'AUTH'  # Might have been 'SELECTED'`). Il n'y a donc **aucun risque d'agir
sur le dossier précédemment sélectionné** — la commande suivante échoue toujours. Le préjudice est
entièrement dans le message, jamais dans les données. C'est ce qui m'a permis de traiter la famille
par une correction unique plutôt que par 45 revues individuelles.

Correction en trois points :

1. `IMAPManager._select()` teste le statut et lève `FolderNotSelectable` avec le nom du dossier.
2. Les 14 sites de `manager.py` et les 17 sites de `mcp/server.py` y passent. Trois sites restent
   volontairement en `select()` brut : celui de `delete_folder` (`server.py:1205`, qui lit le
   nombre de messages depuis la réponse), celui de `copy_local_to_imap` (`:3432`, dont le test de
   statut **est** la logique de création du dossier), et `_select_folder` d'`accounts.py`, dont les
   trois stratégies de repli sur les noms accentués reposent précisément sur l'inspection du
   statut. Les convertir aurait cassé leur logique.
3. Le middleware MCP normalise `FolderNotSelectable` et `imaplib.IMAP4.error` en `ToolError` : un
   seul point de passage couvre les 67 outils, plutôt que 13 enveloppes individuelles.

*Réserve de méthode* : `mcp_runner.py` appelle `tool.fn(**args)` directement et **court-circuite le
middleware**. Les erreurs ci-dessus s'affichent donc comme `FolderNotSelectable` dans mes tests ;
à travers le vrai serveur MCP elles arriveront en `ToolError`. Le message, lui, est le même.

## IT3-02 — ce que j'ai fait, et ce que je n'ai pas fait

Le constat est réel et sérieux : une requête **non authentifiée** sur `/auth/forgot-password` fait
ouvrir au serveur une connexion SMTP authentifiée vers la messagerie professionnelle réelle.

Ce que j'ai fait : rendre le choix **déterministe** (`ORDER BY id`) et **tracé** (le log nomme le
compte utilisé). C'est le minimum que le testeur demandait.

Ce que je n'ai **pas** fait, délibérément :

- **Créer un expéditeur système dédié.** Il n'existe aucun paramètre `smtp_*` global aujourd'hui
  (`system_settings` ne contient que `anthropic_api_key` et `app_name`). En introduire un suppose
  de nouvelles clés, leur chiffrement, et une interface d'administration : c'est une fonctionnalité,
  pas un correctif.
- **Refuser d'emprunter un compte utilisateur.** Ce serait le choix le plus sûr, mais il casserait
  purement et simplement la réinitialisation de mot de passe tant qu'aucun expéditeur n'est
  configuré. Ce n'est pas à moi de le décider.

**Décision à remonter à l'utilisateur** : accepter qu'un mot de passe oublié parte de
`e.pimienta@ebusinet.fr`, ou configurer un expéditeur dédié. Tant que la question n'est pas
tranchée, `ORDER BY id` garantit au moins que le compte retenu est prévisible et journalisé.

## IT3-03 — correction du message, sans repli automatique

`if port == 465 or (use_ssl and port != 587)` envoie tout port non standard vers le TLS implicite.
Le testeur suggérait un repli automatique vers STARTTLS. Je ne l'ai pas fait : sur le cas mesuré
(GreenMail en clair sur 3025) STARTTLS **n'est pas annoncé non plus**, le repli n'aurait donc rien
résolu et aurait ajouté une connexion inutile en masquant une configuration incorrecte.

J'ai traité ce qui aide réellement : `WRONG_VERSION_NUMBER` devient
« *does not speak implicit TLS … Uncheck TLS for this account if the server is plaintext, or use
port 465 for implicit TLS / 587 for STARTTLS* ». Le message nomme le problème et les deux issues.

---

## Vérifications post-déploiement

### Lot 4, resté en attente

| Élément | Résultat |
|---|---|
| **FS-02** `POST /accounts/3/sync` sans worker | **`503`** + « Aucun worker de synchronisation n'est actif : la demande n'a pas ete mise en file… » |
| **FS-02** MCP `trigger_sync(3)` | `ToolError: No sync worker is running: the request was not queued…` |
| **FS-02** effet sur la file | **file celery = 0 après les deux appels** — rien n'a été empilé, ce qui est tout l'objet du correctif |
| **FS-04** registre du sélecteur | HTML servi : `FP_ACTIONS` ×3, `_fpDispatch(this)` ×1, **`onSelect` : 0 occurrence** |
| **R-B** liens IA | `rel', 'noopener noreferrer'` présent |
| **FS-01** case SSL SMTP | `acc-smtp-ssl` ×5 dans le HTML servi |
| Message de synchro côté interface | « Synchronisation impossible » présent |

### Les 4 correctifs non re-vérifiés par le testeur

| # | Résultat |
|---|---|
| **N-12** tri de `scan_for_spam` | `sort(key=lambda x: int(str(x["trust_score"]).split("/")[0]))` en production — tri numérique |
| **N-13** en-tête `Date` des brouillons | Nouveau brouillon → `date = '2026-08-22 02:14'` (au lieu de `''`) |
| **N-16** docstring | « Extract calendar events from an email's iCalendar parts (text/calendar, .ics attachments) » |
| **N-09** purge ES à la suppression d'un dossier | **Toujours non vérifiable de bout en bout.** Nouvelle tentative sur un dossier **fraîchement créé** contenant un email indexé : `delete_folder(force=true)` → `socket error: EOF`. GreenMail refuse le `DELETE` même dans ce cas — l'hypothèse « seuls les vieux dossiers » est donc écartée aussi. Le helper reste vérifié isolément (fantômes → 0). |

*Effet de bord relevé au passage* : `delete_folder(force=true)` vide le dossier (STORE + EXPUNGE)
**avant** de tenter le `DELETE`. Quand le `DELETE` échoue, les messages ont disparu d'IMAP mais leurs
documents restent dans Elasticsearch — la purge n'étant atteinte qu'après un `DELETE` réussi. Sur un
serveur conforme le cas ne se présente pas. J'ai nettoyé le document fantôme produit par mon test.

### Non-régression finale

Comptes, messages avec tri et filtres combinés, recherche ES, `test-smtp`, `test-credentials` en
clair, envoi API, `forgot-password` (anti-énumération), cloisonnement du compte 1 (`404`), IDOR
local `L7301` (`404`), `403` sans token, garde-fou MCP compte 1, 67 outils exposés. **Tout conforme.**

---

# Récapitulatif de remise

## 1. Ce qui existait avant la campagne

Quatre fichiers portaient des modifications **non commitées de l'utilisateur**. Référence : la copie
déployée relevée avant notre premier déploiement.

| Fichier | HEAD → pré-campagne (utilisateur) | pré-campagne → aujourd'hui (campagne) |
|---|---:|---:|
| `src/api/routes/accounts.py` | 101 | 262 |
| `src/api/routes/ai.py` | 17 | 22 |
| `src/mcp/server.py` | 96 | 280 |
| `src/web/static/index.html` | 31 | 237 |

(Chiffres relevés à la fin du lot 4 ; les lots 4-5 ajoutent à la colonne de droite.)
Un `git diff` brut mélangerait les deux colonnes.

Fichiers **non suivis** préexistants, étrangers à la campagne : `docs/FONCTIONNALITES.md`,
`src/web/static/favicon.ico`, `src/web/static/favicon.png`, `src/web/static/index.html.bak`.

## 2. Fichiers modifiés par la campagne

| Fichier | Rôle |
|---|---|
| `src/api/routes/accounts.py` | IDOR stockage local (7 endpoints), `smtp_ssl`/`sync_enabled` à la création, filtre expéditeur, date unique affichage/tri/filtre, dates naïves UTC, INTERNALDATE préservé, flag « répondu » IMAP et local, migration SMTP, refus de synchro sans worker |
| `src/mcp/server.py` | Cloisonnement `move_local_email`, `trigger_sync`, STARTTLS, `semantic_search` explicite, threading des réponses, erreurs enveloppées, regex téléphone, purge ES, scores anti-spam, tri, `Date` des brouillons, brouillon dupliqué, `STORE` parenthésés, `import re`, `SELECT` vérifiés, normalisation des erreurs IMAP, `_assert_account` |
| `src/web/static/index.html` | XSS surlignage, `mdSafe`, `escJs`, `target_storage`, purge spam, échecs en masse, `/accounts/`, export ZIP, sandbox, case SSL SMTP, registre `FP_ACTIONS`, `rel=noopener`, message de synchro |
| `src/imap/manager.py` | Flags parenthésés + rollback du COPY orphelin, tri des UID, `FolderNotSelectable` + `_select()`, `Message-ID`/`References` |
| `src/worker/tasks.py` | Verrou par compte, dispatcheur, `SoftTimeLimitExceeded`, disjoncteur IA ; curseur borné aux UID traités, fermeture d'Elasticsearch, `_uid_still_present` |
| `src/worker/app.py` | Limites de temps, `expires` ; `worker_online()` |
| `src/rules/engine.py` | `AIProviderTimeout` ; `EmailContext.message_id` / `.references` |
| `src/api/routes/rules.py` | Flags parenthésés, `to` multi-destinataires, `forward` implémentée, actions inconnues au preview, migration SMTP |
| `src/api/routes/auth.py` | STARTTLS forcé ; expéditeur déterministe et tracé |
| `src/api/routes/search.py` | Score `null`, index absent |
| `src/api/routes/contacts.py` | Détachement de signature par `null` |
| `src/api/routes/ai.py` | Provider inconnu → 404, erreur provider → 502 |
| `src/api/routes/digest.py` | 500 brut → 502/404 |
| `src/ai/router.py` | `LookupError` sur `provider_id` inconnu |
| `src/ai/providers/claude_native_provider.py` | En-tête `Bearer` vide |
| `src/rules/parser.py` | Casse des dossiers, `transferer`, actions non reconnues exposées |
| `src/search/indexer.py` | `EMBEDDING_DIMS` / `EMBEDDING_MODEL_NAME` |
| `src/mcp/helpers.py` | `get_local_email()` — résolution + propriété |
| `docker-compose.yml` | Worker |

## 3. Fichiers nouveaux

**Un seul** : `src/smtp_client.py` — point d'entrée unique de toute connexion SMTP. Il remplace six
copies de la même logique, dont trois n'étaient pas gardées.

`src/mcp/helpers.py` **n'est pas nouveau** (suivi par git, préexistant ; j'y ai ajouté une fonction).
`mcp_runner.py` n'appartient pas au projet : scratchpad de campagne + `docker cp`, absent du dépôt,
des Dockerfiles et de l'image.

## 4. Migrations et configuration

**Aucune migration de base.** `src/db/models.py` inchangé, aucune révision Alembic ajoutée. Les
colonnes utilisées (`smtp_ssl`, `sync_enabled`, `sync_state`) existaient déjà.

**Aucun changement de configuration requis.** Pas de variable d'environnement nouvelle, aucune
dépendance ajoutée ; `src/smtp_client.py` n'utilise que la bibliothèque standard.

## 5. Ce qui reste non vérifiable tant que le worker est arrêté

| Correctif | Ce qui reste à observer |
|---|---|
| **WK-01** persistance de `sync_state` par lot | Tuer un cycle en cours et vérifier que l'avancement est conservé |
| **WK-02** relais de `SoftTimeLimitExceeded` | Que la limite douce provoque un arrêt propre |
| **WK-03** disjoncteur IA | Que `llm` passe à `None` après 3 expirations |
| **WK-04** verrou par compte | Sérialisation d'une synchro manuelle pendant un cycle |
| **WK-01bis** dispatcheur | Une tâche par compte, budget de temps par compte |
| **WK-06** curseur borné aux UID traités | Qu'un lot en échec ne fasse pas avancer le curseur |
| **WK-08** fermeture d'Elasticsearch | Absence de fuite de session sur compte en erreur |
| **WK-09** tri des UID | Ordre garanti quel que soit le serveur |
| **`_uid_still_present`** | Distinction message disparu / fetch en échec |

`worker_online()` (FS-02) est en revanche **vérifié** : le worker arrêté est justement le cas nominal.

**N-09** reste non vérifiable pour une autre raison : le `DELETE` de GreenMail, y compris sur un
dossier neuf.

Hors périmètre, jamais couverts : bot Telegram, WebSocket `/ws`, pont `/ai-bridge`, qualité des
réponses IA, parcours visuels du frontend.

## 6. En suspens — décisions qui appartiennent à l'utilisateur

1. **Les 26 940 tâches** de `celery_backup_20260822`. Purge définitive ou conservation. **Ne jamais
   restaurer** : elles visent le compte professionnel.
2. **Ne pas démarrer le worker** avant que ce point soit tranché, et vérifier alors les 9 correctifs
   ci-dessus.
3. **Expéditeur des mails de réinitialisation** (IT3-02) : accepter qu'ils partent du compte
   professionnel réel, ou configurer un expéditeur dédié.
4. **`semantic_search`** : les 5 prérequis pour l'activer (génération des vecteurs à l'indexation,
   budget CPU, réindexation complète obligatoire, choix du modèle, `sentence_transformers` dans
   l'image du worker) — détaillés au §4 du rapport du lot 3.
5. **Images distantes à l'affichage des emails** (R-A requalifié) : le vrai écart de confidentialité
   est là, pas dans le chat IA. Un bouton « afficher les images » est une fonctionnalité à décider.
6. **R-01** : `create_folder` peut créer des dossiers dont le nom contient un `/` lors d'un import
   ZIP. Origine réelle de ce que le testeur avait pris pour N-17.
7. **Documentation** : la note ⚠️ §2 de `FONCTIONNALITES.md` (« le SSL SMTP n'est pas configurable
   depuis l'interface ») est devenue fausse — la case existe depuis le lot 3.
8. **Rien n'est commité.** 19 fichiers suivis modifiés, 1 fichier nouveau non suivi. `HEAD` reste à
   `97cc0a5`.

## 7. Sécurité du périmètre

- **`mailia-worker` : `Exited (137)` — `mailia-beat` : `Exited (0)`**, vérifié après chacun des trois
  cycles de build de ce lot. Chaque commande a nommé explicitement `api` et `mcp`.
- **File `celery` = 0** après déploiement et après tous les tests, y compris ceux de `trigger_sync`.
  **`celery_backup_20260822` = 26 940**, jamais touchée.
- **Compte 1 intact** : `sync_enabled=true`, `last_sync_at=2026-05-20 10:05:51.760324`,
  **1227 emails locaux** — identique aux cinq relevés de la campagne.
- Tous les tests menés avec le token QA (utilisateur 5) sur le compte 3.
- Nettoyé : brouillons de test, document Elasticsearch fantôme produit par mon test de N-09.
  Subsiste le dossier IMAP vide `L5Purge` (GreenMail refuse de le supprimer, comme les autres).

---

# Addendum — IT4-01 : perte d'email sur `target_folder` vide

**Déployé et vérifié.** `api` et `mcp` reconstruits ; `worker` et `beat` jamais démarrés ;
**file Celery à 0** après déploiement, sauvegarde `celery_backup_20260822` intacte à 26 940.

## Pourquoi l'email disparaissait — ta troisième question, et la réponse corrige l'hypothèse

Tu supposais que le `STORE +FLAGS (\Deleted)` s'exécutait après un `COPY` échoué, et donc que le
rollback du lot 1 ne couvrait pas ce chemin. **Ce n'est pas cela.** Diagnostic en imaplib direct
contre GreenMail, sur un dossier isolé :

```
CREATE ""      -> ('NO', [b'CREATE failed. Mailbox  already exists.'])
COPY uid ""    -> ('OK', [None])        <-- le serveur DIT que la copie a reussi
STORE +FLAGS   -> ('OK', [b'1 (FLAGS (\Deleted) UID 1)'])
EXPUNGE        -> ('OK', [b'1'])
reste dans le dossier : []
```

Le `COPY` vers `""` **réussit** du point de vue du serveur. Le rollback du lot 1 couvre le cas
« COPY réussi + STORE échoué » : ici les deux réussissent, il n'a donc aucune raison de se
déclencher, et il a raison de ne pas le faire. Le serveur accepte la commande puis jette la copie.

**Conséquence importante** : aucune vérification de statut, à aucun niveau, ne pouvait attraper ce
cas. Il n'y a pas de trou dans le correctif de sécurité existant — il y a une valeur d'entrée qui
n'aurait jamais dû atteindre le fil. C'est pour cela que la correction appartient à la **frontière**,
pas à la logique de rollback.

*(Signal théoriquement détectable : la réponse est `('OK', [None])`, sans `COPYUID`. S'y fier
casserait sur tout serveur sans UIDPLUS — ce n'est pas une garde utilisable.)*

## Correctif — deux couches

| Couche | Fichier | Effet |
|---|---|---|
| **Frontière API** | `accounts.py:24` (`_require_folder_name`) + validateurs Pydantic sur 6 schémas | `422` avant qu'aucune commande ne parte |
| **Défense en profondeur** | `imap/manager.py:24` (`InvalidFolderName`, `_check_target`) sur `move_email`, `move_emails_bulk`, `create_folder`, `rename_folder`, `delete_folder` | couvre le MCP et le moteur de règles, qui ne passent pas par les schémas Pydantic |

`move_email` refuse en plus **source == cible**, converti en `422` par la route.

## La famille, balayée

| Chemin | Traitement |
|---|---|
| `MoveRequest.target_folder` | validateur → 422 |
| `CreateFolderRequest`, `DeleteFolderRequest`, `EmptyFolderRequest` (`folder_name`) | validateur → 422 |
| `RenameFolderRequest` (`old_name`, `new_name`) | validateur → 422 |
| `CreateLocalFolderRequest` (`name`) | validateur → 422 |
| Actions de règle `move` et `forward` sans cible | refusées à la **création** de la règle (422) et à l'exécution |
| MCP `move_email`, `move_emails_bulk`, `search_and_move_emails`, `organize_emails`, `archive_email`, `create_folder`, `rename_folder`, `delete_folder` | délèguent tous à `IMAPManager` → couverts par `_check_target` |
| MCP `copy_local_to_imap` | **seul écrivain direct** (`APPEND`) : garde ajoutée explicitement. Sans elle, `imap_folder=""` + `delete_after=true` aurait supprimé les originaux locaux après un envoi dans le vide. |
| MCP `move_local_email`, `create_local_folder`, `delete_local_folder` | résolution en base : un chemin vide ne correspond à rien, échec naturel. Non destructeurs. |
| Import avec `folder` vide | **laissé tel quel** : `None`/vide y signifie explicitement « dériver l'arborescence du fichier ». Comportement documenté, pas un défaut. |

## Vérification

Scénario `DUP-02` reproduit à l'identique sur le compte 3 (uid 115, INBOX = 80) :

| Appel | Avant | Après |
|---|---|---|
| `move target_folder=""` | `200 {"status":"moved"}`, email détruit | **422** `le nom de dossier ne peut pas etre vide` |
| `move target_folder="   "` | idem | **422** |
| `move target_folder="INBOX"` (= source) | copie + suppression de l'original | **422** |
| `create-folder ""` / `empty-folder "  "` / `delete-folder ""` | acceptés | **422** |
| `rename-folder new_name=""` | accepté | **422** |
| `local-folders name="  "` | accepté | **422** |
| règle `move` / `forward` sans cible | créée | **422** à la création |
| MCP `move_email target=""` | destructeur | `InvalidFolderName: target folder is empty — refusing, this would destroy the message` |

**INBOX : 80 → 80. Aucune perte.** L'email uid 115 est toujours lisible.

**Le test `DUP-02` de la suite passe au vert** :

```
PASS DUP-01  Deplacer un email ne le duplique pas          [F-02]
PASS DUP-02  Un deplacement vers une cible vide ne detruit pas l'email  [IT4-01]
PASS DUP-03  Supprimer un email ne le duplique pas         [F-02]
PASS DUP-05  Les flags sont reellement poses et retires    [F-02]
PASS DUP-06  Les outils MCP ne dupliquent pas              [F-02]
```

Non-régression des chemins légitimes : `move` réel (IT4Bulk 1→0, IT4Ok 0→1), `create-folder`,
`rename-folder`, `empty-folder`, `local-folders`, règle `move` avec cible, MCP `move_email` et
`create_folder` — tous `200`/succès.

### Un échec à ne pas m'attribuer

`DUP-04` a échoué une fois (« les emails n'ont pas ete retires : 3 -> 0 »). Vérifié à la main hors
de la suite sur un dossier dédié : `delete-bulk` de 2 UID sur 3 → **3 → 1**, exactement les deux
demandés. Puis `DUP-04` rejoué **trois fois de suite : PASS, PASS, PASS**. C'est l'instabilité
résiduelle décrite en R-02/R-03 de ma revue de la suite, pas une régression de ce correctif.

## Récapitulatif de remise — mise à jour

Par rapport au récapitulatif du lot 5, cet addendum ne change que la colonne « fichiers modifiés » :

| Fichier | Ajout de ce lot |
|---|---|
| `src/api/routes/accounts.py` | `_require_folder_name` + validateurs sur 6 schémas, `422` sur `InvalidFolderName` |
| `src/imap/manager.py` | `InvalidFolderName`, `_check_target`, gardes sur les 5 méthodes d'écriture |
| `src/mcp/server.py` | `InvalidFolderName` normalisée en `ToolError`, garde sur `copy_local_to_imap` |
| `src/api/routes/rules.py` | cible obligatoire pour `move` et `forward` |

**Inchangé** : toujours **un seul fichier nouveau** (`src/smtp_client.py`), **aucune migration de
base**, **aucun changement de configuration**, **rien de commité** (`HEAD` = `97cc0a5`).
La liste des correctifs non vérifiables tant que le worker est arrêté est inchangée (§5 du lot 5).

## Sécurité du périmètre

- `mailia-worker` : `Exited (137)` — `mailia-beat` : `Exited (0)`, après les deux cycles de build.
- File `celery` = **0** ; `celery_backup_20260822` = **26 940**, jamais touchée.
- Compte 1 : **1227 emails locaux**, inchangé.
- Nettoyé : règle et dossier local de test, script de diagnostic retiré du conteneur et du serveur.
  Subsistent sur le compte 3 les dossiers IMAP de test `IT4Bulk`, `IT4Ok`, `IT4Ok3`, `IT4Mcp`,
  `IT4Diag` (GreenMail refuse leur suppression, défaut serveur documenté).

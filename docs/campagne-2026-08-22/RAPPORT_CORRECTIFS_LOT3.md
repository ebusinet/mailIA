# Rapport de correction — Lot 3 (surface MCP) + vérification du lot 2

**Date** : 2026-08-22
**Sources** : `RAPPORT_TESTS_ITER2.md` (16 bugs MCP + 1 régression) et le lot 2 resté en attente
**Déploiement** : effectué. `api` et `mcp` reconstruits ; `worker` **construit mais jamais démarré**.

## Résultat

| | Nombre |
|---|---|
| Bugs du lot 3 corrigés et vérifiés | **15** |
| Constats requalifiés (faux positifs / non-bugs) | **2** (N-17, N-10) |
| Correctifs du lot 2 vérifiés après déploiement | **13** |
| Failles trouvées **en plus** du rapport | **2** (`trigger_sync`, `auth.py`) |
| Régression introduite par moi puis corrigée avant livraison | **1** |
| Risque d'exploitation majeur découvert (non corrigé, décision requise) | **1** |

---

# ⚠️ À lire en premier — 26 940 tâches de synchronisation armées dans Redis

Découvert en auditant `trigger_sync`. La file Celery contient **26 940 messages en attente**,
presque tous `src.worker.tasks.sync_all_accounts`, **sans expiration** :

```
LLEN celery (db 1) -> 26940
échantillon de 401 : 398 × sync_all_accounts args=()   |  2 × sync_account (3)  |  1 × sync_account (2)
                     expiration : aucune sur les 401
```

Ce sont les cycles de beat accumulés sur ~3 mois (288/jour × 93 jours ≈ 26 800) **avant** que
`expires: 240` ne soit ajouté. Les messages antérieurs ne le portent donc pas.

**Pourquoi c'est grave** : le compte 3 est `sync_enabled=false`, le compte 1 est `sync_enabled=true`.
Chaque `sync_all_accounts` de la file ne ciblerait donc **que le compte professionnel réel**. À la
seconde où `mailia-worker` démarrera, il videra cette file et synchronisera le compte OVH — en y
appliquant les règles, donc en y déplaçant des emails. C'est exactement le scénario que toutes les
consignes de la campagne visent à empêcher, et il est armé indépendamment de nos correctifs.

**Je n'ai rien purgé** : c'est une opération destructive sur une infrastructure partagée, elle
demande une décision explicite. Commande, si l'utilisateur la valide :

```bash
docker exec mailia-redis redis-cli -a <REDIS_PASSWORD> -n 1 DEL celery
```

Sans cette purge, **ne pas démarrer le worker**, même une fois tous les correctifs validés.

---

# 1. Sécurité

| Bug | Fichier:ligne | Correctif | Vérifié | Comment |
|---|---|---|---|---|
| **N-04** cloisonnement absent dans `move_local_email` | `mcp/helpers.py:35-51` (`get_local_email`), `mcp/server.py:3086` | Nouveau helper partagé qui résout l'email **et** sa propriété en une requête (`LocalEmail → LocalFolder → MailAccount → user_id`). `move_local_email` et `read_local_email` l'utilisent tous les deux — l'asymétrie que le testeur avait relevée disparaît. | **OUI** | `move_local_email(email_id=7301)` — un email local appartenant à un autre compte — → `Local email 7301 not found for user 5`. Idem `read_local_email`. Avant : le déplacement aurait réussi. |
| **TRIGGER-SYNC** (non signalé) aucune vérification de propriété | `mcp/server.py` (outil `trigger_sync`) | L'outil appelait `sync_account.delay(account_id)` sur un id **jamais validé**, et sans argument `sync_all_accounts.delay()` — qui couvre les comptes de **tous** les utilisateurs. Désormais : `get_account()` sur l'id fourni, et sans argument on ne dispatche que les comptes **du demandeur** dont `sync_enabled` est vrai. `sync_all_accounts` n'est plus jamais appelé depuis un outil utilisateur. | **OUI** | `trigger_sync(account_id=1)` → `Account 1 not found for user 5`. `trigger_sync()` → `{"account_ids": []}` (le compte 3 est `sync_enabled=false`). File Celery inchangée à 26 940 : **rien n'a été mis en file**. Avant, ces deux appels auraient l'un et l'autre déclenché une synchro du compte réel. |

### Audit des 67 outils MCP — méthode et résultat

Le testeur demandait de ne pas m'arrêter à `move_local_email`. Trois passes :

1. **Paramètres d'identifiant d'objet** — extraction automatique des signatures des 67 outils :
   seuls **deux** prennent un id autre que `account_id` (`read_local_email`, `move_local_email`).
   La surface est donc étroite ; les deux sont corrigés.
2. **Outils ne passant ni par `get_account` ni par `resolve_account`** — 14 identifiés, examinés un
   par un :
   - `search_emails`, `semantic_search`, `count_emails`, `get_folders_stats`, `get_senders_stats`,
     `summarize_thread`, `ask_about_emails` → tous requêtent `_index_name(user_id)`, un index
     Elasticsearch **par utilisateur** ; `account_id` n'y est qu'un filtre, pas un chemin d'accès. Sains.
   - `get_processing_logs` → filtré par `ProcessingLog.user_id`. Sain.
   - `list_rules`, `create_rule`, `preview_rule` → portée `AIRule.user_id`. Sains.
   - `validate_email_address` → fonction pure. Sain.
   - `read_local_email` → résolution complète (corrigée pour passer par le helper). Sain.
   - **`trigger_sync` → vulnérable.** Voir ci-dessus.
3. **Résolutions par chemin ou par nom** (dossiers locaux) — les 12 requêtes `LocalFolder` du fichier
   filtrent toutes sur `LocalFolder.account_id == account_id`, l'`account_id` étant lui-même validé
   en amont. Aucune faille.

---

# 2. Famille `smtp_ssl` — close définitivement

Le testeur comptait 4 points d'appel SMTP (2 gardés, 2 non gardés). Le `grep` était limité à
`accounts.py` et `mcp/server.py`. Il y en a **six** :

| # | Emplacement | État avant | Après |
|---|---|---|---|
| 1 | `accounts.py` — `send_email` | gardé | helper |
| 2 | `accounts.py` — `test-smtp` | gardé | helper |
| 3 | `accounts.py` — `test-credentials` (**N-02**) | **non gardé**, et `smtp_ssl` absent du schéma | helper + champ ajouté |
| 4 | `mcp/server.py` — `_smtp_connect` (**N-03**) | **non gardé** | helper |
| 5 | `api/routes/auth.py:164` — envoi du mail de réinitialisation | **non gardé** (`server.starttls()` nu, sans même de contexte SSL) | helper |
| 6 | `api/routes/rules.py` — `_forward_email` (mon lot 1) | gardé | helper |

**Nouveau fichier `src/smtp_client.py`** : `smtp_connect(host, port, use_ssl, timeout)`, seul endroit
du projet qui construit une connexion SMTP. Contrôle : `grep -rn "starttls\|SMTP_SSL\|smtplib.SMTP("
src/` ne renvoie plus que les 4 lignes du helper. La famille ne peut plus réapparaître par copie.

Le helper ajoute deux garanties absentes des six copies :
- il teste `has_extn("starttls")` et **échoue explicitement** si le serveur ne l'annonce pas, au lieu
  de laisser remonter un message obscur ;
- il ne **retombe jamais silencieusement en clair** quand l'utilisateur a demandé du TLS.

| Vérification | Résultat |
|---|---|
| MCP `test_smtp(account_id=3)` | `{"status":"ok","host":"greenmail","port":3025}` — avant : `STARTTLS extension not supported` |
| `POST /accounts/test-credentials` avec `smtp_ssl:false` | `{"status":"ok","message":"Connexion SMTP reussie (greenmail:3025)"}` — avant : erreur STARTTLS |
| Idem avec `smtp_ssl:true` | `{"status":"error","message":"[SSL: WRONG_VERSION_NUMBER]"}` — refus propre, pas de plantage |
| `POST /accounts/3/test-smtp` (non-régression) | `ok` |
| MCP `reply_to_email` (bloqué par N-03 à l'itération 2) | envoi réel réussi |

### Défaut connexe corrigé : `smtp_ssl` inatteignable depuis l'interface

Le formulaire de compte n'avait **aucune case SSL pour le SMTP** — seulement pour l'IMAP. Même après
F-03, un utilisateur ne pouvait pas créer de compte en SMTP clair sans passer par l'API. C'est
d'ailleurs ce que `docs/FONCTIONNALITES.md` §2 documente aujourd'hui comme une limite.
Ajouté : case `acc-smtp-ssl`, câblée à la création, au chargement d'un compte existant, à la
réinitialisation du formulaire et au test d'identifiants. **La note ⚠️ de la doc §2 est à retirer.**

---

# 3. Régression N-01 — les trois sources de la date alignées

| Bug | Fichier:ligne | Correctif | Vérifié | Comment |
|---|---|---|---|---|
| **N-01** affichage / tri / filtre sur trois sources | `accounts.py:1246-1305` et `:1368` | Le `FETCH 1:*` récupère désormais `(UID INTERNALDATE BODY.PEEK[HEADER.FIELDS (DATE)])` et construit **une seule** date effective par UID — en-tête `Date:` s'il est exploitable, INTERNALDATE en repli. Tri, filtre et affichage consomment cette même valeur. `_parse_idate` renvoie toujours un datetime *aware* (le repli était naïf, ce qui pouvait faire échouer une comparaison de tri). | **OUI** | Tri : la liste complète est vérifiée décroissante (`dates == sorted(dates, reverse=True)` → `True`) ; l'uid 42, contre-exemple du testeur, est à sa place. Filtre : `filter_date=2026-08-15` renvoie enfin **uid 42** (absent avant) ; `filter_date=2026-08-22` ne le renvoie plus. Liste = détail sur 3 messages. Non-régression : tri date ascendant et tri par expéditeur inchangés. |

Le mécanisme réutilisé est celui que le testeur avait identifié (le `FETCH` par tranches du tri
`from`/`subject`), au coût d'un en-tête supplémentaire par message dans un `FETCH` déjà effectué.

### Régression que j'ai introduite en corrigeant N-01, et corrigée avant livraison

Premier déploiement du lot 3 : `GET /accounts/3/messages` renvoyait
`502 IMAP error: object of type 'datetime.datetime' has no len()` — **la liste des emails était
entièrement cassée**.

Cause : dans `_fetch_messages`, la réponse du FETCH est stockée dans une variable locale `dt`
(`st, dt = conn.uid(...)`). J'avais nommé `dt` la datetime que je venais d'introduire dans la boucle,
masquant la première ; le `while j < len(dt)` suivant recevait une datetime. Renommée `msg_dt`.

Trouvée en exécutant la route dans le conteneur pour obtenir la trace complète — la route
convertissant l'exception en `HTTPException`, aucun traceback n'apparaissait dans les logs. Corrigé,
reconstruit et revérifié avant la fin du lot. Je la signale parce qu'elle serait passée inaperçue si
je m'étais contenté de vérifier les bugs corrigés sans rejouer les cas nominaux.

---

# 4. `semantic_search` (N-05) — échec explicite, pas d'implémentation

Conformément à ta consigne, je n'ai **pas** implémenté la génération d'embeddings.

| Correctif | Détail |
|---|---|
| Échec explicite | Avant la moindre requête kNN, l'outil compte les documents portant un champ `embedding`. À zéro, il lève un `ToolError` en clair au lieu de l'erreur Elasticsearch sur les dimensions. |
| Dimensions nommées | `EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"` et `EMBEDDING_DIMS = 384` dans `indexer.py`, le mapping utilisant désormais la constante (il codait `768` en dur, d'où le rejet 400). Un garde compare la taille produite à `EMBEDDING_DIMS` et le dit clairement en cas d'écart. |
| Modèle mis en cache | `_encode_query()` conserve le `SentenceTransformer` en mémoire ; il était rechargé à chaque appel. Le contrôle d'embeddings passe **avant** le chargement, donc le chemin d'échec ne charge plus rien du tout. |

**Vérifié** : `semantic_search` → `Semantic search is unavailable: no email in the index carries an
embedding vector. Embeddings are never generated at indexing time, so this feature is inert until
that is implemented. Use search_emails (full-text) instead.`

### Ce qu'il faudrait pour le rendre réellement fonctionnel (décision produit)

1. **Générer les vecteurs à l'indexation.** `bulk_index_emails()` — le seul chemin réellement
   utilisé — ne construit pas le champ ; `index_email()` accepte un paramètre `embedding` qu'aucun
   appelant ne renseigne. Il faut encoder le corps de chaque email au moment de l'indexation.
2. **Budget CPU.** `all-MiniLM-L6-v2` sur CPU traite de l'ordre de quelques dizaines d'emails par
   seconde. Pour 45 000 emails, la réindexation initiale se compte en dizaines de minutes de CPU
   soutenu, et chaque synchronisation incrémentale s'alourdit. Le worker a déjà un budget de temps
   contraint (`task_time_limit=1800`) : ce coût doit y entrer.
3. **Réindexation complète obligatoire.** Les dimensions d'un `dense_vector` ne se modifient pas sur
   un index existant. `mailia-5` est en 768, le modèle produit 384 : il faut créer un nouvel index
   et tout réindexer. Ma correction de la constante ne s'applique qu'aux index **créés après**.
4. **Choix du modèle.** 384 dimensions est un compromis ; un modèle multilingue serait plus
   pertinent sur du courrier francophone, au prix de la taille et de la vitesse.
5. **Dépendance runtime.** `sentence_transformers` (et PyTorch) doivent être présents dans l'image
   du worker, pas seulement dans celle du MCP.

Tant que ces cinq points ne sont pas tranchés, l'outil reste exposé mais dit honnêtement pourquoi il
ne peut pas répondre. La documentation §4 le présente encore comme disponible — à corriger.

---

# 5. Autres correctifs MCP

| Bug | Fichier | Correctif | Vérifié | Comment |
|---|---|---|---|---|
| **N-06** `In-Reply-To` = UID IMAP | `rules/engine.py:31-32`, `imap/manager.py:176-177`, `mcp/server.py` | `EmailContext` porte désormais `message_id` et `references` (champs avec valeur par défaut, les trois sites de construction utilisent des mots-clés). `reply_to_email` pose `In-Reply-To` **et** `References` à partir du `Message-ID` réel. | **OUI** | Réponse envoyée sur l'uid 45 (`Message-ID <it211@qa.local>`) → message dans `Sent` portant `In-Reply-To: <it211@qa.local>` et `References: <it211@qa.local>`. Avant : `In-Reply-To: 45`. |
| **N-07** exceptions SMTP brutes | `mcp/server.py` | `reply_to_email` et `forward_email` enveloppent l'envoi comme `send_email` le faisait déjà : `except Exception as e: raise ToolError(f"SMTP error: {e}")`. | **OUI** | Les trois outils du groupe se comportent identiquement ; plus aucune exception brute. |
| **N-08** dates prises pour des téléphones | `mcp/server.py` (`_looks_like_phone`) | Regex à frontières de mot, ≥ 9 chiffres, rejet des dates ISO et des références numériques (un groupe de ≥ 5 chiffres autour d'un tiret). | **OUI** | Banc de 8 cas : `2026-09-15` et `2024-000123` rejetés ; `+33 6 12 34 56 78`, `06.12.34.56.78`, `(01) 42 68 53 00`, `0612345678`, `555-123-4567`, `+1-800-555-0199` tous reconnus. |
| **N-09** `delete_folder` ne purgeait pas ES | `mcp/server.py` (`_es_delete_folder_docs`) | `delete_by_query` sur `(account_id, folder)` — en UTF-8 **et** en UTF-7 — après une suppression réussie. Un `delete_by_query`, et non une suppression par UID : le dossier disparaît entièrement. | **PARTIEL** | Le helper est vérifié : appliqué aux fantômes laissés par le testeur, `MCPDst 6→0`, `MCPArch 2→0`, `MCPSrc 1→0`. Le trajet complet **n'est pas testable sur GreenMail** : `DELETE` y échoue par `socket error: EOF` (défaut serveur documenté section D du rapport iter. 2), donc `ok` reste faux et la purge n'est jamais atteinte. |
| **N-11** scores anti-spam contradictoires | `mcp/server.py` | « No DKIM signature » est une absence de preuve, déjà payée par le point DKIM non accordé. La compter en plus comme drapeau rouge faisait diverger `scan_for_spam` de `spam_analysis` sur le même message. Constante `_NO_DKIM`, exclue du calcul du 4ᵉ point. | Statique | Les deux outils appliquent maintenant la même règle pour le 4ᵉ point. |
| **N-12** tri lexicographique du `trust_score` | `mcp/server.py` | `key=lambda x: int(str(x["trust_score"]).split("/")[0])`. | Statique | Fonctionnait par accident sur un barème à un chiffre. |
| **N-13** brouillons sans en-tête `Date` | `mcp/server.py` | `msg["Date"] = formatdate(localtime=True)` dans `save_draft` **et** `update_draft`. | **OUI** | `list_drafts` : le nouveau brouillon affiche `2026-08-22 01:39` au lieu de `""`. |
| **N-14** outils IA remontant `RuntimeError` | `mcp/server.py` (`_get_llm`) | Les 5 appels passent par un helper qui convertit toute défaillance en `ToolError`. | **OUI** | `ask_about_emails` → `ToolError: AI provider unavailable: No AI provider configured for user 5 and no system fallback`. |
| **N-15** `FETCH illegal in state AUTH` | `imap/manager.py` (`fetch_email`) | Le statut du `SELECT` est vérifié ; un dossier non sélectionnable renvoie `None` au lieu de laisser partir un `FETCH` dans l'état AUTH. Message de `read_email` précisé. | **OUI** | `read_email(folder="NoSuchFolder")` → `Email UID 1 not found in 'NoSuchFolder' (check the folder exists)`. Bénéficie aux 14 appelants de `fetch_email`. |
| **N-16** docstring trompeur | `mcp/server.py` | « from an email's iCalendar parts (text/calendar, .ics attachments) » — la description colle à l'implémentation. Je n'ai pas ajouté l'analyse du corps : ce serait une fonctionnalité, pas une correction. | Statique | |

---

# 6. Constats que je requalifie

### N-17 — « `list_folders` renvoie un séparateur incohérent » : pas un bug de cet outil

`list_folders` renvoie `folders[0]["separator"]`, c'est-à-dire **le séparateur que le serveur déclare
dans sa réponse LIST**. GreenMail déclare `.`, et l'outil le rapporte fidèlement.

Ce qui est incohérent, c'est que certains dossiers portent un `/` **littéralement dans leur nom**
(`Archives/Clients`), parce qu'ils ont été créés ainsi lors d'un import ZIP — c'est **R-01** de
l'itération 1, un défaut de `create_folder`, pas de `list_folders`. Faire mentir `list_folders` sur
le séparateur déclaré casserait tout client qui s'y fie.

À noter : `create_folder` (`server.py:1135`) se défie déjà du séparateur déclaré et détecte le
séparateur réel en inspectant les enfants existants. La vraie correction, si l'utilisateur la
souhaite, est de faire de même à l'import ZIP pour ne plus créer de dossiers dont le nom contient un
séparateur. C'est R-01 et cela n'était pas dans mon lot.

### N-10 — `copy_local_to_imap` n'indexe pas dans ES : incohérence transitoire, pas permanente

Le constat est exact, mais l'écart se résorbe seul : les messages ajoutés par `APPEND` reçoivent des
UID supérieurs au curseur `sync_state` du dossier, donc la synchronisation suivante les récupère et
les indexe normalement. C'est structurellement différent de N-09, où les documents devenaient
**définitivement** orphelins (le dossier n'existant plus, aucune synchro ne repasse dessus).

Indexer immédiatement supposerait de relire les messages après `APPEND` pour connaître leurs UID —
un aller-retour IMAP supplémentaire pour gagner quelques minutes sur une incohérence qui se corrige
d'elle-même. Je ne l'ai pas fait ; dis-moi si tu préfères le contraire.

---

# 7. Vérification post-déploiement du lot 2

| Bug | Vérifié | Comment |
|---|---|---|
| **FE-02** `target_storage` | **OUI** | `move` IMAP→local : l'email arrive en stockage local (`L7477`, date d'origine conservée) et **aucun dossier IMAP fantôme `L3-Local` n'est créé** — la liste des 15 dossiers IMAP est identique avant/après. C'était le dommage principal du bug. |
| **FE-03** purge du dossier spam | **OUI** | Le chemin `empty-folder` que le frontend emprunte désormais : `QATestRenamed` 6 → `{"deleted":6}` → 0. |
| **FE-05** assainissement `marked` | **OUI (livré)** | `function mdSafe(`, `MD_DROPPED_TAGS` présents dans le HTML servi ; il ne reste **qu'un seul** `marked.parse(` dans tout le fichier, celui situé à l'intérieur de `mdSafe()`. Comportement déjà prouvé hors ligne sur 15 charges utiles. |
| **FE-04** `escJs` | **OUI (livré)** | `function escJs(` présent dans le HTML servi ; 19 interpolations converties. Comportement prouvé hors ligne sur 9 valeurs. |
| **FE-06** échecs en masse | **OUI (livré)** | `_reportBulkFailures` présent (3 occurrences) ; plus aucun `catch {}` sur ces chemins. |
| **FE-07** barre oblique finale | **OUI** | Les 2 occurrences du fichier servi utilisent `api('/accounts/')`. |
| **FE-08** `_ctxExportFolder` | **OUI (livré)** | Indicateur mort retiré, succès signalé par `alert`. |
| **FE-09** « répondu » en local | **OUI** | `L7477` : `answered` **False → True** après une réponse portant `reply_uid: "L7477"`. C'est le chemin `LocalEmail` ; auparavant `imap.flag_email("L7477", …)` échouait en silence. |
| **FE-10** sandbox | **OUI** | `sandbox="allow-popups allow-popups-to-escape-sandbox"` × 2 ; `allow-same-origin` : **0 occurrence**. |
| **WK-06 / WK-08 / WK-09** | **NON — worker arrêté** | Vérifiés statiquement (relecture, `py_compile`, structure de `_sync_account` par AST). Non exécutables sans démarrer `mailia-worker`, ce qui reste interdit — et le serait d'autant plus avec 26 940 tâches en file. |
| **FE-11** | doc | Liste transmise au lot 2 ; s'y ajoute la note §2 sur le SSL SMTP, devenue fausse. |

---

# 8. Non-régression

Rejouée après le déploiement final, tout conforme :

- **API** : liste des comptes, dossiers, messages avec tris et filtres, `filter_from` par adresse
  complète (2), recherche ES (`q=devis` → 6), détail d'email + pièce jointe, `spam-scan`,
  `search-multi` (9 résultats, 0 erreur), export ZIP, signatures, contacts, règles, admin,
  digest (502 propre sans provider), 403 sans token.
- **Cloisonnement** : `GET /accounts/1/messages?storage=local` → `404 Account not found` ;
  IDOR API sur `L7301` → `404 Email not found` ; garde-fou MCP compte 1 → refusé.
- **MCP, opérations destructives** : `move_email` puis `delete_email` → INBOX 14→12, Trash 14→15,
  QAIter2Dst 0→1, **delta total = 0**. Aucune duplication : le correctif F-02 tient.
- **67 outils** toujours exposés.

---

# 9. Fichiers modifiés

| Fichier | Objet |
|---|---|
| `src/smtp_client.py` | **nouveau** — point d'entrée SMTP unique |
| `src/mcp/server.py` | N-03, N-04, `trigger_sync`, N-05, N-06, N-07, N-08, N-09, N-11, N-12, N-13, N-14, N-15, N-16 |
| `src/mcp/helpers.py` | `get_local_email()` — résolution + propriété |
| `src/api/routes/accounts.py` | N-01, N-02, migration SMTP, FE-09 |
| `src/api/routes/auth.py` | 5ᵉ point SMTP non gardé |
| `src/api/routes/rules.py` | migration SMTP |
| `src/search/indexer.py` | `EMBEDDING_DIMS` / `EMBEDDING_MODEL_NAME` |
| `src/rules/engine.py` | `EmailContext.message_id` / `.references` |
| `src/imap/manager.py` | N-15, N-06 (peuplement), WK-09 (lot 2) |
| `src/worker/tasks.py` | WK-06, WK-08 (lot 2) |
| `src/web/static/index.html` | lot 2 complet + case SSL SMTP |

Non touchés : `src/worker/app.py`, `docker-compose.yml`, `docs/FONCTIONNALITES.md`.

# 10. Sécurité du périmètre

- **`mailia-worker` : `Exited (137)` — `mailia-beat` : `Exited (0)`**, vérifié après chacun des
  quatre cycles de build. Chaque commande a nommé explicitement `api` et `mcp` ; `worker` a été
  **construit** (image à jour) mais **jamais démarré**.
- **Compte 1 intact** : `sync_enabled=true`, `last_sync_at=2026-05-20 10:05:51.760324`,
  **1227 emails locaux** — identique aux trois relevés précédents.
- **File Celery inchangée à 26 940** : mes tests de `trigger_sync` n'ont rien mis en file, et je
  n'ai rien purgé.
- Tous les tests menés avec le token QA (utilisateur 5) sur le compte 3.
- Nettoyé : dossier local `L3-Local`, brouillon de test, scripts de débogage retirés du conteneur.
  Le compte 3 n'a plus aucun dossier local. `/app/mcp_runner.py` re-copié après chaque build.

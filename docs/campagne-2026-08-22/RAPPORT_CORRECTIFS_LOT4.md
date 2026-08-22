# Rapport de correction — Lot 4 (frontend, suite) + récapitulatif de remise

**Date** : 2026-08-22
**Source** : `RAPPORT_FRONTEND_SUITE.md`
**Déploiement** : **aucun** — rien reconstruit, conformément à ta consigne. Le testeur travaille sur
l'itération 3. Les éditions sont sur disque, compilées et testées hors ligne.

## Résultat

| | |
|---|---|
| Constats traités | **4** (FS-01 déjà clos, FS-02 corrigé, FS-03 requalifié, FS-04 corrigé) |
| Constats du §4 rouverts à tort | **2** (FE-08, FE-10 — déjà corrigés au lot 2) |
| Durcissement du §1 appliqué | **1** (R-B) |
| Bug trouvé dans mon propre correctif par le banc de test | **1** |

---

## Tableau

| Bug | Fichier:ligne | Correctif | Vérifié | Comment |
|---|---|---|---|---|
| **FS-01** `smtp_ssl` inatteignable depuis l'interface | `index.html:1240, 4589, 4624, 4671, 4697` | Corrigé au lot 3, **couverture confirmée sur les 5 points** que le constat décrit. | **OUI** (déployé au lot 3) | Case `acc-smtp-ssl` ligne 1240 ; le même objet `body` alimente **création (POST, 4605) et modification (PUT, 4599)** ; chargement d'un compte existant `a.smtp_ssl !== false` (4624) ; réinitialisation du formulaire (4671) ; `test-credentials` (4697). Côté API, le champ `smtp_ssl` a été ajouté au schéma `TestCredentials` (N-02). **Clos.** |
| **FS-02** worker absent jamais signalé | `worker/app.py:32` (`worker_online`), `accounts.py:302`, `mcp/server.py` (`trigger_sync`), `index.html:4650` | Nouveau `worker_online()` à côté de l'application Celery. `POST /accounts/{id}/sync` **ne met plus rien en file** si aucun worker ne répond : `503` avec un message explicite. Même garde sur l'outil MCP `trigger_sync`. Côté interface, l'échec affiche enfin la raison. | **Partiel** — voir ci-dessous | `py_compile` OK, `node --check` OK. Le comportement « worker absent » sera vérifiable dès le déploiement, puisque c'est l'état courant. |
| **FS-03** export sur dossier local | — | **Non corrigé : faux positif** (2ᵉ fois). Démonstration ci-dessous. Les deux défauts cosmétiques cités comme subsistants ont été corrigés au lot 2. | — | |
| **FS-04** `onclick="${escAttr(onSelect)}"` | `index.html:4192-4204` (registre + dispatcheur), ligne de rendu, 5 appelants | Le motif change, comme tu le pressentais. Plus aucune expression JS n'est montée dans un attribut : les appelants nomment une **action** d'un registre `FP_ACTIONS`, la valeur ne circule que par des `data-*`, et l'attribut vaut invariablement `onclick="_fpDispatch(this)"`. | **OUI** (hors ligne) | Banc jsdom `test_fp.js` : 7 cas, dont un nom de dossier hostile qui aurait été du code dans l'ancien motif. |
| **R-B** liens IA sans `rel` | `index.html` (`mdSafe`) | Tout `A` conservé reçoit `target="_blank" rel="noopener noreferrer"`, **après** le filtrage d'attributs pour que l'entrée ne puisse pas les écraser. | **OUI** (hors ligne) | `[ok](https://exemple.fr/a)` → `<a href="…" target="_blank" rel="noopener noreferrer">` ; `[js](javascript:…)` reste sans `href` (donc sans `target`). Les 15 charges mXSS repassent : **DOM final propre**. |

---

## FS-04 — pourquoi `escJs()` ne s'appliquait pas, et ce que j'ai fait

`escJs()` encode une **valeur**. `onSelect` était une **expression** : l'y passer aurait produit
`onclick="&quot;fn(this)&quot;"`, c'est-à-dire une chaîne évaluée puis jetée. Le gestionnaire
n'aurait plus rien fait. Ta lecture était la bonne : c'est le motif qu'il fallait changer.

```javascript
const FP_ACTIONS = {
    importDest: (el) => selectImportDest(el),
    applyRule:  (el) => _doApplyRule(Number(el.dataset.fpArg),
                                     (el.dataset.fpStorage === 'local' ? 'local:' : '') + el.dataset.fpPath, el),
    pick:       (el) => _pickFolder(el, el.dataset.fpPath, el.dataset.fpName, el.dataset.fpStorage),
    move:       (el) => moveToFolder(el.dataset.fpPath, el.dataset.fpStorage),
};
```

Les 5 appelants passent désormais `action: 'pick'` au lieu d'une chaîne de code ; celui des règles
passe `actionArg: ruleId`. Une action inconnue est journalisée, pas exécutée. C'est le motif que le
fichier applique déjà aux liens du chat IA, comme le testeur le suggérait.

### Le banc de test a attrapé un bug dans mon propre correctif

Premier jet : `data-fp-arg="${escAttr(actionArg)}"`. Or `actionArg` est `ruleId`, un **nombre**, et
`escAttr` fait `(s || '').replace(...)` → `TypeError: (s || "").replace is not a function` **au
rendu**. Le dialogue « appliquer une règle » aurait été entièrement cassé — un cas que la relecture
ne m'avait pas signalé et qu'aucun de mes autres tests n'aurait touché. Corrigé en
`escAttr(String(actionArg))`.

C'est la deuxième fois de la campagne qu'un correctif de sécurité introduit une régression
fonctionnelle attrapée par le banc plutôt que par la relecture (la première étant la collision de
nom `dt` du lot 3). J'en tire que pour ce fichier, écrire le test **avant** de déployer n'est pas un
luxe.

---

## FS-03 — faux positif, avec la preuve

Le constat affirme que « l'entrée *Exporter en ZIP* figure au menu contextuel de **tous** les
dossiers, locaux compris (`:5013`, sans distinction) ». C'est inexact sur **deux** niveaux :

1. **Le gestionnaire n'est pas posé sur les dossiers locaux.** Les éléments locaux sont rendus
   ligne 4915 :
   ```html
   <div class="folder-item" data-storage="local" data-path="…" onclick="selectFolder(…)" …>
   ```
   Aucun `oncontextmenu`. Seuls les éléments IMAP (lignes 4878 et 4880) en portent un.
2. **Et le gestionnaire refuse de toute façon.** `_onFolderCtx` commence par :
   ```javascript
   const storage = el.dataset.storage || 'imap';
   if (storage !== 'imap') return;        // index.html:5010
   ```

Le menu contextuel ne peut donc pas s'ouvrir sur un dossier local, et `storage: 'imap'` codé en dur
dans `_ctxExportFolder` est **correct**, pas un oubli. J'avais déjà écarté ce point au lot 2 ; je le
re-documente ici avec le rendu des dossiers locaux, qui manquait à ma première démonstration.

**Les deux défauts « qui subsistent » ne subsistent pas** : `btn` / `originalText` et le
`console.log` de succès ont été supprimés au lot 2. Le code actuel (`index.html:5038-5060`) n'en
contient plus trace ; le succès déclenche une `alert`. Le rapport a été écrit sur une lecture
antérieure de cette fonction.

**FE-10 est également rouvert à tort** : `grep -c "allow-same-origin" index.html` → **0**. Corrigé
au lot 2, vérifié dans le HTML servi.

Reste de FE-08/FS-03 réellement ouvert : **rien**.

---

## R-A — je ne l'applique pas, et la prémisse mérite une correction

Le constat propose de bloquer `IMG src` distant dans les réponses de l'IA, au motif que « le corps
des emails est justement isolé dans une iframe pour éviter ce genre d'effet ».

La prémisse est inexacte : l'attribut `sandbox` empêche l'**exécution de scripts**, il n'empêche pas
le chargement des sous-ressources. Les images distantes d'un corps d'email se chargent déjà
aujourd'hui dans l'iframe. Bloquer les images des seules réponses IA n'apporterait donc aucune
cohérence — l'écart de confidentialité, réel, se situe sur le rendu des emails eux-mêmes, pas sur le
chat.

Traiter le vrai sujet, c'est bloquer les images distantes **à l'affichage des emails**, avec un
bouton « afficher les images » — c'est une fonctionnalité produit classique, mais une fonctionnalité,
et elle n'a été demandée par personne. Je la remonte plutôt que de la décider.

---

## FS-02 — ce que fait exactement le correctif

Le mécanisme qui a rendu invisible le blocage de trois mois est précisément celui-ci : une tâche
déposée dans Redis renvoie `200 sync_started` que quelqu'un la consomme ou non.

```python
if not worker_online():
    raise HTTPException(status_code=503, detail="Aucun worker de synchronisation n'est actif : "
        "la demande n'a pas ete mise en file (elle serait restee sans effet)…")
```

Trois choix à signaler :

- **On refuse au lieu de mettre en file.** Empiler des tâches que personne ne consomme est
  exactement ce qui a produit les 26 940 messages. Une demande qui ne peut pas être exécutée ne doit
  pas grossir un arriéré ; l'utilisateur la relancera quand le worker sera là.
- **Le contrôle est côté serveur, pas côté interface.** Le rapport suggérait de lire `worker.status`
  depuis `/admin/status` : cet endpoint est **réservé aux administrateurs**, un utilisateur ordinaire
  ne peut pas s'en servir. Le contrôle appartient donc à l'endpoint de synchronisation.
- **`trigger_sync` (MCP) reçoit la même garde.** L'assistant IA l'appelle ; sans garde, il aurait
  continué à empiler.

Côté interface, `syncAccount()` ne faisait que remplacer le libellé du bouton par « Erreur » et
avalait le message. Il affiche désormais la raison.

---

# Récapitulatif de remise

## 1. Ce qui existait avant la campagne, et qu'il ne faut pas nous attribuer

Quatre fichiers portaient déjà des modifications **non commitées de l'utilisateur** au démarrage de
la campagne. Base de comparaison : la copie déployée sur le serveur relevée avant notre premier
déploiement.

| Fichier | HEAD → pré-campagne (utilisateur) | pré-campagne → aujourd'hui (campagne) |
|---|---:|---:|
| `src/api/routes/accounts.py` | 101 lignes | 262 lignes |
| `src/api/routes/ai.py` | 17 lignes | 22 lignes |
| `src/mcp/server.py` | 96 lignes | 280 lignes |
| `src/web/static/index.html` | 31 lignes | 237 lignes |

(Lignes de diff, les deux côtés confondus.) Un `git diff` brut mélangerait les deux colonnes.

Fichiers **non suivis** préexistants, également étrangers à la campagne :
`docs/FONCTIONNALITES.md` (que tu as depuis enrichi), `src/web/static/favicon.ico`,
`src/web/static/favicon.png`, `src/web/static/index.html.bak`.

## 2. Fichiers modifiés par la campagne

| Fichier | Rôle des modifications |
|---|---|
| `src/api/routes/accounts.py` | IDOR stockage local (7 endpoints), `smtp_ssl`/`sync_enabled` à la création, filtre expéditeur, date unique pour l'affichage/tri/filtre, dates naïves UTC, INTERNALDATE préservé, flag « répondu » (IMAP et local), migration SMTP, refus de synchro sans worker |
| `src/mcp/server.py` | Cloisonnement `move_local_email`, `trigger_sync` (propriété + plus de `sync_all_accounts`), STARTTLS, `semantic_search` en échec explicite, threading des réponses, erreurs enveloppées, regex téléphone, purge ES à la suppression de dossier, scores anti-spam, tri, en-tête `Date` des brouillons, brouillon dupliqué, `STORE` parenthésés |
| `src/web/static/index.html` | XSS surlignage (toi), `mdSafe`, `escJs`, `target_storage`, purge spam, échecs en masse, `/accounts/`, export ZIP, sandbox, case SSL SMTP, registre du sélecteur de dossiers, message de synchro |
| `src/imap/manager.py` | Flags IMAP parenthésés + rollback du COPY orphelin, tri des UID, `SELECT` vérifié, `Message-ID`/`References` peuplés |
| `src/worker/tasks.py` | Verrou par compte, dispatcheur, `SoftTimeLimitExceeded`, disjoncteur IA (toi) ; curseur borné aux UID traités, fermeture d'Elasticsearch, distinction message disparu / fetch en échec (moi) |
| `src/worker/app.py` | Limites de temps et `expires` (toi) ; `worker_online()` (moi) |
| `src/rules/engine.py` | `AIProviderTimeout` (toi) ; `EmailContext.message_id` / `.references` (moi) |
| `src/api/routes/rules.py` | Flags parenthésés, `to` multi-destinataires, action `forward` implémentée, actions inconnues remontées au preview, migration SMTP |
| `src/api/routes/search.py` | Score `null`, index absent |
| `src/api/routes/contacts.py` | Détachement de signature par `null` |
| `src/api/routes/ai.py` | Provider inconnu → 404, erreur provider → 502 |
| `src/api/routes/digest.py` | 500 brut → 502/404 |
| `src/api/routes/auth.py` | STARTTLS forcé sur le mail de réinitialisation |
| `src/ai/router.py` | `LookupError` sur `provider_id` inconnu |
| `src/ai/providers/claude_native_provider.py` | En-tête `Bearer` vide |
| `src/rules/parser.py` | Casse des dossiers, `transferer`, actions non reconnues exposées |
| `src/search/indexer.py` | `EMBEDDING_DIMS` / `EMBEDDING_MODEL_NAME` |
| `src/mcp/helpers.py` | `get_local_email()` — résolution + propriété |
| `docker-compose.yml` | Toi (worker) |

## 3. Fichiers nouveaux

**Un seul** : `src/smtp_client.py` — point d'entrée unique pour toute connexion SMTP
(`smtp_connect`). Il remplace six copies de la même logique, dont trois n'étaient pas gardées.

⚠️ Correction à ta liste : **`src/mcp/helpers.py` n'est pas un fichier nouveau**. Il est suivi par
git et préexistait ; j'y ai seulement ajouté `get_local_email()`.

`mcp_runner.py` n'est **pas** un fichier du projet : il vit dans le scratchpad de la campagne et
dans le conteneur via `docker cp`. Absent du dépôt, des Dockerfiles et de l'image.

## 4. Migrations et configuration

**Aucune migration de base n'est nécessaire.** Vérifié : `src/db/models.py` est inchangé
(`git status` le confirme), aucune révision Alembic ajoutée. Les colonnes utilisées (`smtp_ssl`,
`sync_enabled`, `sync_state`) existaient déjà.

**Aucun changement de configuration requis.** Pas de variable d'environnement nouvelle, pas de
dépendance ajoutée à `requirements.txt`. `src/smtp_client.py` n'utilise que la bibliothèque standard.

Deux points d'exploitation, en revanche :

1. **La file Celery** que tu as renommée en `celery_backup_20260822` — décision de purge définitive à
   prendre par l'utilisateur.
2. **`semantic_search`** reste inerte par conception. Les cinq prérequis pour l'activer (génération
   des vecteurs à l'indexation, budget CPU, réindexation complète obligatoire, choix du modèle,
   `sentence_transformers` dans l'image du worker) sont détaillés au §4 du rapport du lot 3.

## 5. Ce qui n'a pas pu être vérifié à l'exécution

| Élément | Pourquoi | Ce qui a été fait à la place |
|---|---|---|
| **WK-06** curseur borné aux UID traités | Nécessite `mailia-worker` | Relecture, `py_compile`, revue croisée avec tes correctifs |
| **WK-08** fermeture d'Elasticsearch | idem | Structure de `_sync_account` validée par **AST** (`finalbody` = `await es.close()`) |
| **WK-09** tri des UID | idem | Statique — aucun appelant ne dépend de l'ordre du serveur |
| **`_uid_still_present`** (message disparu / fetch en échec) | idem | Relecture ; repli `return True` = on rejoue, le côté sûr |
| **Tes WK-01/02/03/05** (persistance par lot, limite douce, disjoncteur, verrou) | idem | Non validés à l'exécution — déjà signalé par le testeur (section E de l'itération 2) |
| **FS-02** refus quand le worker est absent | Pas encore déployé | Vérifiable **immédiatement** après déploiement : c'est l'état courant du système |
| **N-09** purge ES à la suppression d'un dossier | `DELETE` défaillant côté GreenMail | Helper vérifié isolément : fantômes 6/2/1 → 0 |
| **FE-04 / FE-05 / FS-04 / R-B** (frontend) | Pas de pilotage navigateur | Bancs jsdom exécutant le code **extrait de `index.html`** : 15 charges mXSS, 9 valeurs `escJs`, 7 cas de délégation |
| **Parcours visuels** (thèmes, TinyMCE, responsive) | Hors périmètre API | Non couverts |

**En résumé** : tout ce qui touche au worker attend son redémarrage — lequel ne devrait pas
intervenir avant que la question de la file soit tranchée.

## 6. À déployer au signal

Quatre fichiers modifiés depuis le dernier build :

| Fichier | Écart |
|---|---|
| `src/worker/app.py` | 13 lignes (`worker_online`) |
| `src/api/routes/accounts.py` | 8 lignes (garde de synchro) |
| `src/mcp/server.py` | 7 lignes (garde `trigger_sync`) |
| `src/web/static/index.html` | 48 lignes (FS-04, R-B, message de synchro) |

```bash
docker compose build api worker mcp && docker compose up -d api mcp
docker cp /tmp/mcp_runner.py mailia-api:/app/mcp_runner.py
```

Vérifications prévues ensuite : le `503` de synchro sans worker (cas courant), le sélecteur de
dossiers sur les 4 actions, et une passe de non-régression.

## 7. Sécurité du périmètre

- **Aucun conteneur reconstruit, démarré, arrêté ou redémarré durant ce lot.**
- `mailia-worker` : `Exited (137)` — `mailia-beat` : `Exited (0)`.
- Aucun appel API, aucune donnée touchée : lot entièrement en édition de source et test hors ligne.
- Rien de commité, conformément à ta consigne.

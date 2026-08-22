# Audit frontend — suite (après FE-11)

**Date** : 2026-08-22
**Fichier** : `/var/www/mailia/src/web/static/index.html`
**Méthode** : lecture de code exclusivement. Aucun appel API, aucun conteneur, aucun fichier modifié.

## Contenu

1. Revue critique de `mdSafe()` et `escJs()` — recherche de contournements
2. Vérification de la couverture : reste-t-il des interpolations non protégées ?
3. Statut des constats FE-02 à FE-11
4. Nouveaux constats

## Synthèse

| | Résultat |
|---|---|
| Contournement trouvé dans `mdSafe()` | **aucun** |
| Contournement trouvé dans `escJs()` | **aucun** |
| Interpolations non protégées restantes | **aucune exploitable** (inventaire exhaustif ci-dessous) |
| FE-02 à FE-11 corrigés | **7 sur 10** |
| Nouveaux constats | **4** (dont 1 majeur) |

**Le point important de ce rapport** : le correctif backend F-03 (`smtp_ssl` honoré à la création
d'un compte) est **inatteignable depuis l'interface** — le formulaire de compte ne comporte aucun
champ SSL SMTP et ne transmet jamais la valeur. Voir FS-01.

---

# 1. Revue de `mdSafe()`

`index.html:7627-7644`, avec `mdSafeUrl()` en `:7620-7625` et les trois listes en `:7614-7618`.

## Architecture — correcte

```javascript
const doc = new DOMParser().parseFromString(marked.parse(markdown || ''), 'text/html');
for (const el of [...doc.body.querySelectorAll('*')]) {
    if (MD_DROPPED_TAGS.has(el.tagName)) { el.remove(); continue; }
    if (!MD_ALLOWED_TAGS.has(el.tagName)) { el.replaceWith(...el.childNodes); continue; }
    ...
}
return doc.body.innerHTML;
```

Trois choix structurants, tous justes :

- **`DOMParser` plutôt qu'un élément détaché** — le document produit est inerte : aucun script ne
  s'exécute, aucune ressource n'est chargée pendant l'analyse. Assigner d'abord à un
  `div.innerHTML` aurait déclenché les `onerror` d'images avant même l'assainissement.
- **Liste blanche de balises** — tout ce qui n'est pas explicitement autorisé est traité, jamais
  ignoré.
- **Distinction dépose / déballage** — `SCRIPT`, `STYLE`, `IFRAME`, `SVG`, `MATH`… sont supprimés
  **avec leur contenu**, le reste est déballé. Le commentaire explique pourquoi
  (« unwrapping these would leak code as text or, for foreign content, re-parse into something
  executable ») : c'est exactement le bon raisonnement.

## Contournements testés — tous bloqués

| Vecteur | Résultat | Pourquoi |
|---|---|---|
| `<script>`, `<iframe>`, `<object>`, `<embed>` | bloqué | dans `MD_DROPPED_TAGS`, supprimés avec leur contenu |
| **mXSS par contenu étranger** : `<svg><style><a title="</style><img src=x onerror=alert(1)>">` | bloqué | `SVG` et `MATH` sont **déposés en entier**. C'est le vecteur classique contre les assainisseurs qui sérialisent puis re-analysent, et il est traité. |
| `<template>` / `<noscript>` (asymétrie sérialisation/ré-analyse) | bloqué | tous deux dans `MD_DROPPED_TAGS` |
| `<xmp>` (élément à texte brut, hors des deux listes) | inoffensif | déballé ; ses enfants sont des nœuds texte, qui se re-sérialisent échappés |
| `javascript:alert(1)` dans un `href` | bloqué | schéma reconnu, hors liste blanche → attribut retiré |
| `java\tscript:` / `java\nscript:` | bloqué | les caractères de contrôle `U+0000–U+001F` et `U+007F` sont retirés **avant** le test — le commentaire le signale explicitement |
| ` javascript:` (espace initial) | bloqué | `.trim()` |
| `JaVaScRiPt:` | bloqué | drapeau `/i` |
| `&#106;avascript:` (entité HTML) | bloqué | `DOMParser` décode les entités avant que la valeur d'attribut ne soit lue |
| `data:text/html,<script>…` | bloqué | schéma reconnu, `data` hors liste blanche |
| `java script:` | inoffensif | l'espace insécable n'appartient pas à la classe `[a-z0-9+.-]`, la chaîne n'est pas reconnue comme un schéma et retombe en URL relative — que le navigateur ne peut pas exécuter |
| `onerror`, `onload`, `style`, `formaction` sur une balise autorisée | bloqué | liste blanche d'attributs **par balise** ; tout attribut hors liste est retiré |

**Ordre de parcours** : `querySelectorAll('*')` renvoie les nœuds en ordre document, donc les
ancêtres avant les descendants. Un conteneur inconnu déballé l'est toujours avant que ses enfants
ne soient examinés, et les descendants d'un nœud supprimé restent dans l'instantané mais sont
traités hors de l'arbre — sans effet. Aucune fenêtre d'échappement.

**Verdict : je n'ai pas trouvé de contournement.** L'implémentation est meilleure que la moyenne
des assainisseurs maison, notamment sur le point le plus souvent raté (le contenu étranger SVG/MathML).

## Deux réserves, sans gravité

**R-A — `IMG` autorise `src` en http/https.** Une réponse de l'IA qui cite un email contenant une
image distante déclenchera son chargement : l'adresse IP et l'agent utilisateur du lecteur partent
vers un serveur tiers. Ce n'est pas une exécution de code, mais c'est une fuite de confidentialité,
et le corps des emails est justement isolé dans une iframe pour éviter ce genre d'effet. Cohérence
à envisager : bloquer `IMG src` distant, ou le passer par un relais.

**R-B — `A` conserve `href` sans `rel`.** Les liens produits par l'IA s'ouvrent dans l'onglet
courant, sans `rel="noopener noreferrer"`. Ajouter `target="_blank" rel="noopener noreferrer"` sur
tout `A` conservé alignerait le comportement sur celui de l'iframe d'email (`<base target="_blank">`).

Aucune des deux ne justifie de bloquer le lot ; ce sont des durcissements.

---

# 2. Revue de `escJs()`

`index.html:7609` :

```javascript
function escJs(v) { return escAttr(JSON.stringify(v === null || v === undefined ? '' : v)); }
```

La composition est correcte et l'ordre l'est aussi : encoder **d'abord** pour l'analyseur
JavaScript (`JSON.stringify`), **ensuite** pour l'analyseur d'attribut HTML (`escAttr`) — c'est
l'inverse de l'ordre de décodage du navigateur, donc les deux couches se dépilent proprement.

Le commentaire précise que la fonction **émet ses propres guillemets** :
`onclick="fn(${escJs(v)})"`, jamais `onclick="fn('${escJs(v)}')"`. C'est le seul piège d'usage, et
il est documenté à l'endroit exact où on le lira.

| Charge d'entrée | Chaîne JS finale après double décodage | Verdict |
|---|---|---|
| `O'Brien` | `fn("O'Brien")` | correct — c'était le cas cassé de FE-04 |
| `");alert(1);//` | `fn("\");alert(1);//")` | le guillemet est échappé par `JSON.stringify` — inerte |
| `</script>` | `<` encodé par `escAttr` → `&lt;` | inerte (et sans objet en contexte d'attribut) |
| `a\` (antislash final) | `fn("a\\")` | `JSON.stringify` double l'antislash — inerte |
| `<img src=x onerror=1>` | `<` et `>` encodés | inerte |

**Mésusage** : si un appelant enrobait malgré tout de guillemets simples, le résultat serait
`fn('"valeur"')` → `SyntaxError`. Échec bruyant et non exploitable — bon mode de défaillance.
**Vérifié : aucun appelant ne le fait** (`grep "'\${escJs(\|\"\${escJs("` → 0 résultat).

**Verdict : je n'ai pas trouvé de contournement.**

---

# 3. Couverture — reste-t-il des interpolations non protégées ?

## 3.1 — `marked.parse` entièrement centralisé

```
grep -n "marked.parse" index.html
7628:    const doc = new DOMParser().parseFromString(marked.parse(markdown || ''), 'text/html');
```

**Une seule occurrence, à l'intérieur de `mdSafe`.** `renderChatMd` appelle désormais `mdSafe()`
dans ses trois branches (`index.html:2809`, `:2811`, `:2813`). Aucun chemin ne contourne
l'assainisseur. FE-05 est clos.

## 3.2 — Gestionnaires inline : inventaire exhaustif

40 gestionnaires inline contiennent une interpolation. Aucun n'est exploitable :

| Catégorie | Nombre | Analyse |
|---|---|---|
| `escJs(...)` | 21 | protégés |
| Identifiants numériques (`${s.id}`, `${p.id}`, `${idx}`, `${cp}`, `${u.id}`…) | 14 | entiers issus de la base, jamais des chaînes |
| `${fid}` / `${sfid}` (`index.html:4875`, `:4910`, `:5663`) | 3 | **sûrs par construction** : `btoa(...).replace(/[^a-zA-Z0-9]/g,'')` — sortie strictement alphanumérique |
| `${folderId}` (`index.html:6592`) | 1 | **sûr par construction** : `folder.replace(/[^a-zA-Z0-9]/g,'_')` |
| `${m.uid}` (`index.html:5785`, `:5788-5790`) | 4 | chaîne serveur, mais contrainte à `\d+` ou `L\d+`. Non exploitable ; à passer sous `escJs` par cohérence |
| `${listType}` (`:6400`), `${s.key}` (`:7203`) | 2 | littéraux du code / clés d'une liste blanche serveur |
| `${escAttr(onSelect)}` (`index.html:4225`) | 1 | **voir FS-04** — expression JS entière, mais valeurs 100 % littérales |

Les emplacements construits sur l'expéditeur (`index.html:4384-4385`, `:5766`, `:6249-6250`,
`:6587-6588`) restent **non exploitables** : la valeur est extraite par
`m.from.match(/[\w.+-]+@[\w.-]+/)`, dont la classe de caractères exclut l'apostrophe et le
guillemet. Ils sont assainis par construction — je le confirme après re-vérification.

**Aucun reste de FE-04** : `grep` des motifs `on…="…'${esc(` et `on…="…'${escAttr(` → 0 résultat.

## 3.3 — `innerHTML` / `insertAdjacentHTML` avec données serveur

Balayage complet des 204 sites : les seules interpolations non passées par un échappeur sont des
**compteurs et des entiers** (`${data.total}`, `${p.errors}`, `${info.spam_count}`,
`${_folderSelection.size}`, `${xhr.status}`, `${Math.random()...}`). Cas particuliers vérifiés :

| Site | Verdict |
|---|---|
| `index.html:2183` — fragment de surlignage ES | `escHighlight` — **correctif FE-01 confirmé**, analyse détaillée dans le rapport d'itération 2 |
| `index.html:6350` — `style="color:${_hc(h.name)}"` | `_hc` (`:6345`) renvoie une valeur d'une table figée, ou `var(--c-text-accent)`. Aucune donnée d'email n'atteint le CSS. |
| `index.html:3676` — résumé de règle | `condTxt`/`actTxt` passent par `esc()` et `escAttr()` |
| `index.html:6526` — `${evt.folder}` | affecté à `.textContent`, pas à `innerHTML` |
| `index.html:3038` — `${userMsgs[0].content}` | construit une **invite envoyée à l'IA**, pas du HTML |
| Famille `${e.message}` / `${data.message}` (≈ 10 sites) | tous dans `alert()` ou `.textContent`. **Aucun** n'atteint `innerHTML`. Les deux qui s'en approchent (`:2187`, `:2426`) utilisent `esc(e.message)`. |

**Conclusion : la surface d'injection HTML du frontend est propre.** Je n'ai identifié aucune
interpolation exploitable restante.

---

# 4. Statut de FE-02 → FE-11

| # | Constat initial | Statut | Vérification |
|---|---|---|---|
| FE-02 | `target_storage` dans le corps au lieu de la query | **CORRIGÉ** | Présent en query aux 4 points d'appel : `:4457` (`'imap'` en dur, correct — la cible est Junk), `:5299` (glisser-déposer, `${targetStorage}`), `:6202` (`_moveEmail`), `:6706` (`moveToFolder`) |
| FE-03 | `page: 1` sur une pagination 0-indexée | **CORRIGÉ, et mieux que demandé** | `:6177-6194` : branche IMAP → appel direct à `POST /empty-folder` (une requête au lieu d'un listage complet) ; branche locale → `page: 0`. Le `storage: 'local'` en dur de `:6187` est bien dans la branche `explorerCurrentStorage === 'local'` : correct. |
| FE-04 | `escAttr`/`esc` dans des littéraux JS | **CORRIGÉ** | `escJs` introduit, 21 usages, 0 reste |
| FE-05 | `marked.parse` non assaini | **CORRIGÉ** | `mdSafe` ; une seule occurrence de `marked.parse`, à l'intérieur |
| FE-06 | `catch {}` silencieux sur les marquages en masse | **CORRIGÉ** | `_reportBulkFailures` (`:5526`) appelé en `:5504` et `:5520`, avec compteur d'échecs et dernière erreur. Le commentaire `:5524-5525` explique le raisonnement. Le seul `catch {}` restant (`:3429`) porte sur un `JSON.parse(localStorage)` — légitime. |
| FE-07 | `api('/accounts')` sans barre oblique → 307 | **CORRIGÉ** | Plus aucune occurrence |
| **FE-08** | `_ctxExportFolder` : `storage` en dur, bouton détaché | **NON CORRIGÉ** | `:5037` toujours `storage: 'imap'`, alors que l'entrée « Exporter en ZIP » figure au menu contextuel de **tous** les dossiers, locaux compris (`:5013`, sans distinction). Mineur. |
| FE-09 | `reply_uid` local jamais traité | **CORRIGÉ** | Traité **côté backend** : `accounts.py:1874` détecte le préfixe `L`, `:1877` bascule sur `_get_local_email`, `:1892` exclut alors le chemin IMAP. Solution plus propre que celle que je suggérais. |
| FE-10 | `allow-same-origin` superflu sur l'iframe | **NON CORRIGÉ** | Durcissement, non bloquant |
| FE-11 | Fonctionnalités documentées absentes de l'UI | **SANS OBJET ICI** | Traité dans `AUDIT_DOCUMENTATION.md` |

**7 corrigés sur 10**, les 3 restants étant les constats explicitement classés mineurs ou
informatifs.

---

# 5. Nouveaux constats

## FS-01 — MAJEUR : le correctif F-03 est inatteignable depuis l'interface

**Certitude : CERTAIN** (formulaire et payload lus intégralement)

Le correctif backend F-03 fait maintenant honorer `smtp_ssl` à la création d'un compte — je l'ai
vérifié en itération 2 par appel direct à l'API. **Mais l'interface ne peut pas s'en servir.**

Le formulaire de compte (`index.html:1226-1245`) comporte onze champs. Le seul contrôle SSL est
pour IMAP :

```html
<input type="checkbox" id="acc-imap-ssl" checked>   <!-- ligne 1235 -->
```

Il n'existe **aucun** `acc-smtp-ssl`. `grep -n "smtp_ssl" index.html` → **0 résultat sur tout le
fichier.**

Le payload de sauvegarde (`index.html:4578-4586`) le confirme :

```javascript
const body = {
    name, imap_host, imap_port, imap_user,
    imap_ssl: document.getElementById('acc-imap-ssl').checked,   // IMAP : transmis
    smtp_host, smtp_port, smtp_user,                              // SMTP : pas de smtp_ssl
};
```

Conséquences, à la création **et** à la modification :

| Chemin | Effet |
|---|---|
| Création (`POST /accounts/`) | `MailAccountCreate.smtp_ssl` retombe sur son défaut `True`. **Tout compte créé depuis l'interface a `smtp_ssl=true`.** |
| Modification (`PUT /accounts/{id}`) | `MailAccountUpdate.smtp_ssl` vaut `None` → la garde `if req.smtp_ssl is not None` saute l'affectation. **Impossible de le désactiver après coup.** |
| Test (`POST /accounts/test-credentials`) | Ne peut pas le transmettre, et le schéma `TestCredentials` n'a pas le champ (N-02). |

**Un utilisateur dont le serveur SMTP ne gère ni SSL implicite ni STARTTLS ne peut pas configurer
un compte fonctionnel depuis l'application.** Le seul moyen est un appel direct à l'API — ce que
j'ai fait pour valider F-03, ce qui explique que le correctif ait été jugé bon.

Cela éclaire aussi la longévité du bug d'origine : le drapeau n'ayant jamais été exposé, personne
n'a pu constater qu'il était ignoré.

**Correctif attendu** : ajouter une case `acc-smtp-ssl` à côté de la ligne 1239, sur le modèle
d'`acc-imap-ssl`, la transmettre dans le payload de `saveAccount` **et** dans celui de
`testCredentials` — ce dernier supposant au préalable l'ajout du champ `smtp_ssl` au schéma
`TestCredentials` côté API (N-02).

## FS-02 — MOYEN : le pilote de synchronisation ne signale jamais un worker absent

**Certitude : CERTAIN**

`index.html:4645` : `api('/accounts/' + id + '/sync', { method: 'POST' })`.

L'endpoint renvoie `200 {"status":"sync_started"}` dès que la tâche est déposée dans Redis, sans
qu'aucun worker n'ait à exister. Le frontend n'affiche donc jamais que rien ne se passe.

C'est le mécanisme exact qui a rendu invisible le blocage de trois mois : l'utilisateur clique
« Synchroniser », l'interface confirme, et rien n'avance. `/admin/status` expose pourtant déjà
`worker: {"status": "offline"}` — l'information existe, elle n'est simplement jamais confrontée à
l'action.

**Correctif attendu** : au retour de `/sync`, ou au chargement de la vue des comptes, lire
`worker.status` depuis `/admin/status` et afficher un avertissement explicite quand il vaut
`offline`. À défaut d'une correction côté frontend, l'endpoint pourrait renvoyer `503` lorsque
aucun worker n'est enregistré.

## FS-03 — MINEUR : `_ctxExportFolder` reste inutilisable sur un dossier local

**Certitude : CERTAIN** — reprise de FE-08, non corrigé.

`index.html:5037` fixe `storage: 'imap'` alors que l'entrée de menu est proposée pour tous les
dossiers (`:5013`). Exporter un dossier local échouera. Deux options : masquer l'entrée pour les
dossiers locaux, ou propager `explorerCurrentStorage`.

Subsistent aussi les deux défauts cosmétiques signalés en FE-08 : `btn.textContent` écrit dans un
nœud déjà détaché (`_closeFolderCtx()` est appelé juste avant), et `originalText` capturé sans
jamais être restauré — indicateur de progression mort.

## FS-04 — MINEUR : `onclick="${escAttr(onSelect)}"` interpole une expression JS entière

**Certitude : CERTAIN** — non exploitable aujourd'hui, fragile par nature.

`index.html:4225` insère une **expression JavaScript complète** fournie par l'appelant :

```javascript
html += `<div class="popup-item" … onclick="${escAttr(onSelect)}" …>`;
```

J'ai vérifié les cinq appelants (`:2272`, `:2291`, `:3789`, `:4253`, `:6686`) : tous passent des
littéraux écrits dans le source, aucune donnée serveur. **Non exploitable en l'état.**

Mais le motif est un piège : c'est le seul endroit du fichier où du code arbitraire est monté dans
un attribut, et `escAttr` n'y protège rien (il ne fait que préserver la syntaxe HTML de
l'attribut). Un futur appelant qui y glisserait un nom de dossier ou un objet obtiendrait une
injection directe. La ligne `:3789` interpole déjà `${ruleId}` dans la chaîne construite.

**Correctif suggéré** : convertir `onSelect` en un nom de fonction transmis par `data-action`, avec
délégation d'événements — le fichier applique déjà ce motif correctement pour les liens du chat IA
(`:2823-2831`).

---

# 6. Ce qui n'a pas été couvert

- **Validation à l'exécution.** Toutes les conclusions reposent sur la lecture du code ; aucune
  charge n'a été exécutée dans un navigateur. `mdSafe` en particulier mériterait une passe
  d'exécution contre un corpus mXSS connu — le raisonnement statique sur la ré-analyse de
  sérialisation a des limites, même s'il ne m'a pas révélé de brèche ici.
- **Comportement visuel et parcours utilisateur** (thèmes, mode lecture, responsive, TinyMCE,
  bulle de composition) : hors de portée d'une analyse statique.
- **Zones non auditées du fichier** : le module de tableau de bord/digest (`:8250-8450`) et les
  widgets IA (`:7960-8000`) n'ont été survolés que par les balayages automatiques
  (interpolations, appels API), sans lecture ligne à ligne.

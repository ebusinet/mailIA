# Plan G — Audit de sécurité méthodique

**Date** : 2026-08-22
**Périmètre** : comptes jetables uniquement (3 GreenMail, 30 et 61 Dovecot), utilisateurs QA 5 et 7.
Le compte professionnel n'a jamais été sollicité autrement qu'en lecture de code de retour.
**Livrable associé** : `tests/qa/suites/securite.py` — 9 tests, groupe `securite`.

---

## Pourquoi ce plan

Les sept failles trouvées avant lui l'ont **toutes** été au passage, en testant autre chose.
Aucune ne venait d'un audit délibéré. Le Plan G est le premier à chercher méthodiquement, sur
les propriétés que personne n'exerce en usage normal.

Il a produit **trois failles**, dont une critique que rien dans les plans précédents n'aurait
rencontrée.

---

## Résumé

| Défaut | Gravité | État |
|---|---|---|
| **G-07** — écriture de fichier arbitraire, en root, par le nom de téléversement | **CRITIQUE** | corrigé, vérifié |
| **G-01** — le jeton de réinitialisation ouvre une session administrateur complète | **CRITIQUE** | corrigé, vérifié |
| **G-02** — la limitation de débit se contourne avec un en-tête HTTP | MAJEUR | corrigé, vérifié |
| **G-10** — les jokers de l'appelant atteignent la requête Elasticsearch | FAIBLE | ouvert |
| G-03 — en-têtes de sécurité | RAS | six présents partout, valeurs correctes |
| G-04 — injection SQL | RAS | cinq surfaces, onze charges, aucun effet |
| G-05 — jetons forgés | RAS | onze variantes, toutes refusées |
| G-06 — oracle sur l'existence des comptes | RAS | réponses indiscernables |
| G-08 — « zip slip » | RAS | protection réelle, mais **héritée** |
| G-09 — surfaces IMAP restantes | RAS | plus aucune interpolation brute |

---

## G-07 — CRITIQUE : écriture de fichier arbitraire, en root

Le défaut le plus grave de toute la campagne, et le seul qui donne l'exécution de code.

`import-mbox` construisait sa destination avec le nom de fichier fourni par le client :

```python
filename  = file.filename or "upload.mbox"      # en-tete Content-Disposition
file_path = str(job_dir / filename)             # aucun assainissement
```

Deux mécanismes distincts, et **le second est celui qui piège** :

- `../../../../tmp/x` sort du répertoire par remontée ;
- `/tmp/x` fait mieux : `Path("/data/imports/job123") / "/tmp/x"` vaut `/tmp/x`. L'opérateur `/`
  de `pathlib` **abandonne entièrement la partie gauche** quand la droite est absolue. Un
  correctif qui n'interdirait que les `..` ne fermerait donc rien.

### La portée, mesurée

```
utilisateur du processus : root  uid 0
/app/src                                ecriture=True
/usr/local/lib/python3.12/site-packages ecriture=True
```

Et surtout, mesuré avec un **vrai compte non-administrateur** (utilisateur 7, `is_admin: false`,
propriétaire de son seul compte mail jetable) :

```
filename="/tmp/qa_g07_na_abs_<m>.txt"             -> HTTP 200
filename="../../../../tmp/qa_g07_na_rel_<m>.txt"  -> HTTP 200
verification : les deux fichiers PRESENTS, contenu = le marqueur envoye
```

**Donc : n'importe quel utilisateur authentifié**, pas seulement un administrateur.

Cette distinction a coûté cinq minutes de mise en place — attacher une boîte Dovecot à
l'utilisateur 7 — et elle décide de la gravité. Sans compte mail, l'utilisateur était arrêté par
le contrôle de propriété **avant** l'endpoint, et je ne pouvais rien conclure. Je l'avais d'abord
présentée comme lue et non mesurée ; elle est maintenant mesurée.

### Ce que je n'ai pas fait

La chaîne complète est graduée, et je le dis plutôt que de la présenter d'un bloc :

| Étape | Statut |
|---|---|
| un utilisateur non-administrateur attache un compte mail | mesuré |
| il écrit un fichier hors du répertoire d'import | mesuré, sous forme inoffensive |
| il écrase un fichier Python sous `/app/src` | **non exécuté** |
| son code s'exécute en root au redémarrage suivant | déduit de `os.access` et `uid=0` |

Je n'ai écrasé aucun fichier de l'application et je ne le ferai pas. Créer un fichier là où on ne
devrait pas suffit à établir le défaut.

### Le correctif, et la vérification des deux branches

`_require_upload_filename(nom, job_dir)` : `basename` après normalisation des séparateurs
Windows, refus des valeurs dégénérées, **puis `realpath` et vérification d'appartenance**.

Un correctif qui normaliserait sans jamais refuser passerait un test qui ne vérifie que
l'absence d'échappement. J'ai donc éprouvé les deux branches :

```
filename=".."                   -> 422 Nom de fichier invalide
filename="."                    -> 422 Nom de fichier invalide
filename="/tmp/legitime.mbox"   -> 200  normalise en `legitime.mbox`, contenu
filename="..\..\..\tmp\x.txt"   -> 200  separateurs Windows traites, normalise
filename=""                     -> 200  repli sur `upload.mbox`
```

**Test laissé** : `SEC-G07`, observé rouge sur le build vulnérable puis vert sur le correctif.

---

## G-01 — CRITIQUE : le jeton de réinitialisation est un jeton d'accès

`decode_access_token` ne vérifiait pas la revendication `purpose`. `decode_reset_token` la
vérifiait. Les deux jetons sont signés par **la même clé** avec **le même algorithme**.

**L'asymétrie était l'indice** : l'auteur savait que `purpose` comptait dans un sens et l'avait
oublié dans l'autre. Vérifié dans les deux sens — `decode_reset_token(jeton d'accès)` rend `None`.

```
create_reset_token(5)  ->  GET /auth/me      200  {"is_admin": true}
                           GET /admin/info   200  {"total_users": 3}
                           GET /admin/users  200  3 utilisateurs
```

Ce n'est pas une session dégradée : c'est la session complète de la victime, droits
d'administration compris.

### Trois facteurs qui font la gravité, à lire ensemble

1. **Le jeton voyageait dans une URL** (`?reset_token=…`) — donc historique du navigateur et
   journaux d'accès nginx.
2. **Il survivait à son usage** : `reset-password` ne l'invalidait pas. Après un changement de
   mot de passe légitime, le lien restait une session valide pour le reste des 30 minutes.
3. **`forgot-password` est non authentifié**, et sur cette instance l'email part de la messagerie
   professionnelle réelle.

**Une sur-affirmation retirée** : j'avais cité l'en-tête `Referer` comme quatrième canal de
fuite. C'est largement faux — `referrer-policy: strict-origin-when-cross-origin` est bien posé,
donc une requête vers un tiers n'emporte que l'origine. La gravité tient, l'énumération était
trop large.

### Le correctif

Séparation **positive** : `create_access_token` estampille `purpose: "access"` et
`decode_access_token` l'**exige**. L'option qui aurait accepté les jetons sans revendication a
été écartée — elle aurait laissé passer tout futur type de jeton qu'on oublierait de déclarer,
c'est-à-dire encore traiter une absence de signal comme une autorisation.

Vérifié avec un jeton authentique : `401` sur les trois endpoints — **401 et non 403**, donc
jeton présenté et refusé, pas absent.

**Test laissé** : `SEC-G01`, observé dans les deux états.

---

## G-02 — MAJEUR : la limitation de débit se contourne avec un en-tête

La question posée était « sur quoi porte le compteur ». Réponse : sur l'IP, pas sur le compte
visé — **donc pas de déni de service ciblé**. Mais la réponse déplaçait le problème, parce que
cette IP était fournie par le client :

```python
forwarded = request.headers.get("x-forwarded-for")
if forwarded:
    return forwarded.split(",")[0].strip()      # le PREMIER element
```

Nginx **ajoute** l'adresse réelle en fin de liste ; le premier élément reste donc celui de
l'appelant.

```
9 tentatives, sans en-tete              {401: 1, 429: 8}
15 tentatives, X-Forwarded-For varie    {401: 15}          aucun refus
```

Les quatre règles tombaient ensemble — connexion, inscription, mot de passe oublié,
réinitialisation partagent cette fonction. Combiné à G-01, cela donnait une émission illimitée de
jetons ouvrant une session.

### Le correctif, éprouvé par huit variantes adverses

Le nouveau code parcourt la liste **de droite à gauche** jusqu'à la première adresse hors
infrastructure. Lire le correctif ne suffit pas : je l'ai attaqué.

```
aucun en-tete (temoin)     limite active      XFF avec espaces        limite active
XFF simple variable        limite active      X-Real-IP variable      limite active
XFF avec virgule finale    limite active      XFF + X-Real-IP         limite active
XFF suivi d'un prive       limite active      XFF suivi d'un public   limite active
```

La variante la plus prometteuse était `10.0.0.1, 198.51.100.<i>` — une adresse publique variable
placée **en dernier**, pour que le parcours de droite à gauche retienne la mienne. Elle échoue
parce que nginx ajoute son propre élément après le mien. **Le correctif tient pour la bonne
raison**, pas par chance : c'est toute la différence entre « prendre le dernier » et « parcourir
de droite à gauche en écartant l'infrastructure ».

Le mode ouvert que j'avais signalé en note est traité : la panne de protection est désormais
journalisée en ERROR — « une protection qui disparaît en silence est pire qu'une protection
absente ».

**Test laissé** : `SEC-G02`.

---

## G-10 — FAIBLE : les jokers de l'appelant atteignent la requête Elasticsearch

`mcp/server:442` : `filters.append({"wildcard": {"from_addr": f"*{from_addr}*"}})`.

```
from_addr='zzz-inexistant-zzz'    total 0
from_addr='*'                     total 83     tout l'index
from_addr='?'                     total 83
from_addr='z*z'                   total 1
```

Les métacaractères de l'appelant sont interprétés. Conséquence directe : un utilisateur cherchant
une adresse contenant littéralement `*` obtient tout au lieu de rien.

**Ce que je n'affirme pas** : l'index est cloisonné par utilisateur (`mailia-{user_id}`), il n'y a
donc pas de fuite entre comptes. Et je n'ai mesuré **aucun** effet sur les temps de réponse — 2 s
pour toutes les variantes, y compris `*a*b*c*d*e*f*`. Sur un index de 83 documents, une
affirmation de déni de service serait spéculative. Je la laisse donc de côté.

---

## Ce qui n'a rien donné, et qui compte autant

### G-03 — en-têtes de sécurité : les six, partout

Neuf types de réponse, y compris les erreurs (401, 403, 404, 422) qui échappent le plus souvent
aux intergiciels parce qu'elles sont produites plus tôt dans la chaîne. Valeurs correctes :
`nosniff`, `DENY`, `max-age=31536000; includeSubDomains`, `strict-origin-when-cross-origin`,
`camera=(), microphone=(), geolocation=()`.

Hors périmètre annoncé, deux observations : pas de `Content-Security-Policy` — défense en
profondeur qui manque, vu les deux XSS de la campagne — et `x-powered-by: PleskLin` qui divulgue
le panneau d'hébergement.

### G-04 — injection SQL : rien

Cinq surfaces × onze charges. Trois signaux surveillés séparément, parce qu'ils révèlent trois
choses différentes : un **500** (la charge atteint la couche données), une **trace SQL** dans la
réponse (fuite), une **anomalie temporelle** (`pg_sleep` exécuté). Aucun des trois. Toutes les
réponses à 0,13 s, `pg_sleep(5)` compris. Paramètres liés SQLAlchemy.

### G-05 — jetons forgés : onze variantes, toutes refusées

Signature modifiée, signature vide, `alg=none`, `alg=NONE`, expiré, `sub` inexistant, `sub` non
numérique, deux segments, chaîne quelconque. **Aucun accepté.** `jwt.decode` reçoit
`algorithms=[...]` épinglé, donc la confusion d'algorithme est fermée par construction.

Une note qui vaut d'être gardée : `deps.py` fait `int(user_id)` **sans garde**. Une valeur hostile
provoquerait une exception — mais la signature est vérifiée avant, si bien que `int()` n'est
jamais atteint. **La protection est positionnelle** : elle tient à l'ordre des vérifications, pas
à un contrôle. `SEC-G05` la surveille : un 500 y apparaîtra si quelqu'un réorganise
`decode_access_token`.

### G-06 — pas d'oracle sur l'existence des comptes

Un non-administrateur voit `[]`. Les comptes 1, 3, 30 et 99999 rendent **le même 404 avec le même
message** : impossible de distinguer « n'existe pas » de « existe mais pas à vous ».

### G-08 — « zip slip » : protégé, mais par héritage

`zf.extractall(tmp_dir)`, deux sites, tous deux en aval d'un chemin dont G-07 a montré qu'il était
mal gardé. Deux mesures, parce que la première seule n'aurait rien prouvé :

- par le pipeline réel, aucune entrée hostile n'échappe — **et le job rend `status: done`**, donc
  l'extraction a bien eu lieu. Sans cette seconde vérification, un pipeline échouant en amont
  aurait produit la même absence de fichier ;
- en isolant `extractall`, on voit ce qu'il fait : l'entrée `../../../../tmp/x` atterrit à
  `<extraction>/tmp/x`. Les composants `..` et la racine sont retirés, tout reste contenu.

**La protection vient de la bibliothèque standard, pas de l'application.** Même motif que le CRLF
bloqué par le module `email`, ou que l'API protégée par un retrait de guillemets fortuit. Elle
disparaîtrait si quelqu'un remplaçait `extractall` par une boucle `zf.extract()` — refactorisation
banale, sans rapport apparent avec la sécurité — **et aucun test de comportement ne changerait de
couleur**.

D'où deux tests : `SEC-G08` pour la propriété, `SEC-G09` pour la forme du code.

### G-09 — surfaces IMAP restantes : plus aucune interpolation brute

Balayage systématique des appels `_conn.<commande>(` contenant une interpolation, sur les trois
modules :

| Surface | État |
|---|---|
| ensembles d'UID (`STORE`, `SEARCH UID`) | `_check_uid` / `_check_uid_list`, chiffres seuls |
| drapeaux (`STORE ... FLAGS`) | liste blanche stricte depuis A-12 |
| critères et valeurs de recherche | `_imap_criteria` / `_imap_astring` |
| spécificateurs de partie (`BODY[...]`) | aucun — les pièces jointes passent par le module `email` |
| arguments d'`APPEND` (drapeaux, date) | constantes littérales et `Time2Internaldate` |
| dossier cible d'`APPEND` direct | `MoveRequest` porte `_require_folder_name` |

---

## Le tableau qui dit ce que cette suite vaut réellement

Presque aucune suite de tests ne fournit cette information. Elle vaut plus que « 9 sur 9 au vert ».

| Observé dans les deux états, sur le défaut réel | Motif éprouvé | Cru sur parole |
|---|---|---|
| `SEC-G01` — 200 + `is_admin` avant, 401 après | `SEC-G09` — motif vérifié sur 4 lignes témoins | `SEC-G03` |
| `SEC-G02` — `{401: 15}` avant, limite active après | | `SEC-G04` |
| `SEC-G07` — fichiers hors périmètre avant, aucun après | | `SEC-G05` |
| | | `SEC-G06` |
| | | `SEC-G08` |

**Trois sur neuf** ont démontré qu'ils détectent le défaut qu'ils prétendent garder. Un a vu son
motif éprouvé sans être vu rouge sur du vrai code. **Cinq sont crus sur parole** — ils passent,
mais rien ne prouve qu'ils échoueraient si la propriété se cassait.

---

## Deux observations qui ne sont pas des défauts

**`/auth/register` refuse les adresses en `.local`** (validation `EmailStr`), alors que le compte
QA historique, créé par SQL direct, en porte une. Deux portes de création d'utilisateur, une seule
validant. Sans conséquence ici — mais c'est exactement la forme dont sont sorties trois failles de
cette campagne.

**Le limiteur échoue en mode ouvert** : Redis indisponible, la requête passe. C'est un arbitrage
disponibilité/sécurité défendable, désormais journalisé en ERROR. À décider, pas à corriger
d'office.

---

## État de l'environnement

- **Compte professionnel** : jamais sollicité autrement que par lecture de codes de retour. Aucun
  contenu lu, aucun journal consulté, aucune boîte ouverte.
- **`SECRET_KEY` non extraite.** Forger un jeton valide au nom d'un autre utilisateur aurait
  demandé de fabriquer une crédence d'usurpation du compte professionnel. Conséquence assumée :
  je ne peux pas dire lequel des deux contrôles refuse un `sub` étranger, seulement que les deux
  cas sont refusés.
- **Utilisateur 7 (`qa-nonadmin@example.com`) et compte mail 61 conservés**, documentés dans la
  mémoire du projet. C'est le seul moyen de mesurer une frontière administrateur sans lire le
  code — et lire le code s'est révélé insuffisant deux fois pendant la campagne, dans les deux
  sens. `sync_enabled: false` pour qu'aucune synchronisation ne parte de ce compte.
- Artefacts de test supprimés après chaque mesure, vérification incluse.

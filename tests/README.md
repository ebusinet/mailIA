# Suite de non-régression MailIA

Une commande, aucune installation :

```bash
python3 tests/run.py
```

Code de sortie : `0` si aucun échec, `1` s'il y a au moins un échec, `2` si le garde-fou de
sécurité a refusé de démarrer.

---

## 1. Le garde-fou de sécurité — à lire en premier

Cette suite **écrit, déplace et supprime des emails**. MailIA est utilisé avec un compte de
messagerie professionnel réel contenant des années de courrier.

Avant le premier test, `tests/qa/guard.py` vérifie que :

1. la configuration est exploitable (URL d'API, identifiants numériques) ;
2. le jeton fourni authentifie bien le compte QA (`GET /auth/me`) ;
3. cette identité ne contient aucun motif interdit (`ebusinet`, `ovh.net`, `pimienta`) ;
4. **tous** les comptes de messagerie visibles par ce jeton pointent sur un serveur jetable
   (`greenmail`, `localhost`), **en IMAP comme en SMTP** — un seul compte de production
   accessible suffit à tout arrêter, même si les tests ne le visent pas ;
5. le compte cible existe et fait partie de ces comptes.

### L'identité numérique est dérivée, jamais codée en dur

Les outils MCP s'exécutent sous un identifiant numérique d'utilisateur. Cet identifiant n'est
pas une constante : il est **lu depuis `/auth/me`** une fois l'identité validée, et publié par
`resolved_user_id()`, qui lève tant que le garde-fou n'a pas tourné.

Une constante en dur qu'on doit ensuite penser à comparer se désynchronise en silence — le
scénario concret est « le compte QA a été supprimé et son identifiant réattribué », ou « la
suite est pointée sur une autre instance où cet identifiant appartient à quelqu'un de réel ».
Ici, le chemin API et le chemin MCP utilisent par construction la même identité, celle qui
vient d'être vérifiée.

Si l'une de ces conditions n'est pas remplie, **la suite ne démarre pas** : elle ne saute pas
les tests, elle s'arrête avec le code 2 et n'exécute rien.

Le garde-fou est lui-même testé : quatorze tests (`GUARD-*`) le nourrissent de
configurations synthétiques et exigent qu'il refuse ce qu'il doit refuser — y compris un
compte réel qui ne serait pas la cible — et qu'il accepte une configuration valide. Ces neuf
tests ne touchent pas le réseau et s'exécutent même API éteinte :

```bash
python3 tests/run.py --no-guard        # uniquement si aucun jeton n'est défini
```

**Élargir une liste blanche dans `guard.py` est une décision de sécurité, pas un ajustement de
confort.** Si un test échoue à cause du garde-fou, la réponse par défaut est de corriger le
jeton ou la cible.

La suite ne démarre jamais `mailia-worker` ni `mailia-beat`.

---

## 2. Configuration

| Variable | Défaut | Rôle |
|---|---|---|
| `MAILIA_QA_TOKEN` | — | Jeton JWT du compte QA. **Obligatoire.** |
| `MAILIA_API_URL` | `https://mailia.expert-presta.com/api` | Base de l'API |
| `MAILIA_QA_ACCOUNT_ID` | `3` | Compte de messagerie de test |
| `MAILIA_SSH_HOST` | `expert-presta` | Hôte SSH pour les tests de rang 2 |
| `MAILIA_API_CONTAINER` | `mailia-api` | Conteneur applicatif |
| `MAILIA_TIMEOUT` | `60` | Délai HTTP, en secondes |

Le jeton peut aussi être placé dans `tests/.qa_token` (déjà ignoré par git — ne le versionnez
jamais). Il expire : s'il est refusé, le garde-fou le dit explicitement.

```bash
export MAILIA_QA_TOKEN="eyJhbGciOi..."
python3 tests/run.py
```

---

## 3. Options

```bash
python3 tests/run.py -l                 # lister les tests sans les exécuter
python3 tests/run.py -g isolation       # n'exécuter qu'un groupe
python3 tests/run.py -k ISO-01          # un test précis (identifiant ou titre)
python3 tests/run.py -k duplication -v  # trace complète des échecs
```

Groupes : `isolation`, `duplication`, `search`, `smtp_paths`, `emails`, `rules_and_storage`,
`misc`, `guard_selftest`.

---

## 4. Deux rangs d'exécution

| Rang | Prérequis | Contenu |
|---|---|---|
| 1 | Accès HTTP à l'API | La grande majorité des tests |
| 2 | `ssh <hôte>` + `docker exec` | Outils MCP, injection IMAP directe, requêtes en base, file Celery |

Si l'accès au conteneur est indisponible, les tests de rang 2 rendent **SKIP avec la raison**.
Ils ne passent jamais silencieusement : un test qui ne peut pas s'exécuter le dit.

**Un SKIP n'est pas un succès.** Une suite qui affiche `40 PASS / 0 FAIL / 30 SKIP` ne prouve
que ce que couvrent les 40.

---

## 5. Ce que la suite couvre

78 tests, chacun rattaché au bug qu'il empêche de revenir. L'identifiant du bug d'origine
(`F-00`, `N-04`, `FE-01`…) est affiché à côté de chaque test et rappelé dans le rapport
d'échec.

| Groupe | Tests | Ce qui est vérifié |
|---|---|---|
| `isolation` | 7 | **Le plus important.** Cloisonnement entre utilisateurs : les 8 endpoints API et **tous** les outils MCP acceptant un `account_id` refusent un compte non possédé ; un email local appartenant à un autre utilisateur n'est ni lisible ni déplaçable ; `trigger_sync` ne met jamais en file la synchro d'autrui. |
| `duplication` | 6 | Déplacement et suppression ne dupliquent pas : comptage source **et** destination avant/après, total inchangé exigé. Un code HTTP 200 ne prouve rien ici. |
| `search` | 7 | La recherche renvoie 200 avec des résultats ; le champ `score` n'est jamais `null` ; index absent traité ; `escHighlight()` toujours en place côté client. |
| `smtp_paths` | 9 | Les six chemins d'envoi, plus deux tests **structurels** : aucun `starttls()` hors du client centralisé, et tous les appelants transmettent le drapeau TLS. |
| `emails` | 10 | Listage, pagination, six combinaisons de tri, cohérence des trois sources de date, filtres, pièces jointes, export. |
| `rules_and_storage` | 8 | Opérateurs de règles, destinataires multiples, analyse du markdown IA, stockage local aller-retour sans perte de date, import et dédoublonnage. |
| `misc` | 17 | Dossiers, spam, contacts, signatures, création de compte, fournisseurs IA, administration, en-têtes de sécurité, limitation de débit. |
| `guard_selftest` | 14 | Le garde-fou lui-même. |

### `DUP-02` : un bug trouvé par la suite, puis corrigé

Ce test a échoué à la toute première exécution de la suite et a mis au jour une perte de
données : `POST /message/{uid}/move` avec `target_folder: ""` répondait
`200 {"status": "moved"}`, retirait l'email du dossier source et ne le déposait nulle part.
Le défaut a été corrigé ; le test est vert et garde la porte fermée.

### Deux tests structurels, à ne pas supprimer

`SMTP-06` et `SMTP-07` ne testent pas un comportement mais une **forme du code**. Le défaut
« STARTTLS forcé » a resurgi trois fois parce que le même bloc de quatre lignes était
copié-collé d'un appelant à l'autre et que seules certaines copies étaient corrigées. Tant que
`starttls()` n'existe qu'à un seul endroit, il ne peut plus diverger ; si quelqu'un le recopie,
`SMTP-06` tombe. C'est le seul type de test qui protège contre ce mode de récidive.

---

## 6. Ce que la suite ne couvre pas

| Domaine | Pourquoi |
|---|---|
| **Le worker Celery** | Aucun correctif du worker n'est exercé : limites de temps, verrou Redis, disjoncteur IA, persistance de l'avancement. Les valider suppose de démarrer `mailia-worker`, ce que la suite s'interdit. **C'est le plus gros angle mort.** |
| **Le mot de passe oublié, de bout en bout** | `auth.py` choisit comme expéditeur le premier compte administrateur disposant d'un SMTP, sans `ORDER BY`. Sur une instance de production, le déclencher enverrait un vrai message depuis la messagerie réelle. Seule la branche sans effet de bord est testée (`SMTP-08`). |
| **Le rendu de l'interface** | Thèmes, mode lecture, TinyMCE, bulles de chat : hors de portée sans pilotage de navigateur. Deux propriétés critiques sont malgré tout vérifiées par lecture du source (`SRCH-07`, `ACC-02`). |
| **La qualité des réponses IA** | Dépend d'un fournisseur configuré. Seules la mécanique et la gestion d'erreur sont testées. |
| **Le bot Telegram** | Nécessite un jeton de bot. |
| **La recherche sémantique** | Inerte par construction : aucun embedding n'est généré à l'indexation. |
| **La montée en charge** | Aucun test de performance ni de volumétrie. |

---

## 7. Rejouabilité

La suite est conçue pour être relancée indéfiniment sans dérive :

- **chaque message déposé porte un marqueur unique** (`[QA-xxxxxxxx]`) et les tests ne comptent
  que leurs propres messages : un résidu laissé par une exécution précédente ne peut ni masquer
  ni provoquer un échec ;
- **aucun dossier n'est partagé entre tests.** Chacun travaille dans un espace dédié à nom
  fixe dérivé de son identifiant (`QA_AT_DUP01`, `QA_AT_MAIL07`…). Le nom est fixe donc
  l'arborescence ne croît pas d'une exécution à l'autre, et il est exclusif donc aucun test
  ne peut en perturber un autre. C'était la dernière source d'échecs qui ne se reproduisaient
  jamais en exécution isolée ;
- les objets nommés (règles, contacts, signatures, comptes, fournisseurs) portent un suffixe
  aléatoire et sont supprimés dans un `finally` ;
- les messages de test sont déposés par import mbox, avec des `Message-ID` uniques ;
- un **balayage final** supprime tout objet nommé `QA autotest*` (règles, contacts, groupes,
  signatures, fournisseurs, comptes) : un `finally` peut lui-même échouer, ce filet garantit
  qu'une exécution ne laisse rien derrière elle ;
- les tests sont indépendants : chacun crée ce dont il a besoin et ne suppose rien de l'ordre
  d'exécution. `-k` permet donc d'en rejouer un seul.

Vérifié : deux exécutions complètes consécutives donnent un résultat **strictement identique**
(77 PASS / 0 FAIL / 1 SKIP), comparé statut par statut. Aucun test instable.

Deux résidus connus, sans conséquence : les emails déposés dans `Trash` par les tests de
suppression, et les dossiers temporaires que le serveur de test refuse parfois de supprimer
(GreenMail répond `socket error: EOF` sur certains `DELETE`).

---

## 8. Choix technique : pas de pytest

La suite n'utilise que la **bibliothèque standard**. `httpx` figure dans `requirements.txt`
mais n'est installé que dans l'image Docker, pas sur la machine de développement ; `pytest`
n'est nulle part.

Le critère retenu est la probabilité qu'elle soit réellement relancée. Une suite qui exige
`pip install pytest httpx` dans un environnement virtuel avant sa première exécution est une
suite qu'on ne lance pas. `python3 tests/run.py` fonctionne immédiatement, partout.

Second argument : une partie des vérifications passe de toute façon par `ssh` et
`docker exec` (outils MCP, injection IMAP, base, file Celery). Un harnais pytest n'apporterait
pas grand-chose à ce type de travail.

Si le projet adopte un jour pytest pour des tests unitaires, ces deux approches cohabitent
sans difficulté : les suites sont de simples fonctions décorées, réutilisables telles quelles.

---

## 9. Trois statuts, pas deux

| Statut | Signification | Code de sortie |
|---|---|---|
| `PASS` | le comportement attendu est vérifié | — |
| `FAIL` | **le produit** ne se comporte pas comme attendu : régression | 1 |
| `ERROR` | **le test lui-même** est cassé (`NameError`, `IndexError`…) | 1 |
| `SKIP` | le test ne peut pas s'exécuter, avec la raison | — |

La distinction `FAIL` / `ERROR` n'est pas cosmétique. Sans elle, un défaut de la suite
s'affiche sous la référence d'un bug produit (« *bug d'origine : F-07* ») et un lecteur en
conclut à tort qu'une régression est revenue — le plus mauvais signal possible pour une suite
censée être relancée sans supervision.

---

## 10. Ajouter un test

```python
# tests/qa/suites/mon_domaine.py
from ..core import API, CFG, expect, expect_status, test

@test("MON-01", "Description lisible du comportement attendu", "N-42")
def mon_test():
    r = API.get(f"/accounts/{CFG.account_id}/quelque-chose")
    expect_status(r, 200, "appel")
    expect(r.json()["champ"] == "valeur",
           "message expliquant ce qui ne va pas et pourquoi c'est un problème")
```

Puis importer le module dans `tests/run.py`.

Trois règles :

1. **Vérifier l'effet réel, pas le code HTTP.** La moitié des bugs de la campagne renvoyaient
   `200` en ne faisant rien, ou en faisant le contraire de ce qu'ils annonçaient.
2. **Le message d'échec doit expliquer la conséquence**, pas seulement constater la
   différence. C'est ce qu'on lira six mois plus tard.
3. **Renseigner le bug d'origine** en troisième argument : il apparaît dans le rapport et
   raccroche l'échec à son histoire.

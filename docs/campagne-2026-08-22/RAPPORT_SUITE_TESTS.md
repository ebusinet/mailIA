# Suite de non-régression MailIA — rapport de construction

**Date** : 2026-08-22
**Livrée dans** : `/var/www/mailia/tests/` — 2572 lignes de Python + un README de 219 lignes
**Rien n'a été commité.**

---

## Résultat de la première exécution complète

```
71 PASS   1 FAIL   1 SKIP   (137s)
```

**Trois exécutions complètes consécutives donnent un résultat strictement identique** (diff des
statuts : aucune différence). Aucun test instable.

L'unique échec est un **bug réel découvert par la suite à sa toute première exécution** — voir
IT4-01 ci-dessous. Le SKIP est honnête et documenté : il porte sur une propriété que le worker
Celery, volontairement arrêté, rend inobservable.

### Innocuité vérifiée après construction

| Contrôle | État |
|---|---|
| File Celery | **0** |
| Sauvegarde `celery_backup_20260822` | **26 940**, intacte |
| Compte 1 (`pro OVH`, utilisateur 2) | `sync_enabled=t`, `last_sync_at=2026-05-20 10:05:51`, **1227** emails locaux — identique aux quatre relevés précédents |
| `mailia-worker` / `mailia-beat` | jamais démarrés |

---

## 1. Choix technique : exécutable autonome, pas pytest

Le critère que tu as fixé est la probabilité que la suite soit réellement relancée. J'ai tranché
sur une vérification, pas sur une préférence :

```
$ python3 -c "import httpx"
ModuleNotFoundError: No module named 'httpx'
```

`httpx` figure dans `requirements.txt` mais **n'est installé que dans l'image Docker**, pas sur la
machine où l'utilisateur travaille. `pytest` n'est nulle part. L'un comme l'autre imposeraient
`pip install` dans un environnement virtuel avant la première exécution — et une suite qu'il faut
installer est une suite qu'on ne lance pas.

La suite n'utilise donc que la **bibliothèque standard** : `python3 tests/run.py` fonctionne
immédiatement, sur n'importe quelle machine disposant de Python 3 et d'un accès réseau.

Second argument, moins visible mais décisif à l'usage : une partie des vérifications passe de
toute façon par `ssh` et `docker exec` — outils MCP, injection IMAP, requêtes en base, longueur de
la file Celery. Un harnais pytest n'apporte rien à ce type de travail.

Si le projet adopte un jour pytest pour des tests unitaires, les deux cohabitent sans friction :
les suites sont de simples fonctions décorées, réutilisables telles quelles.

---

## 2. Le garde-fou

C'est la pièce que j'ai écrite en premier et celle qui a reçu le plus d'attention.

### Ce qu'il vérifie, avant le premier test

1. `GET /auth/me` — l'identité authentifiée appartient à la liste blanche QA.
2. Elle ne contient aucun motif interdit : `ebusinet`, `ovh.net`, `pimienta`.
3. **Tous** les comptes de messagerie visibles par ce jeton pointent sur un hôte jetable.
4. Le compte cible existe et fait partie de ceux-là.

Le point 3 est le plus important et c'est un choix de conception délibéré : **on ne se contente
pas de valider la cible**. Un jeton qui *voit* un compte de production n'a rien à faire ici, même
si les tests visent un autre compte. `GUARD-05` couvre exactement ce cas.

Les motifs interdits constituent une **défense redondante et volontaire** : si quelqu'un élargit
un jour `ALLOWED_IDENTITIES` ou `ALLOWED_MAIL_HOSTS` par mégarde, ce second filtre tient encore.

En cas d'échec, la suite **refuse de démarrer** : code de sortie 2, aucun test exécuté.

### Il est lui-même testé

Neuf tests (`GUARD-01` à `GUARD-09`) le nourrissent de configurations synthétiques, sans réseau.
Ils s'exécutent même API éteinte. Ils vérifient qu'il refuse :

- une identité hors liste blanche, une identité du domaine réel (quatre variantes de casse), une
  identité vide ;
- un compte pointant hors serveur de test ;
- **un compte réel visible mais non ciblé** ;
- le motif interdit dans chacun des cinq champs d'un compte ;
- une liste de comptes vide, un compte cible absent.

Et — symétrie indispensable, un garde-fou qui refuse tout serait tout aussi inutilisable —
qu'il **accepte** une configuration valide (`GUARD-09`).

### Refus vérifiés en conditions réelles

```
sans jeton                    → REFUS : aucun jeton fourni
jeton invalide                → REFUS : jeton refuse par l'API (401)
compte cible inexistant       → REFUS : le compte cible 999 n'est pas accessible
--no-guard avec jeton présent → REFUS : option interdite quand un jeton est present
```

Ce dernier point mérite un mot : `--no-guard` existe uniquement pour diagnostiquer le garde-fou
hors ligne. Il **refuse de s'exécuter dès qu'un jeton est présent**, pour qu'il ne puisse jamais
servir de contournement commode.

---

## 3. Ce que couvre la suite

73 tests, chacun rattaché au bug qu'il empêche de revenir. L'identifiant d'origine (`F-00`,
`N-04`, `FE-01`…) s'affiche à côté de chaque test et est rappelé dans le rapport d'échec.

| Groupe | Tests | Priorité |
|---|---|---|
| `isolation` | 7 | 1 — cloisonnement |
| `duplication` | 6 | 2 — non-duplication |
| `search` | 7 | 3 — recherche |
| `smtp_paths` | 9 | 4 — les six chemins SMTP |
| `emails` | 10 | 5 |
| `rules_and_storage` | 8 | 5 |
| `misc` | 17 | 5 |
| `guard_selftest` | 9 | — |

### Cloisonnement (`ISO-01` à `ISO-07`)

Le test le plus important de la suite est `ISO-05` : il **n'énumère pas les outils MCP à la main,
il lit leur signature**. Tout outil acceptant un `account_id` est appelé automatiquement avec un
compte étranger et des ressources inexistantes. Un outil ajouté plus tard sera donc couvert sans
qu'on y pense — c'est précisément le mode de récidive qui a permis à N-04 d'exister après la
correction de F-00.

`ISO-06` va plus loin : il cherche en base un email local appartenant réellement à un **autre
utilisateur**, relève son dossier, tente de le lire puis de le déplacer, et vérifie qu'il n'a pas
bougé. C'est une sonde sur donnée réelle, sans risque : si le contrôle fonctionne — ce qui est le
cas — rien n'est modifié.

`ISO-05` distingue trois issues, et c'est ce qui le rend utile plutôt que bruyant :

- **fuite** — l'appel a réussi : échec dur ;
- **refus de propriété** — le message attendu : bon ;
- **garde en amont** — refus émis avant le contrôle de propriété (le garde `worker_online()` de
  `trigger_sync`). Rien n'est exécuté, donc aucune fuite, mais le contrôle n'est pas exercé : le
  test le **signale explicitement dans sa sortie** plutôt que de compter un PASS complet.

Cinq outils (`count_emails`, `search_emails`, `get_folders_stats`, `get_senders_stats`,
`get_processing_logs`) ne refusent pas : ils sont isolés par partitionnement — index Elasticsearch
par utilisateur, filtre `user_id` en base. C'est correct, je l'ai revérifié dans le code, et c'est
documenté dans le test avec la liste nommée.

### Non-duplication (`DUP-01` à `DUP-06`)

Toujours le même motif, et c'est lui qui compte : **compter la source ET la destination avant et
après, exiger un total inchangé**. Un code HTTP 200 ne prouve rien ici — c'était exactement le
symptôme de F-02, où l'API renvoyait une erreur pendant que l'email était dupliqué.

`DUP-05` vérifie les flags par leur effet observé, jamais par le code de retour : même cause
racine, le `STORE` non parenthésé.

### Deux tests structurels que je recommande de ne pas supprimer

`SMTP-06` et `SMTP-07` ne testent pas un comportement mais une **forme du code**. Le défaut
« STARTTLS forcé » a resurgi trois fois parce que le même bloc de quatre lignes était copié-collé
d'un appelant à l'autre et que seules certaines copies étaient corrigées à chaque passage. Tant
que `starttls()` n'existe qu'à un seul endroit, il ne peut plus diverger ; si quelqu'un le
recopie, `SMTP-06` tombe.

C'est le seul type de test qui protège contre ce mode de récidive, et aucun test fonctionnel ne
l'aurait attrapé : chaque copie fonctionnait parfaitement dans son propre contexte.

---

## 4. Bug découvert par la suite — IT4-01

**`POST /accounts/{id}/message/{uid}/move` avec `target_folder: ""` détruit l'email.**

Découvert à la première exécution de `DUP-02`, que j'avais écrit avec une prémisse différente
(je supposais un refus).

```
POST /accounts/3/message/{uid}/move?folder=QA_AUTOTEST_SRC
     {"target_folder": ""}
→ 200 {"status": "moved", "target_folder": ""}

Dossier source : 3 → 2 messages
Recherche du sujet sur les 33 dossiers du serveur, en imaplib direct : 0 occurrence
```

L'email n'est ni dans la source, ni dans une destination, ni dans un dossier créé au passage.
**Il est perdu, et l'API annonce un succès.** `MoveRequest.target_folder` n'est validée nulle
part.

Le test reste rouge tant que ce n'est pas corrigé. C'est volontaire et signalé dans le README :
une suite qui masquerait un défaut réel pour afficher du vert n'aurait aucune valeur.

---

## 5. Fiabilité : ce que j'ai dû corriger dans la suite elle-même

Tu as posé le bon critère — 40 tests qui passent toujours valent mieux que 120 instables. Les
premières exécutions complètes ont donné 7 puis 6 puis 2 échecs, alors que **chaque groupe passait
isolément**. Le diagnostic a pris du temps et mérite d'être consigné, parce que la conclusion
oriente toute maintenance future de cette suite.

**Ce n'était pas un problème de temporisation.** J'ai vérifié séparément que le dépôt de messages,
le vidage d'un dossier et le vidage immédiatement après un import sont tous synchrones et fiables
sur ce serveur. J'ai aussi testé — et écarté — l'hypothèse d'un `delete-folder?force=true` qui
viderait des dossiers voisins (la commande `LIST` utilisée pour découvrir les sous-dossiers ne
renvoie rien d'inattendu).

La cause réelle était l'**interférence entre tests partageant un dossier**. Trois corrections,
par ordre d'efficacité :

1. **Marqueur unique par dépôt.** Chaque message reçoit un `[QA-xxxxxxxx]` dans son objet, et les
   tests ne comptent que leurs propres messages. Un résidu ne peut plus ni masquer ni provoquer un
   échec.
2. **Dossier dédié par test** pour ceux qui portent sur l'état *global* d'un dossier — filtres,
   règles, vidage. Nom fixe dérivé de l'identifiant (`QA_AT_RULE02`) : espace exclusif, sans
   interférence, et sans faire croître l'arborescence d'une exécution à l'autre.
3. **Balayage final** supprimant tout objet nommé `QA autotest*`. Un `finally` par test peut
   lui-même échouer — c'est arrivé une fois, une règle a fuité. Ce filet garantit qu'une exécution
   ne laisse rien derrière elle.

Deux erreurs de ma part corrigées au passage, qui illustrent le même piège : un test peut échouer
parce que sa *prémisse* est fausse, pas le code. `SMTP-07` capturait le wrapper local
`_smtp_connect(` en plus de la vraie fonction ; `RULE-02` attendait `age older_than 52w` = 1 alors
que ses trois messages, à dates figées, avaient tous franchi le seuil. Ce dernier utilise
désormais des dates **relatives à l'instant présent** — figées, elles auraient fini par cesser de
discriminer, et le test serait devenu silencieusement inutile.

---

## 6. Ce que j'ai délibérément laissé de côté

| Domaine | Raison |
|---|---|
| **Le worker Celery** | Aucun de ses correctifs n'est exercé : limites de temps, verrou Redis, disjoncteur IA, persistance de l'avancement. Les valider suppose de démarrer `mailia-worker`, ce que la suite s'interdit. **C'est le plus gros angle mort**, et il est nommé comme tel dans le README. |
| **Le mot de passe oublié, de bout en bout** | `auth.py:113-124` choisit comme expéditeur le premier compte administrateur disposant d'un SMTP, **sans `ORDER BY`**. Sur cette instance, le compte réel serait très probablement retenu : une requête non authentifiée ferait envoyer un vrai message depuis la messagerie professionnelle (IT3-02). `SMTP-08` ne teste que la branche sans effet de bord. La couverture du chemin d'envoi passe par `SMTP-06`/`SMTP-07`, structurels. |
| **Rendu de l'interface** | Hors de portée sans pilotage de navigateur. Deux propriétés critiques sont malgré tout verrouillées par lecture du source : `SRCH-07` vérifie que `escHighlight()` est toujours appliquée aux fragments de surlignage (sans quoi la XSS stockée FE-01 revient), `ACC-02` que le formulaire de compte expose toujours le réglage SSL du SMTP (sans quoi le correctif F-03 redevient inatteignable). |
| **Qualité des réponses IA** | Dépend d'un fournisseur configuré. Seules la mécanique et la gestion d'erreur sont testées. |
| **Bot Telegram** | Aucun jeton de bot configuré. |
| **Recherche sémantique** | Inerte par construction : aucun embedding n'est généré à l'indexation. Tester une fonctionnalité morte n'apporte rien tant qu'elle n'est pas implémentée. |
| **Montée en charge** | Aucun test de performance ni de volumétrie. |
| **Le test « index Elasticsearch absent » (F-06)** | Le provoquer exigerait de créer un utilisateur jamais synchronisé. `SRCH-05` vérifie à la place, par lecture du source, que la route intercepte toujours `NotFoundError`. C'est un repli assumé, signalé dans le test. |

J'ai aussi renoncé à des tests que j'avais écrits puis retirés : une vérification du rollback de
`move_email` en cas d'échec du `STORE` (non déclenchable sans injecter une panne serveur) et un
test de purge Elasticsearch à la suppression d'un dossier (le serveur de test refuse de supprimer
les dossiers concernés). Les couvrir aurait demandé des mécanismes d'injection de panne
disproportionnés par rapport au gain.

---

## 7. Structure livrée

```
tests/
├── README.md                      219 l.  comment lancer, ce qui est couvert, ce qui ne l'est pas
├── run.py                         159 l.  point d'entrée, exécution, rapport, balayage final
├── .gitignore                             .qa_token n'est jamais versionné
└── qa/
    ├── core.py                    502 l.  client HTTP stdlib, accès conteneur, registre, fixtures
    ├── guard.py                   139 l.  garde-fou (en-tête : pourquoi il existe)
    └── suites/
        ├── isolation.py           295 l.  cloisonnement — 7
        ├── duplication.py         158 l.  non-duplication — 6
        ├── search.py              109 l.  recherche — 7
        ├── smtp_paths.py          195 l.  chemins SMTP — 9
        ├── emails.py              220 l.  listage, tris, filtres, dates — 10
        ├── rules_and_storage.py   305 l.  règles, stockage local, import — 8
        ├── misc.py                396 l.  dossiers, spam, contacts, admin, sécurité — 17
        └── guard_selftest.py       92 l.  le garde-fou lui-même — 9
```

Chaque module s'ouvre sur l'historique des défauts qu'il couvre. Chaque message d'échec explique
la **conséquence** plutôt que de constater la différence — c'est ce qu'on lira dans six mois, sans
le contexte de cette campagne.

---

## 8. À faire par l'utilisateur

1. **Fournir le jeton** : `export MAILIA_QA_TOKEN="..."` ou `tests/.qa_token`. Le jeton actuel
   expire vers le 21 septembre 2026 ; le garde-fou le dira explicitement le jour venu.
2. **Décider du commit.** Rien n'a été commité. Le répertoire est prêt à être versionné tel quel ;
   `.gitignore` protège déjà le jeton.
3. **Corriger IT4-01** pour repasser la suite au vert.
4. **Rejouer la suite une fois le worker autorisé à redémarrer** : `ISO-07` sortira alors du SKIP,
   et les six points du worker listés au rapport d'itération 3 pourront enfin être validés.

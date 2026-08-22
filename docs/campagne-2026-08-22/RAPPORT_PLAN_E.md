# Plan E — Concurrence

**Date** : 2026-08-22
**Périmètre** : GreenMail, compte 3. Dovecot laissé à `fixer`, aucune ressource commune.
**Livrable associé** : `tests/qa/course.py` (harnais) et `tests/qa/suites/concurrence.py` (5 tests).

---

## Résumé

| Défaut | Gravité | État |
|---|---|---|
| **E-01** — deux déplacements simultanés dupliquent le message | **MAJEUR** | ouvert |
| **E-02** — deux suppressions simultanées dupliquent dans la corbeille | **MAJEUR** | ouvert |
| **E-03** — un déplacement qui ne déplace rien annonce un succès | MOYEN | ouvert |
| E-04 — deux clients sur le même brouillon | **RETIRÉ** | comportement normal de l'endpoint |
| E-05 — création simultanée du même dossier | RAS | un seul dossier, aucun 500 |
| E-06 — suppression pendant lecture | RAS | jamais de contenu tronqué |

Les trois défauts sont dans la même famille et se corrigent probablement ensemble : **le
déplacement n'est atomique ni vis-à-vis d'un autre déplacement, ni vis-à-vis de lui-même.**

---

## Pourquoi ce plan a demandé un harnais séparé

Toute la suite mesurait jusqu'ici par recensement avant/après. **La concurrence invalide ce
motif** : quand deux opérations se chevauchent, il n'existe pas d'instant « avant » ni
« après » commun, et un recensement pris entre les deux ne décrit aucun état cohérent.

Quatre règles le remplacent, chacune réparant un piège observé pendant la campagne.

**1. La course doit être réellement simultanée.** Deux requêtes lancées à la suite dans une
boucle ne se chevauchent pas forcément : le temps de construction de la requête suffit à les
sérialiser. Les fils partent donc d'une `threading.Barrier`.

**2. Le chevauchement se prouve.** On horodate départ et arrivée de chaque requête et on
vérifie que les intervalles se recouvrent. Sans recouvrement, le test rend `Skip` — **jamais
`PASS`**.

C'est le piège central du plan, et il est pire que les autres parce qu'**il se cache derrière
un vert** : un test de concurrence qui ne fait que de la séquence passe toujours, et passera
encore le jour où le défaut apparaîtra. Une sonde ratée finit par se voir ; un faux négatif
permanent, non.

**3. L'invariant ne suppose aucun instant.** `total_avant == total_après` demande un instant
que la concurrence n'a pas. `le message existe exactement une fois` s'évalue à la fin et
attrape aussi bien la duplication que la perte. C'est l'invariant qui a trouvé A-18 sans le
chercher, porté sur une identité plutôt que sur un comptage.

**4. Un défaut de concurrence ne se manifeste pas à tous les coups.** Chaque scénario rejoue
sa course 12 fois et **annonce combien** ont réellement été simultanées. « Vert sur 12 courses
dont 12 avec chevauchement prouvé » n'est pas la même information que « vert ».

### Le harnais a dû être corrigé avant de servir

En l'éprouvant sur ses propres cas limites, un défaut est apparu : un chevauchement de
quelques microsecondes est mathématiquement positif, donc mon détecteur déclarait
« simultanées » deux requêtes qui s'étaient à peine croisées. Le critère est devenu relatif —
le chevauchement doit couvrir **au moins la moitié** de la requête la plus courte — et une
requête quasi instantanée est déclarée incapable de courir contre quoi que ce soit.

Cas de contrôle, tous vérifiés : deux opérations lentes (100 % de recouvrement, simultané),
une instantanée contre une lente (0 %, **non** simultané), 20 ms contre 600 ms (100 % du plus
court, simultané), deux intervalles disjoints (non simultané).

---

## E-01 — MAJEUR : deux déplacements simultanés dupliquent le message

Deux `POST /message/{uid}/move` sur le **même UID**, vers deux dossiers différents.

```
                       sequentiel   concurrent
le message existe...    1 fois       2 fois        12 courses sur 12
chevauchement                        210 a 296 ms
```

`move` fait `COPY` puis `STORE \Deleted` puis `EXPUNGE`, sans atomicité. Les deux `COPY`
aboutissent — une vers A, une vers B — et l'`EXPUNGE` ne retire l'original qu'une fois.

**Test laissé** : `CONC-01`.

---

## E-02 — MAJEUR : deux suppressions simultanées dupliquent dans la corbeille

```
                       sequentiel   concurrent
le message existe...    1 fois       2 fois        12 courses sur 12
chevauchement                        258 a 336 ms
```

Même mécanisme, la corbeille étant la cible. C'est la famille `F-02` et A-18 rejouée en
course.

**Test laissé** : `CONC-05`.

---

## E-03 — MOYEN : un déplacement qui ne déplace rien annonce un succès

```
POST /accounts/3/message/999999/move?folder=QAE_SRC  ->  200 {"status":"moved"}
dossier cible                                        ->  0 message
```

Aucune concurrence nécessaire pour le constater. Mais c'est lui qui rend E-01 et E-02
**invisibles côté client** : dans une course, les deux clients reçoivent « moved » alors qu'un
seul a déplacé quelque chose. Le second croit avoir agi et rien ne le détrompe.

Même famille qu'IT4-01 et A-18 — un succès annoncé sans effet réel.

Trouvé dans le témoin séquentiel, en cherchant autre chose : le second `move` répondait
`200 'moved' target QAE_B` alors que le message était resté dans A.

**Test laissé** : `CONC-04`.

### Une remarque pour le correctif

`UID MOVE` (RFC 6851) est atomique et fermerait E-01 et E-02 d'un coup. Deux réserves :

- la capacité n'est pas universelle ; **le repli doit être écrit et testé**, sinon le défaut
  reste ouvert sur le serveur qui ne l'annonce pas — exactement le genre de protection
  dépendante du serveur déjà rencontré sur le zip slip ;
- **`MOVE` atomique ne ferme pas E-03.** Un `MOVE` sur un UID inexistant réussit du point de
  vue du protocole. Il faut vérifier qu'il a bougé quelque chose.

---

## Le constat retiré, et ce qui l'a retourné

**Trois scénarios sur cinq échouaient 12 fois sur 12.** Cette régularité m'a alerté avant de
me convaincre : **une vraie course ne se déclenche presque jamais à tous les coups.** Une
reproductibilité parfaite sur un défaut censé être intermittent indique qu'on ne mesure pas ce
qu'on croit.

J'ai donc mesuré le **témoin séquentiel** — les mêmes deux opérations, l'une après l'autre —
avant de rapporter quoi que ce soit.

```
                          sequentiel   concurrent   verdict
deux deplacements          1 message    2 messages   vrai defaut de concurrence
deux suppressions          1 message    2 messages   vrai defaut de concurrence
deux enregistrements       2 brouillons 2 brouillons AUCUN defaut de concurrence
```

**Le scénario des brouillons n'en était pas un.** Deux appels séquentiels à `save-draft`
produisent aussi deux brouillons : l'endpoint n'a pas d'identifiant de brouillon, chaque appel
fait un `APPEND`. C'est une propriété de l'endpoint, pas une course.

Sans témoin, j'imputais à la concurrence un comportement qui lui préexiste — **l'erreur
exactement inverse de celle commise sur A-18**, où j'avais imputé à mon test un défaut du
produit. Même cause dans les deux sens : conclure d'une observation sans le contrôle qui
l'interprète.

Le test a été remplacé par celui d'E-03, et **l'explication du retrait est restée dans le
fichier** : un test supprimé sans trace se réécrit.

---

## Ce qui n'a rien donné

**E-05 — création simultanée du même dossier.** 12 courses simultanées prouvées. Un seul
dossier porte le nom à la fin, aucun 500 sur la course perdante. Les deux réponses peuvent
légitimement être des succès : `create_folder` traite « already exists » comme un succès
idempotent, ce qui est le bon comportement.

**E-06 — suppression pendant lecture.** 12 courses simultanées prouvées. La lecture aboutit
avec le contenu complet, ou échoue proprement. **Jamais de réussite partielle** — pas de 200
avec un corps vide ou tronqué, qui aurait fait passer un message supprimé pour un message
vide.

---

## Le signal A-20, vérifié avant d'être cherché

Le correctif A-20 introduit une réutilisation de la sélection de dossier :
`fetch_email(reutiliser_selection=True)` saute le `SELECT` si le marqueur `_selection` vaut
déjà `(dossier, True)`. C'est un « vérifier puis agir » sur un état partagé — donc une course
potentielle où un fil sauterait le `SELECT` pendant qu'un autre change de dossier sur la même
connexion, et lirait des UID dans le mauvais dossier. **Le message reviendrait normalement, il
serait simplement faux.**

Vérification : **le motif est réel mais inatteignable aujourd'hui.** `get_imap()` rend une
instance neuve à chaque appel, et les 26 instanciations côté API sont toutes des
`with IMAPManager(config)` à portée locale. Aucun pool, aucun cache, aucune globale — donc
aucun partage entre opérations concurrentes.

Surveillé quand même sur les 60 courses : aucun message dont le contenu ne corresponde à son
UID. Les sujets portent un marqueur unique par semis, donc un décalage de dossier se verrait.

**C'est une sûreté par absence de partage, que rien ne signalera à qui l'enlève.** Le jour où
quelqu'un introduit un pool de connexions — optimisation banale, sans rapport apparent avec
A-20 — `_selection` devient une course. Même forme que le zip slip protégé par la
bibliothèque standard.

---

## Le tableau qui dit ce que ces tests valent

| Observé dans l'état qu'il surveille | Cru sur parole |
|---|---|
| `CONC-01` — rouge sur le défaut réel | `CONC-02` |
| `CONC-04` — rouge sur le défaut réel | `CONC-03` |
| `CONC-05` — rouge sur le défaut réel | |

Trois des cinq tests de ce groupe ont démontré qu'ils détectent le défaut qu'ils gardent.

Sur l'ensemble de la suite, cela porte à **six sur quatorze** les tests de sécurité et de
concurrence observés dans l'état qu'ils surveillent — `SEC-G01`, `SEC-G02`, `SEC-G07`,
`CONC-01`, `CONC-04`, `CONC-05`.

Les deux verts de ce groupe (`CONC-02`, `CONC-03`) sont crus sur parole. Ils annoncent
néanmoins leur nombre de courses simultanées prouvées, ce qui dit au moins qu'ils ont
réellement mesuré quelque chose.

---

## État de l'environnement

- **Périmètre respecté** : GreenMail et compte 3 uniquement. `fixer` garde Dovecot, ses boîtes
  `qa-volume` (compte 60) et `qa-fixer`, jamais touchées.
- **Compte professionnel** : jamais sollicité.
- Utilisateur 7 et compte mail 61 conservés, hors périmètre de ce plan, `sync_enabled: false`.
- Dossiers de travail des courses laissés en place — la suite les réutilise et les vide à
  chaque exécution.

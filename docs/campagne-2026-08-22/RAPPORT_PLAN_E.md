# Plan E — Concurrence

**Date** : 2026-08-22
**Périmètre** : GreenMail, compte 3. Dovecot laissé à `fixer`, aucune ressource commune.
**Livrable associé** : `tests/qa/course.py` (harnais) et `tests/qa/suites/concurrence.py` (5 tests).

---

## Résumé

| Défaut | Gravité | État |
|---|---|---|
| **E-01** — deux déplacements simultanés dupliquent le message | **MAJEUR** | corrigé, vérifié |
| **E-02** — deux suppressions simultanées dupliquent dans la corbeille | **MAJEUR** | corrigé, vérifié |
| **E-03** — un déplacement qui ne déplace rien annonce un succès | MOYEN | corrigé, vérifié |
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

## Après correctif : la course resserrée, et ce que ma mesure ne prouve pas

`fixer` a corrigé E-01, E-02 et E-03, puis signalé une réserve qui valait plus que son
résultat : sa mesure **au niveau protocole** montre que `UID MOVE` reste dupliquant sur
Dovecot (6/6), et que le 0/6 obtenu par l'API vient peut-être du coût d'établissement des
connexions, qui désaligne les requêtes avant la section critique. **Une protection par le
calendrier, pas par construction.**

J'ai donc resserré la course autant que possible depuis l'extérieur : 6 clients au lieu de 2,
connexions HTTPS **pré-établies et déjà utilisées** (handshake et démarrage hors course),
`keep-alive`, barrière de synchronisation, 10 courses par serveur.

| Serveur | chevauchement HTTP | duplications | codes |
|---|---|---|---|
| GreenMail (compte 3) | 203 à 1040 ms | **0 / 10** | 1 × `200`, 5 × `404` |
| Dovecot (compte 30) | 45 à 60 ms | **0 / 10** | 1 × `200`, 5 × `404` |

Le motif de réponses est exactement celui attendu : un gagnant, cinq clients informés que le
message n'est plus là. Le `MessageGone` → `404` de `fixer` tient à six clients.

### Ce que cette mesure ne dit pas

**Le chevauchement affiché est celui des requêtes HTTP, pas celui des commandes IMAP.** Chaque
requête ouvre sa propre connexion IMAP, s'authentifie et fait un `SELECT` avant d'atteindre le
`MOVE`. Ce préambule coûte à lui seul de l'ordre de la durée du chevauchement obtenu.

Je n'ai donc **pas réfuté** l'hypothèse de `fixer` : je n'arrive pas à atteindre la fenêtre
résiduelle depuis l'extérieur, ce qui n'est pas la même chose que montrer qu'elle est fermée.
Le désalignement s'est peut-être simplement déplacé du HTTP vers l'IMAP.

Sa mesure au niveau protocole reste la plus informative des deux.

### Ce qu'elle établit, après quatre modèles successifs dont trois faux

La question « la course est-elle fermée ? » a reçu quatre réponses successives, en trois
heures, entre `fixer` et moi. Les trois premières étaient fausses — et **toutes cohérentes,
appuyées sur de vraies mesures**, ce qui est exactement ce qui les rendait crédibles.

| Modèle | Fenêtre supposée | Ce qui l'a démoli |
|---|---|---|
| 1 — le `MOVE` n'est pas atomique | durée du `MOVE` | `UID MOVE` atomique duplique quand même |
| 2 — intervalle `SELECT` → `MOVE` | ~6 ms | une session gardant son instantané 10 s ne duplique pas |
| 3 — mes requêtes sont trop espacées | facteur 30 puis 10 | mes envois sont à 0,06–0,24 ms, **plus serrés** que la fenêtre |
| 4 — variance du préambule | < 1 ms, marge ×2–3 | tient |

**Le modèle retenu**, mesuré par `fixer` avec la barrière placée avant la connexion — une
mesure impossible depuis l'API :

```
clients   preambule median   ecart-type   ecart MIN entre MOVE voisins
     6            12 ms         5,0 ms                2,06 - 2,43 ms
    12            20 ms         9,0 ms                1,86 - 2,01 ms
    24            35 ms        18,1 ms                0,93 - 1,70 ms
    48            65 ms        35,1 ms                1,00 - 1,28 ms
```

Fenêtre : duplication systématique sous 0,5 ms, jamais au-delà de 1 ms.

**La marge réelle est un facteur 2 à 3.** Pas trente, pas dix. À 24 clients l'écart minimal
touche 0,93 ms — le bord exact de la fenêtre, frôlée sans être franchie.

Et la protection est **auto-régulée** : plus il y a de clients, plus le préambule ralentit, ce
qui ré-étale les requêtes. L'écart minimal ne s'effondre pas sous la charge, il plafonne vers
la milliseconde. C'est pourquoi ni `fixer` ni moi n'avons jamais vu de duplication par l'API
sur plus d'une centaine de courses.

> **Ce n'est pas une marge de sécurité, c'est un équilibre — et personne ne l'a conçu.**

### Ce que cela change pour la décision

La formulation « la fenêtre n'est pas atteignable » était trop rassurante. La bonne est :
**la fenêtre est maintenue hors de portée par un mécanisme accidentel, avec un facteur 2 à 3
de marge.**

Deux conséquences pour l'arbitrage du verrou, qui appartient à l'utilisateur :

- **un pool de connexions supprime la variance** et amène la fenêtre à portée. C'est la même
  cause que le risque `_selection` d'A-20, et `CONC-06` garde les deux ;
- **le chemin worker n'exécute pas ce préambule à chaque opération**, donc il ne bénéficie
  pas de cet équilibre. Non mesurable tant que le worker est arrêté — signalé comme **non
  couvert**, pas comme fermé.

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

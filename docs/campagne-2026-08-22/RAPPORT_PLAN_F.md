# Plan F — Volume, limites, et la mesure qui débloque le redémarrage du worker

**Date** : 2026-08-22
**Périmètre** : boîte Dovecot `qa-volume` créée pour l'occasion, compte MailIA 60.
Aucun accès au compte 1, au compte 30 ni à GreenMail.

---

## La réponse à la question posée

> *Les limites actuelles (1500 s / 1800 s) tiennent-elles pour le compte réel ?*

**Non. Le premier cycle de synchronisation dépasse la limite douce de 5 secondes.**

```
2 allers-retours IMAP par email   (mesuré, exact)
× 32,5 ms de latence vers OVH     (mesuré, médiane sur 7 poignées TCP)
= 65 ms par email de latence pure

Premier cycle ≈ 22 993 emails  →  1 494 s  =  24 min 55 s
Limite douce actuelle           →  1 500 s  =  25 min 00 s
                                    ────────
                          marge :        5 secondes
```

**Mais la bonne réponse n'est pas de relever la limite.** L'un des deux allers-retours par
email est inutile : `fetch_email` émet un `EXAMINE` avant *chaque* `UID FETCH`, alors que le
dossier ne change pas d'un email au suivant. Le supprimer **divise la durée par deux** et
transforme une marge de 5 secondes en marge de 12 minutes, sans toucher aux limites.

| | latence pure, 45 000 emails | premier cycle |
|---|---|---|
| **aujourd'hui** | 48,8 min | **24 min 55 s** — au-dessus de la limite douce |
| **avec le `SELECT` mémorisé** | 24,5 min | **12 min 28 s** — la moitié de la limite |

---

# 1. Ce qui a été mesuré, et comment

## Le banc

- **9 974 messages, 30 dossiers, 55,7 Mo**, écrits directement en Maildir dans le volume
  Dovecot — 0,4 s, contre plusieurs heures en `APPEND` IMAP.
- Contenu volontairement varié : corps courts et longs (×40), **8 % avec pièce jointe**
  (~50 Ko en base64), sujets accentués, dates étalées sur 3 ans, hiérarchie `Archives/2021..2024`.
- Le passdb Dovecot est statique : n'importe quel identifiant avec `testpass123` ouvre une
  boîte à lui. **Aucun compte n'a été créé côté serveur mail**, et le compte 30 du testeur n'a
  pas été approché.

## La synchronisation, chronométrée sans limite de temps

Appel direct de `_sync_account_by_id(60)` — **ni Celery, ni worker démarré**, donc aucune
limite ne s'applique. C'était la condition posée par le testeur.

```
TOTAL : 15,5 s pour 7 768 emails  →  2,0 ms/email (serveur local)

phase                  cumul (s)   appels     moyenne    part
0_ensure_index              0,00        1     1,91 ms    0,0%
1_connexion_imap            0,01        1    11,18 ms    0,1%
2_list_folders              0,00        1     1,05 ms    0,0%
3_get_uids                  0,02       31     0,67 ms    0,1%
4_fetch_email              14,42     7768     1,86 ms   93,2%   <<<
5_indexation_es             0,76      180     4,22 ms    4,9%
non instrumenté             0,26                         1,7%
```

**93 % du temps est dans `fetch_email`.** Tout le reste — listage, curseurs, indexation
Elasticsearch — est du bruit à cette échelle.

## Pourquoi les millisecondes locales ne s'extrapolent pas, et ce qui s'extrapole

Sur un serveur local le coût est celui du CPU ; sur un serveur distant c'est la **latence**,
donc le **nombre d'allers-retours**. C'est cette métrique que j'ai mesurée, et elle est exacte
parce qu'elle est comptée, pas estimée :

```
100 emails récupérés → 201 commandes IMAP
  EXAMINE  100   (1,00 par email)
  UID      100   (1,00 par email)
→ 2,0 allers-retours par email
```

Et la latence réelle, mesurée depuis le serveur applicatif :

```
pro1.mail.ovh.net:993 — min 26,2 ms | médiane 32,5 ms | max 54,7 ms (n=7)
```

> **Divulgation.** Cette mesure est une **poignée TCP seule** : pas de `LOGIN`, aucune commande
> IMAP, aucune donnée de boîte lue. Je l'ai faite parce qu'elle transforme une fourchette en
> chiffre, et je la signale parce qu'elle contacte tout de même le fournisseur de l'utilisateur.

---

# 2. Trois défauts trouvés par le volume — dont un bloquant

Aucun n'était visible à petite échelle. C'est l'argument du Plan F.

## A-19 — un sujet 8-bit bloque définitivement la synchro de son dossier ⛔

**Le plus grave, et il s'est déclenché à la première exécution.**

`fetch_email` renvoie `subject=msg.get("Subject", "")`. Quand un expéditeur met des octets
8-bit bruts dans l'en-tête au lieu d'un encoded-word RFC 2047, Python ne renvoie pas une
chaîne mais un objet **`email.header.Header`**. Cet objet voyage jusqu'à Elasticsearch, dont
le sérialiseur le refuse — et comme le lot échoue, **le curseur du dossier ne repart jamais**.

Mesuré : la synchro s'est arrêtée à **1 450 emails sur 9 768**. Chaque cycle suivant rejoue le
même lot et échoue identiquement. Le dossier est mort.

### Le message d'erreur est un leurre, et c'est ce qui rend le défaut coûteux

```
Index error for UID 14 in INBOX: `np.float_` was removed in the NumPy 2.0 release.
```

NumPy n'a rien à y voir : le sérialiseur d'`elasticsearch-py` teste les types NumPy dans son
chemin de repli, et ce test plante sur NumPy 2.5 avant même d'expliquer quel objet il ne sait
pas sérialiser. **Un développeur qui suit ce message part sur une mise à jour de NumPy et ne
trouve rien.** Hypothèse confirmée par mesure directe :

| Sujet passé à `index_email` | Résultat |
|---|---|
| objet `Header` brut | `AttributeError: np.float_ was removed…` |
| le même après `str()` | **OK** |

### Correctif — au point de passage, avec décodage réel

`_decode_header()` ajouté dans `manager.py`, appliqué au sujet ; `str()` sur `From`/`To`, qui
peuvent revenir sous la même forme. La technique existait déjà **dans `accounts.py`** et
n'avait pas été appliquée ici — encore la même famille.

| Forme du sujet | Avant | Après |
|---|---|---|
| 8-bit brut | `Header` → **blocage** | `'Résumé du devis'` |
| encoded-word RFC 2047 | `'=?utf-8?B?…'` non décodé | `'Résumé du devis'` |
| ASCII | inchangé | inchangé |

> **Vérification faite avant de livrer** : ce correctif décode désormais les encoded-words, ce
> qui est exactement « la mine » que j'avais signalée pour `get_thread`. J'ai vérifié qu'aucun
> sujet n'atteint une commande IMAP — les seuls consommateurs sont Elasticsearch, les réponses
> JSON et les invites de l'IA. **La surface d'injection n'est pas rouverte.**

**Écrit, compilé, NON déployé** (gel en vigueur). La mesure ci-dessus a été obtenue avec le
correctif appliqué en mémoire dans le banc.

## A-20 — un `EXAMINE` par email : la moitié du temps de synchro ⚡

`fetch_email` re-sélectionne le dossier avant chaque `UID FETCH`, alors que la boucle de
synchronisation enchaîne des centaines d'emails **du même dossier**.

```
actuel                    400 commandes pour 200 emails = 2,00 / email
avec le SELECT mémorisé   201 commandes pour 200 emails = 1,00 / email
                          → réduction de 50 %
```

À 32,5 ms et 45 000 emails : **48,8 min → 24,5 min**, soit **24 minutes économisées par
rattrapage complet**.

Le `SELECT` n'est pas gratuit à supprimer : il faut mémoriser le dossier courant *et*
l'invalider dès qu'une autre commande change de dossier, sinon un `FETCH` s'exécute sur la
mauvaise boîte. C'est un correctif de quelques lignes mais qui touche le cœur du chemin de
lecture — je ne l'ai **pas écrit**, il mérite sa propre fenêtre et sa propre vérification.

## A-21 — `delete_emails_bulk` fait 25× plus d'allers-retours que `move_emails_bulk`

Deux fonctions voisines, le même travail, deux stratégies :

| Opération sur 200 messages | Allers-retours | Par message | À 32,5 ms, pour 1 000 |
|---|---|---|---|
| `move_emails_bulk` | 9 | **0,04** | 1,5 s |
| `delete_emails_bulk` (avec corbeille) | 204 | **1,02** | **33 s** |

`move_emails_bulk` regroupe les UID en jeux (`1:50,51:100`) ; `delete_emails_bulk` émet un
`COPY` par message. Or supprimer *est* un déplacement vers la corbeille — le code efficace
existe vingt lignes plus haut.

Non corrigé : c'est une optimisation, pas un défaut de correction, et le gel est en vigueur.

## A-22 — les dossiers `\Noselect` ne sont pas ignorés (mineur)

`Archives`, nœud parent de `Archives/2021..2024`, est annoncé `\Noselect` par Dovecot. La
synchro tente quand même de le sélectionner, échoue, et journalise une erreur par cycle.
Sans gravité, mais `list_folders()` expose déjà le drapeau : un filtre d'une ligne suffirait.
Sur 180 dossiers réels, il y a vraisemblablement plusieurs nœuds de ce type.

---

# 3. La recommandation chiffrée

## L'arithmétique du premier cycle

`MAX_PER_FOLDER = 2000` plafonne chaque dossier par cycle. Avec l'arborescence réelle
(45 000 emails, `Trash` 18 992, `INBOX` 7 015, `Éléments supprimés` 1 568) :

```
Trash              → 2 000 (plafonné)
INBOX              → 2 000 (plafonné)
Éléments supprimés → 1 568
~176 autres        → ~17 425   (moyenne ~99, aucun plafonné)
                     ───────
premier cycle      ≈ 22 993 emails
```

`Trash` étant plafonné à 2 000, le **rattrapage complet demande une dizaine de cycles**, quelle
que soit la limite de temps. C'est un choix de conception, pas un défaut — mais il faut le
savoir : redémarrer le worker ne rattrape pas trois mois en une fois.

## Recommandation

**1. Corriger A-20 avant de toucher aux limites.** C'est la différence entre « au-dessus de la
limite » et « à la moitié ». Relever une limite pour accommoder un aller-retour inutile serait
payer deux fois.

**2. Puis fixer les limites ainsi :**

| Réglage | Actuel | Proposé | Justification |
|---|---|---|---|
| `task_soft_time_limit` | 1 500 s | **2 700 s** (45 min) | 3,6× le pire premier cycle après A-20 (12,5 min) |
| `task_time_limit` | 1 800 s | **3 000 s** (50 min) | 300 s pour dérouler `SoftTimeLimitExceeded` proprement |
| `SYNC_LOCK_TTL` | 2 100 s | **3 300 s** | **doit** rester au-dessus de la limite dure, sinon un verrou survit à la tâche tuée |

**Sans corriger A-20**, il faudrait au minimum 3 600 s en limite douce — ce qui signifie qu'une
synchronisation bloquée immobilise un emplacement de worker pendant une heure. C'est
l'argument le plus fort pour A-20 : les limites protègent d'autant mieux qu'elles sont basses,
et elles ne peuvent être basses que si le travail est efficace.

## D'où vient le facteur 3,6

Ce n'est pas de la prudence en vrac :

| Source d'écart | Facteur | Mesure |
|---|---|---|
| Pic de latence | ×1,7 | max 54,7 ms contre 32,5 de médiane |
| Messages réels plus lourds que le banc | ×1,5 | estimation, mon banc fait 5,9 Ko de moyenne |
| Traitement serveur du `FETCH` | ×1,2 | non mesuré, majoration prudente |
| **Composé** | **×3,1** | |

3,6 laisse un peu au-delà. **Je ne prétends pas que 2 700 s soit optimal — je prétends que
c'est la première valeur de ce projet qui repose sur une mesure plutôt que sur un pari.**

## Ce que la mesure explique aussi

Le beat déclenche toutes les **5 minutes**, un cycle dure **25 minutes**. Quatre déclenchements
sur cinq ne peuvent donc pas s'exécuter. Le `expires: 240` ajouté pendant la campagne les
périme correctement — **c'est exactement le mécanisme dont l'absence a produit les 26 940
tâches accumulées.** La cadence réelle de synchronisation est celle du cycle, pas celle du beat.

---

# 4. Le reste du Plan F

## Pagination et filtres : rien à signaler

Sur `INBOX` à 4 000 messages, via l'API :

| Mesure | Résultat |
|---|---|
| page 0 / 5 / 20 / 50 (taille 50) | 0,150 s / 0,084 / 0,084 / **0,083 s** |
| taille 50 / 200 / 500 | 0,079 / 0,099 / 0,126 s |
| `filter_from`, `filter_subject` | 0,138 s / 0,070 s |
| `filter_subject=Été` (accentué) | **0,072 s — 200** |

**La pagination ne se dégrade pas avec la profondeur** : la page 50 coûte autant que la page 5.
Et la recherche accentuée répond 200 : le correctif `_uid_search` déployé ce matin tient à
l'échelle.

## Opérations en masse

Voir A-21. `move_emails_bulk` est efficace, `delete_emails_bulk` ne l'est pas.

## Non traité

Import mbox volumineux, pièce jointe de 25 Mo, recherche à plus de 10 000 résultats,
arborescence à 5 niveaux. La recommandation sur les limites de temps était le livrable ; ces
mesures viendront si une fenêtre s'ouvre.

---

# 5. État et sécurité du périmètre

**Un point d'attention que j'ai corrigé en cours de route** : le compte 60 que j'ai créé était
`sync_enabled = true` par défaut. Si le worker avait démarré, il aurait été synchronisé — sans
risque pour l'utilisateur, mais c'est exactement le genre d'inattention qui, sur un autre
compte, en aurait créé un. Passé à `false` dès que je l'ai vu. **Seul le compte 1 reste
`sync_enabled`, comme avant mon intervention.**

| Élément | État |
|---|---|
| Worker, beat | **`Exited`** — jamais démarrés, appel direct de la fonction |
| File Celery | 0 · sauvegarde 26 940 intacte |
| Compte 1 | **1227 emails locaux, inchangé** — aucun accès, hors la poignée TCP signalée |
| Comptes 3 et 30 | non touchés |
| Déploiements | **aucun** — A-19 est écrit et compilé, pas déployé |

## En attente d'une fenêtre

| Réf | Objet | État |
|---|---|---|
| **A-19** | sujet 8-bit qui bloque un dossier | écrit, compilé, **non déployé** |
| **A-20** | `SELECT` mémorisé — moitié du temps de synchro | **non écrit**, mérite sa propre vérification |
| **A-21** | `delete_emails_bulk` en jeux d'UID | non écrit |
| **A-22** | ignorer les dossiers `\Noselect` | non écrit |
| Limites | 2 700 / 3 000 / 3 300 | à décider par l'utilisateur |

**Ordre recommandé** : A-19 d'abord — c'est un blocage, pas une lenteur, et il se déclenche sur
du courrier ordinaire. A-20 ensuite, seul, avec sa propre mesure avant/après.

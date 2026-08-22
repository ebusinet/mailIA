# Revue critique de la suite de non-régression

**Date** : 2026-08-22
**Objet** : `/var/www/mailia/tests/` — relecture **sans modification**
**État relu** : `guard.py` (04:11), `run.py` (04:22), `core.py` (04:39), suites (04:15 → 04:30)

⚠️ **Le répertoire évoluait pendant ma revue.** `core.py` a été réécrit à 04:39 — un mécanisme de
marqueurs uniques y est apparu, qui corrige précisément le défaut que j'avais mesuré une heure plus
tôt. Deux constats que j'avais relevés (ISO-07, et une partie de l'instabilité) sont donc déjà
traités. Les mesures ci-dessous sont horodatées ; le testeur saura celles qui portent encore.

---

# 1. Le garde-fou — verdict tranché

**Il est solide.** Conception juste, échec fermé, testé par lui-même. Je n'ai trouvé aucun scénario
réaliste où la suite s'exécuterait sur un compte @ebusinet.fr.

Ce qui est bien fait, et qui mérite d'être dit :

- **La vérification porte sur TOUS les comptes visibles, pas seulement la cible.** Un jeton qui *voit*
  un compte de production fait tout échouer même si les tests ne le visent pas. C'est le bon
  invariant, et c'est plus fort que ce que j'attendais.
- **Double barrière volontaire** : liste blanche d'identités *et* motifs interdits (`ebusinet`,
  `ovh.net`, `pimienta`). Si quelqu'un élargit la liste blanche par confort, le second filtre tient.
- **Le garde-fou est testé** (9 tests `GUARD-*`), y compris le **cas nominal** — un garde-fou qui
  refuse tout serait tout aussi inutilisable. Cette symétrie est rarement écrite.
- **`--no-guard` refuse de s'exécuter si un jeton est présent** et restreint aux tests hors ligne.
  L'échappatoire évidente est fermée.

## Vérification empirique — 6 scénarios, 6 refus

| Scénario | Résultat | Code de sortie |
|---|---|---|
| Aucun jeton | `REFUS : aucun jeton fourni` | **2** |
| Jeton invalide | `REFUS : jeton refuse par l'API (401)` | **2** |
| `--no-guard` avec un jeton présent | `REFUS : --no-guard est interdit quand un jeton est present` | **2** |
| API injoignable | `REFUS` avant tout test | **2** |
| `MAILIA_QA_ACCOUNT_ID=""` / `MAILIA_TIMEOUT=""` | `ValueError` à l'import, avant le premier test | non nul |
| Identité du domaine réel (`contact@ebusinet.fr`, `E.Pimienta@Ebusinet.FR`, `quelquun@pro.ovh.net`) | refusée par motif interdit | — |

Le message d'échec dit explicitement « *Aucun test n'a ete execute, aucune donnee n'a ete touchee* ».
C'est le bon mode de défaillance.

## Deux trous réels — non exploitables aujourd'hui, mais par chance de configuration

Ce sont exactement deux des cinq scénarios que tu m'as demandé d'attaquer. Dans les deux cas, ce qui
protège aujourd'hui est l'état de l'instance, pas le code.

### G-01 — MAJEUR : `QA_USER_ID` n'est jamais confronté à l'identité authentifiée

`guard.py:37` fixe `QA_USER_ID = 5`. `core.py:221-228` s'en sert pour **forcer** l'utilisateur des
appels MCP :

```python
S.USER_ID = 5
assert S.USER_ID == 5, 'garde-fou MCP'      # tautologie : vérifie sa propre affectation
```

`run_guard()` lit bien `me.get("id")` (`guard.py:136`) — **mais uniquement pour l'afficher.** Aucune
comparaison avec `QA_USER_ID` n'existe.

Conséquence : le garde-fou valide l'identité par **l'adresse email**, alors que les tests MCP
s'exécutent sous un **identifiant numérique codé en dur**. Si les deux se désynchronisent, le
garde-fou dit « OK » et tous les tests MCP — dont ceux qui déplacent et suppriment des emails —
tournent sous l'identité de l'utilisateur 5, quel qu'il soit.

C'est précisément ton scénario « l'utilisateur QA a été supprimé et son id réattribué ». C'est aussi
ce qui arriverait en pointant la suite sur une **autre instance** MailIA où `qa@mailia.local`
porterait un autre id — et où l'id 5 appartiendrait à quelqu'un de réel.

*Correctif* : une ligne dans `run_guard()`, après `check_identity` —
`if me.get("id") != QA_USER_ID: raise GuardError(...)`. Le contrôle devient alors cohérent de bout
en bout : même identité vérifiée côté API et côté MCP.

### G-02 — MOYEN : `smtp_host` échappe à la liste blanche des hôtes

`check_accounts` (`guard.py:94-101`) compare **`imap_host`** à `ALLOWED_MAIL_HOSTS`. `smtp_host`
n'est soumis qu'aux motifs interdits. Vérifié en exécutant le garde-fou :

```
imap_host=greenmail, smtp_host=smtp.gmail.com   -> ACCEPTE
imap_host=imap.free.fr                          -> refusé
```

Un compte dont l'IMAP pointe sur GreenMail mais le SMTP sur un serveur externe passe. Or la suite
**envoie réellement des emails** (`SMTP-*`, `action_forward`, `mcp_envois`) : elle enverrait alors du
courrier par un relais tiers, depuis une adresse réelle, à chaque exécution.

C'est ton scénario « le compte 3 a été reconfiguré » — à moitié couvert : l'IMAP est gardé, le SMTP
ne l'est pas.

*Correctif* : appliquer le même test de liste blanche à `smtp_host` (en tolérant la valeur vide,
un compte sans SMTP étant légitime).

### G-03 — MINEUR : `MAILIA_API_URL=""` produit une trace au lieu d'un refus

Une variable **définie mais vide** court-circuite le défaut de `os.environ.get(..., défaut)`.
`api_url` vaut `""`, et l'appel échoue sur `ValueError: unknown url type: '/auth/me'` — traceback
brut, **code de sortie 1**, celui qui signifie « des tests ont échoué », et non 2 « le garde-fou a
refusé ». En intégration continue, la distinction compte.

Rien ne s'exécute : c'est fermé, mais mal signalé.

## Contournement par lancement isolé — pas de brèche, une gêne de nommage

Tu demandais si un test lancé hors du point d'entrée peut sauter le garde-fou.

- **`pytest` par défaut ne collecte rien** : il ne ramasse que `test_*.py` / `*_test.py`, or aucun
  fichier de suite ne porte ce nom. Vérifié.
- **Importer une suite n'exécute aucun test** : le décorateur `@test` se contente d'enregistrer, et
  les seules instructions de niveau module sont des affectations de constantes (`SRC = CFG.folder_src`).
  Vérifié par AST sur les 8 suites : aucun effet de bord à l'import. C'est important, car `run.py`
  importe les suites **avant** d'exécuter le garde-fou (ligne 34, puis `main()`).
- **Réserve** : deux fonctions s'appellent `test_credentials_smtp` et `test_smtp_compte`
  (`smtp_paths.py:27,40`). Elles échappent à la collecte par défaut, mais un
  `pytest tests/qa/suites/smtp_paths.py` explicite, ou un `python_files` élargi dans une future
  configuration, les exécuterait **sans garde-fou**. Les renommer (`credentials_smtp`,
  `smtp_du_compte`) coûte deux lignes et supprime le piège.

Reste le contournement délibéré (`python3 -c "import qa.suites.x; x.f()"`). Aucun garde-fou ne
protège de quelqu'un qui l'évite volontairement ; ce n'est pas un défaut.

## Worker, beat, file Celery : rien à signaler

- Aucun `docker start|run|compose up`, aucun `celery` dans la suite. Les seuls appels conteneur sont
  `docker exec … python3` (lecture/exécution), sur un conteneur nommé par configuration.
- `ISO-07` est le seul test touchant à la synchronisation. Il est **bien conçu** : il vérifie le refus
  du compte étranger **et** que la longueur de la file Celery n'a pas bougé.
- Aucune référence à `celery_backup_20260822` : la sauvegarde ne peut pas être touchée.
- `FOREIGN = 99999` — un compte inexistant, jamais le compte 1. Bon choix : les tests de cloisonnement
  ne dépendent pas de l'existence du compte réel.

*Réserve sur ISO-07* : si la longueur de file est illisible, `_longueur_file()` renvoie `None` et
l'assertion est simplement sautée — le test rend PASS en ayant vérifié moins. Un `Skip` serait plus
honnête que ce PASS partiel.

---

# 2. Fiabilité — mesurée, pas supposée

J'ai exécuté la suite **quatre fois** contre l'environnement réel. Le déterminisme est le critère qui
décide si une suite est utilisée ou abandonnée, et il se mesure en la rejouant.

| Exécution | État du code | Résultat |
|---|---|---|
| 1 | avant 04:39 | 63 PASS / **9 FAIL** / 1 SKIP |
| 2 | avant 04:39 | 65 PASS / **7 FAIL** / 1 SKIP |
| 3 | après la réécriture de `core.py` | 66 PASS / **6 FAIL** / 1 SKIP |
| 4 | idem | 68 PASS / **4 FAIL** / 1 SKIP |

**Entre les exécutions 1 et 2, seules 4 défaillances sur 9 étaient communes.** Cinq disparaissaient,
trois apparaissaient. Entre 3 et 4, trois défaillances sur six ont encore changé. La suite n'est pas
encore reproductible.

## R-01 — RÉSOLU depuis : l'état partagé entre suites

Diagnostic d'origine : `seed()` attendait 8 s la fin du job d'import puis comptait **sans exiger que
le job soit terminé**. Les six suites partageant les mêmes dossiers `QA_AUTOTEST_SRC`/`_DST`, un job
en retard déversait ses messages dans le dossier du test suivant. Signature caractéristique :
`5 message(s) sur 2 attendus` — **plus** que demandé.

La réécriture de `core.py` (04:39) traite exactement cela : marqueur unique `[QA-xxxxxxxx]` injecté
dans l'objet, comptage restreint au marqueur, délai porté à 30 s, échec explicite si le job ne finit
pas, et attente active jusqu'à ce que le compte corresponde. C'est la bonne correction : les tests
deviennent indépendants du contenu préexistant plutôt que de tenter de le nettoyer.

Le gain est réel (9 → 4 défaillances) mais l'instabilité **n'a pas disparu** : voir R-02 et R-03.

## R-02 — Des erreurs de code de la suite remontées comme des régressions produit

C'est le défaut le plus dommageable, parce qu'il produit un **faux signal de régression**.

```
STO-02 — Deplacement IMAP vers local et retour [F-07/F-08]
      NameError: name 'mk' is not defined
DUP-06 — Les outils MCP ne dupliquent pas [F-02]
      IndexError: list index out of range
```

`rules_and_storage.py:240` utilise `mk` sans l'avoir lié (vérifié par AST) — reliquat du passage aux
marqueurs. `run.py` attrape toute exception et affiche `FAIL … — bug d'origine : F-07/F-08`. Un
lecteur conclut que **F-07 est revenu**. Il ne l'est pas : c'est la suite qui est cassée.

Deux améliorations distinctes :

1. Corriger les `mk` non liés (au moins `rules_and_storage.py:240`).
2. **Distinguer « le test a échoué » de « le test n'a pas pu s'exécuter ».** Un `NameError` ou un
   `IndexError` dans le code du test n'est pas une régression du produit et ne devrait pas s'afficher
   sous la référence d'un bug. Un statut `ERROR` distinct de `FAIL` suffirait.

**10 emplacements** font `messages_in(...)[0]` sans garde : dès que la préparation livre moins que
prévu, on obtient un `IndexError` opaque au lieu du message explicite que `seed()` sait produire.

## R-03 — Instabilité résiduelle sur RULE-02

`RULE-02` échoue aux deux dernières exécutions, mais **différemment** :

```
run 3 : starts_with 2→0, not_contains 1→0, age older_than 1an 1→0, age newer_than 1sem 2→0
run 4 : starts_with 2→0
```

Quatre opérateurs faux puis un seul, sur des données censées être identiques. Ce n'est pas un
comportement produit : c'est la préparation qui ne livre pas toujours le même jeu. `starts_with`
échoue dans les deux cas et mérite un examen séparé — c'est le seul candidat à un vrai défaut.

## R-04 — Ce qui est déjà bien fait, et qu'il ne faut pas perdre

- **Les SKIP ne mentent pas.** `Skip` est une exception dédiée, affichée avec son motif, et le rapport
  rappelle « *Les SKIP ne sont pas des succes* ». Le test MCP indisponible rend SKIP, jamais PASS.
- **Les tests vérifient l'effet réel, pas le code HTTP.** C'est le piège que tu redoutais et il est
  évité : un seul helper (`_regle()`, `rules_and_storage.py:21`) se limite à `expect_status`, et c'est
  une fabrique d'objet, pas un test. Les tests de duplication comparent des comptages
  avant/après **dans les deux dossiers** ; `SMTP-04` vérifie que le drapeau `answered` a changé ;
  `DUP-02` va jusqu'à chercher l'email dans tous les dossiers.
- **Aucune dépendance à des UID fixes** : les UID sont toujours relus depuis l'API.
- **Pas de dérive d'arborescence** : les dossiers de travail sont fixes et vidés, jamais recréés — un
  choix explicitement commenté, et justifié puisque GreenMail refuse certains `DELETE`.
- **Bibliothèque standard uniquement**, avec la bonne justification : « une suite qu'il faut installer
  est une suite qu'on ne relance pas ».

---

# 3. Un vrai bug produit, que la suite a trouvé — et que j'ai confirmé

`DUP-02` échoue aux **quatre** exécutions. Je l'ai reproduit à la main, hors de la suite :

```
uid 82, objet « QA-SMTP09 QA82905b4a », INBOX = 48 messages
POST /accounts/3/message/82/move?folder=INBOX   {"target_folder": ""}
  -> HTTP 200  {"status":"moved","target_folder":""}
INBOX = 47
recherche de l'objet sur 12 dossiers -> retrouvé uniquement dans Sent (l'autre exemplaire)
```

**L'email a disparu, et l'API a répondu « moved ».** `MoveRequest.target_folder` n'est validée nulle
part : `move_email()` fait `create('""')` puis `COPY uid '""'`, GreenMail répond `OK`, et le
`STORE \Deleted` + `EXPUNGE` supprime l'original. Ma vérification du statut du `COPY` (lot 1) ne se
déclenche pas, puisque le serveur dit que la copie a réussi.

C'est une perte de données silencieuse, dans un chemin que j'ai modifié sans la voir. Correctif
naturel : refuser en 422 un `target_folder` vide ou composé d'espaces, dans `move_message` et dans
l'outil MCP équivalent. **Je ne l'ai pas appliqué** — ce lot était une revue, pas une correction ;
dis-moi si tu veux que je le fasse.

# 4. Un faux positif à ne pas transmettre au testeur tel quel

`MAIL-04` (« le filtre de date porte sur la date affichée », N-01) a échoué à l'exécution 2 :

```
filter_date=2024-02-10 ne renvoie pas l'email 362 qui affiche pourtant cette date
```

J'ai vérifié N-01 indépendamment, sur les **47 messages** d'INBOX et leurs **9 jours distincts** :

| jour | affichés | `filter_date` | |
|---|---:|---:|---|
| 2023-04-05 | 1 | 1 | ok |
| 2024-03-02 | 1 | 1 | ok |
| 2024-06-06 | 5 | 5 | ok |
| 2026-08-14 → 08-22 | 40 | 40 | ok |

Aucune divergence. L'échec venait du dossier pollué (4 messages au lieu de 3, cf. R-01) : un message
étranger avec une autre date. **N-01 tient.** C'est l'illustration du coût de R-01 : une suite
instable ne se contente pas d'être ignorée, elle accuse à tort du code correct.

---

# 5. Synthèse

**Le garde-fou : oui, il est solide.** Je le considère apte à sa mission. Les deux trous (G-01, G-02)
ne sont pas des réserves de principe : ce sont deux des scénarios que tu as nommés, et dans les deux
cas seule la configuration actuelle protège, pas le code. Ils se ferment en quelques lignes.

**La fiabilité : pas encore.** La correction par marqueurs de 04:39 est la bonne, et elle a divisé les
défaillances par deux. Il reste des erreurs de code dans la suite (`mk` non lié, indexations non
gardées) qui remontent comme des régressions produit — le plus mauvais signal possible pour une
suite censée être relancée par l'utilisateur.

**Par priorité, pour le testeur** :

1. **G-01** — comparer `me["id"]` à `QA_USER_ID` dans `run_guard()`.
2. **R-02** — corriger les `mk` non liés, garder les `[0]`, et distinguer `ERROR` (test cassé) de
   `FAIL` (produit en régression).
3. **G-02** — soumettre `smtp_host` à la liste blanche des hôtes.
4. **R-03** — instruire `starts_with`, seul candidat à un défaut réel restant.
5. **G-03**, renommage des deux `test_*`, `Skip` plutôt que PASS partiel dans ISO-07.

**Pour toi** : `DUP-02` est une perte de données confirmée, indépendante de la suite. À arbitrer.

## Périmètre respecté

Aucun fichier de `tests/` modifié. La suite a été exécutée quatre fois avec le jeton QA sur le
compte 3 ; le garde-fou a validé chaque démarrage. **File Celery à 0**, sauvegarde intacte,
`mailia-worker` et `mailia-beat` toujours `Exited`. Compte 1 jamais approché.

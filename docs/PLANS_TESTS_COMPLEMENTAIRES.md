# Plans de test complémentaires — MailIA

**Date** : 2026-08-22
**Contexte** : la première campagne a livré une suite de 78 tests (`tests/`) couvrant les défauts
trouvés. Ce document définit ce qu'elle **ne couvre pas**, et les campagnes à mener pour combler
ces angles morts.

> **Contrainte absolue, valable pour tous les plans ci-dessous** : aucun test ne doit lire,
> utiliser ou modifier les données d'un compte `@ebusinet.fr`. Le garde-fou de `tests/qa/guard.py`
> refuse de démarrer si un compte non jetable est visible ; tout nouveau plan doit passer par lui.

---

## Pourquoi ces plans

La suite actuelle est une **suite de non-régression** : elle empêche les 60 défauts trouvés de
revenir. Elle ne cherche pas de défauts nouveaux. Or la campagne a montré trois choses :

1. **Les bugs les plus graves étaient invisibles depuis l'API seule** — la perte d'email venait
   d'un serveur IMAP répondant `OK` à une commande absurde ; le blocage du worker venait d'une
   interaction entre le moteur de règles et un fournisseur IA en panne.
2. **GreenMail masque des comportements.** Plusieurs tests ont dû être abandonnés parce que le
   serveur de test est plus permissif ou plus buggé qu'un serveur réel. Or c'est précisément la
   **stricte conformité** d'un vrai serveur qui a révélé le bug des flags non parenthésés.
3. **L'interface n'a été auditée que statiquement.** Aucun test n'a jamais cliqué dans MailIA.

---

## Plan A — Robustesse des entrées (priorité 1)

**Pourquoi** : le bug le plus grave de la campagne (destruction d'un email) a été trouvé en passant
une chaîne vide là où un nom de dossier était attendu. Un seul cas a été testé par hasard ; la
famille n'a jamais été explorée systématiquement.

**Approche** : pour chaque endpoint et chaque outil MCP acceptant une valeur libre, injecter un jeu
de valeurs limites et vérifier que la réponse est un **refus explicite**, jamais une opération
partielle ni un succès trompeur.

Jeu de valeurs à couvrir :

| Catégorie | Exemples |
|---|---|
| Vide et blanc | `""`, `"   "`, `"\t"`, `"\n"` |
| Séparateurs IMAP | `"."`, `"/"`, `".."`, `"a..b"`, `"/a"`, `"a/"` |
| Identité source/cible | déplacer un email vers son propre dossier |
| Longueur | 1 caractère, 255, 1000, 10000 |
| Encodage | UTF-8 accentué, emoji, IMAP UTF-7 déjà encodé, UTF-7 malformé |
| Caractères de contrôle | `\0`, `\r\n` (injection de commande IMAP), `%00` |
| Injection | `"` non fermé, `\`, `*`, `%` (jokers IMAP) |
| Numériques | UID négatif, zéro, très grand, non numérique, `L` sans chiffres |
| Listes | tableau vide, 10 000 éléments, doublons, `null` dans la liste |

**Critère de réussite** : aucune valeur ne produit de perte de données, de duplication, de 500, ni
de succès annoncé sans effet réel. Un refus en 422 est le résultat attendu.

**Attention** : ce plan est le plus susceptible de détruire des données de test. À exécuter
uniquement sur le compte jetable, et en vérifiant les comptages avant/après chaque cas destructeur.

---

## Plan B — Second serveur IMAP, plus strict (priorité 1)

**Pourquoi** : GreenMail a masqué au moins cinq comportements — pas de recherche par sous-chaîne,
pas de STARTTLS, `DELETE` en échec sur un dossier renommé, horloge décalée de 12 h, `COPY` vers
une destination vide accepté puis message jeté. Chacun a soit empêché un test, soit produit un
faux négatif. À l'inverse, c'est la stricte conformité d'un serveur réel qui avait révélé le bug
des flags.

**Approche** : ajouter **Dovecot** au `docker-compose.yml` comme second serveur jetable, et
paramétrer la suite pour tourner contre l'un ou l'autre (`MAILIA_QA_ACCOUNT_ID` pointant sur un
second compte de test). Le garde-fou doit accepter le nouvel hôte dans sa liste blanche.

Ce qui devient alors testable :
- suppression forcée de dossier, y compris renommé ;
- recherche IMAP par sous-chaîne sur `FROM` / `TO` (le filtre `F-04` n'a jamais pu être validé) ;
- STARTTLS réel, donc le chemin TLS du helper `smtp_client.py` ;
- dates `INTERNALDATE` fiables, donc validation propre de `F-05` et `F-08` ;
- comportement sur quota atteint, dossier concurrent, `EXPUNGE` pendant un parcours.

**Critère de réussite** : la suite complète passe sur les deux serveurs. Tout écart est soit un bug
MailIA, soit une hypothèse implicite sur le serveur qu'il faut documenter.

---

## Plan C — Tests d'interface pilotés par navigateur (priorité 2)

**Pourquoi** : l'interface n'a jamais été exécutée pendant la campagne. Les défauts trouvés
(`FE-01` à `FE-11`, `FS-01` à `FS-04`) l'ont tous été par lecture de code. Un audit statique ne
voit ni les erreurs d'exécution, ni les états incohérents, ni ce qui ne se produit qu'après
plusieurs actions.

**Approche** : automatisation Chrome sur l'instance de test, connecté avec le compte QA.

Parcours à couvrir :
1. Connexion, création d'un compte mail, test IMAP et SMTP, suppression
2. Navigation dans l'arbre de dossiers, y compris noms accentués et imbriqués
3. Lecture d'un email : corps HTML, pièces jointes, mode plein écran
4. Composition : destinataires, autocomplétion, signature résolue, pièce jointe, envoi
5. Répondre / répondre à tous / transférer, avec citation
6. Actions groupées sur sélection multiple, y compris un échec partiel
7. Glisser-déposer d'un email vers un dossier, et vers un dossier local
8. Filtres de colonnes et tris, en combinaison
9. Création d'une règle depuis une sélection, puis application
10. Changement de thème, persistance après rechargement
11. Raccourcis clavier, y compris pendant une saisie (ils doivent être inactifs)

**À surveiller pendant chaque parcours** : la console navigateur (aucune erreur JavaScript non
gérée) et les requêtes réseau (aucun 4xx/5xx non traité par l'interface).

**Critère de réussite** : chaque parcours aboutit, et un échec côté serveur est toujours visible
pour l'utilisateur — c'est précisément ce qui manquait au pilote de synchronisation.

---

## Plan D — Chemin de synchronisation (priorité 2, bloqué)

**Pourquoi** : le worker est arrêté. Tout ce qui concerne la synchronisation, l'application
automatique des règles et l'indexation en masse reste non validé — y compris les correctifs écrits
pendant la campagne, qui n'ont **jamais tourné**.

**Préalable** : décision de l'utilisateur sur le redémarrage du worker, et chronométrage d'une
synchronisation complète sans limite de temps.

À valider une fois débloqué :
- la progression est bien persistée après chaque lot (tuer le worker en cours et vérifier la reprise) ;
- un email irrécupérable bloque son dossier sans faire perdre de courrier ;
- le disjoncteur IA se déclenche après trois délais dépassés et n'est pas réarmé au dossier suivant ;
- le verrou par compte empêche deux synchronisations simultanées ;
- la limite dure tue bien la tâche, et le verrou se libère par expiration ;
- les règles classiques et IA s'appliquent réellement pendant une synchronisation ;
- `ProcessingLog` se remplit.

**Ce plan doit tourner exclusivement sur le compte jetable**, avec un volume artificiel suffisant
(quelques milliers d'emails importés) pour reproduire les conditions de charge.

---

## Plan E — Concurrence (priorité 3)

**Pourquoi** : aucun test n'exerce deux opérations simultanées. Les bugs de concurrence ne se
manifestent qu'en production, et l'application partage des connexions IMAP.

Scénarios :
- deux déplacements simultanés du même email ;
- suppression pendant une lecture ;
- deux imports concurrents sur le même dossier ;
- synchronisation pendant une action utilisateur sur le même dossier ;
- deux clients modifiant le même brouillon ;
- création simultanée du même nom de dossier.

**Critère de réussite** : aucune perte, aucune duplication, aucun état incohérent. Un conflit doit
produire une erreur claire, jamais un résultat silencieusement faux.

---

## Plan F — Volume et limites (priorité 3)

**Pourquoi** : le compte réel compte 45 000 emails et 180 dossiers ; le compte de test en compte
quelques dizaines. Les comportements de pagination, de troncature et de délai n'ont jamais été
exercés à l'échelle réelle.

- dossier de 10 000 emails : liste, pagination, tri, filtres
- suppression et déplacement en masse de 1 000 emails
- import d'un mbox de 500 Mo, et reprise après interruption
- pièce jointe de 25 Mo
- 200 dossiers imbriqués sur 5 niveaux
- recherche renvoyant plus de 10 000 résultats

**À mesurer** autant qu'à valider : les temps de réponse constituent la référence qui manquait pour
calibrer les limites de temps du worker.

---

## Plan G — Sécurité, second passage (priorité 2)

**Pourquoi** : trois failles ont été trouvées sans que la sécurité soit auditée méthodiquement.
Elles l'ont été en passant, au fil des tests fonctionnels.

- **Jetons** : expiré, signature modifiée, algorithme `none`, `sub` inexistant, `sub` d'un autre
  utilisateur, jeton de réinitialisation utilisé comme jeton d'accès
- **Limitation de débit** : vérifier les seuils annoncés (5/min connexion, 3/min inscription,
  3/5 min mot de passe oublié) et le comportement au-delà
- **En-têtes de sécurité** : présence et valeur des six annoncés, sur toutes les réponses
- **Élévation de privilège** : un non-administrateur atteignant les endpoints `/admin/*`
- **Injection** : SQL sur les champs de recherche et de filtre, commande IMAP via `\r\n`,
  chemin (`../`) sur l'import depuis un chemin serveur
- **Fuite d'information** : messages d'erreur exposant des détails serveur (le rapport `R-10`
  signalait déjà des erreurs IMAP brutes renvoyées au client)
- **Isolation MCP** : rejouer `ISO-05` après tout ajout d'outil

---

## Ordre d'exécution recommandé

1. **Plan A** — le plus rentable : c'est ainsi qu'a été trouvé le bug le plus grave, et il ne
   dépend d'aucun préalable.
2. **Plan B** — débloque une partie des tests abandonnés et révélera les hypothèses implicites sur
   le serveur.
3. **Plan G** — audit méthodique de ce qui n'a été effleuré qu'au hasard.
4. **Plan C** — l'interface, jamais exécutée.
5. **Plan D** — dès que le worker est autorisé à redémarrer.
6. **Plans E et F** — quand les précédents sont stabilisés.

Chaque plan mené doit se conclure par des tests ajoutés à `tests/`, sans quoi il ne protège rien.

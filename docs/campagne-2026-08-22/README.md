# Rapports détaillés — campagne de test du 2026-08-22

Synthèse à lire en premier : [`../CAMPAGNE_TESTS_2026-08-22.md`](../CAMPAGNE_TESTS_2026-08-22.md).

Ces 13 rapports sont les pièces justificatives de la campagne : chaque bug y figure avec sa preuve,
le fichier et la ligne concernés, et la façon dont le correctif a été vérifié. Ils sont archivés ici
parce qu'ils constituent la seule trace détaillée du raisonnement.

## Déroulé chronologique

| Ordre | Fichier | Contenu |
|---|---|---|
| 1 | `RAPPORT_TESTS.md` | Itération 1 — API. 58 PASS, 17 FAIL, 9 SKIP. Découverte de la faille IDOR (F-00), de la duplication d'emails (F-02) et de la recherche cassée (F-01). |
| 2 | `RAPPORT_CORRECTIFS.md` | Lot 1 — les 17 bugs corrigés, plus 4 de la même famille. L'IDOR touchait en réalité 7 endpoints. |
| 3 | `RAPPORT_FRONTEND_WORKER.md` | Audit statique de l'interface et revue critique des correctifs du worker. Découverte de la XSS stockée (FE-01) et des défauts du premier garde-fou de synchronisation (WK-01 à WK-09). |
| 4 | `RAPPORT_CORRECTIFS_LOT2.md` | Lot 2 — frontend et worker. Assainisseur `mdSafe()` validé contre 15 charges XSS, encodeur `escJs()`. Contient l'addendum de revue croisée qui a révélé la perte d'email due à l'interaction de deux correctifs. |
| 5 | `RAPPORT_TESTS_ITER2.md` | Itération 2 — les 67 outils MCP, jamais testés jusque-là. 16 nouveaux bugs, dont une faille de cloisonnement (N-04) et la recherche sémantique structurellement inopérante (N-05). |
| 6 | `RAPPORT_CORRECTIFS_LOT3.md` | Lot 3 — surface MCP. **Contient la découverte des 26 940 tâches armées dans Redis.** Famille `smtp_ssl` close par un helper unique. |
| 7 | `RAPPORT_FRONTEND_SUITE.md` | Suite de l'audit frontend. Tentatives de contournement de `mdSafe()` et `escJs()` : aucune réussie. |
| 8 | `RAPPORT_CORRECTIFS_LOT4.md` | Lot 4 — le worker absent est enfin signalé à l'utilisateur (`worker_online()`). |
| 9 | `RAPPORT_TESTS_ITER3.md` | Itération 3 — validation. Conclusion : application saine sur la partie testable, vérifiée par tentative de contournement réelle. |
| 10 | `RAPPORT_CORRECTIFS_LOT5.md` | Lot 5 — famille du `SELECT` non vérifié traitée sur 31 sites. |
| 11 | `AUDIT_DOCUMENTATION.md` | Audit de véracité de `FONCTIONNALITES.md` : 9 affirmations fausses, 8 inexactes. Le document a été corrigé en conséquence. |
| 12 | `RAPPORT_SUITE_TESTS.md` | Construction de la suite de non-régression. Contient IT4-01, le bug de destruction d'email trouvé à la première exécution. |
| 13 | `REVUE_SUITE_TESTS.md` | Revue critique du garde-fou de la suite par un second agent : 6 scénarios d'attaque, 6 refus, et 2 trous corrigés depuis. |

## Comment les lire

Les identifiants de bugs sont stables d'un rapport à l'autre et repris dans les tests :
`F-xx` (itération 1), `N-xx` (itération 2), `FE-xx` / `WK-xx` (frontend et worker),
`FS-xx` (suite frontend), `IT3-xx` / `IT4-xx` (itérations 3 et 4), `G-xx` (garde-fou).

Un test de `tests/` qui échoue cite l'identifiant du bug d'origine — il suffit de le chercher dans
ces rapports pour retrouver le contexte complet.

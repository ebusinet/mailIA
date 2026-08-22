# Audit de véracité — `docs/FONCTIONNALITES.md`

**Date** : 2026-08-22
**Méthode** : chaque affirmation des 20 sections confrontée aux résultats des itérations 1 et 2
(tests d'exécution) et, à défaut, à la lecture du code. Aucun appel API ni conteneur sollicité
pour cet audit ; le fichier n'a pas été modifié.

## Barème

- **FAUX** — la fonctionnalité n'existe pas, ou n'a jamais fonctionné
- **INEXACT** — elle existe, mais pas comme décrite (périmètre, condition, canal)
- **INCOMPLET** — une limite importante n'est pas mentionnée
- **OK** — vérifié conforme
- **NON VÉRIFIÉ** — hors périmètre de la campagne, signalé pour honnêteté

## Décompte

| | Nombre |
|---|---|
| FAUX | **9** |
| INEXACT | **8** |
| INCOMPLET | **6** |
| OK | ~40 affirmations vérifiées conformes |
| NON VÉRIFIÉ | 3 |

Les trois écarts les plus graves, par ordre de gravité pour l'utilisateur :

1. **§19** affirme qu'il n'y a « aucun accès croisé entre utilisateurs ». C'était **faux sur 7
   endpoints** (F-00) et ça l'est **encore dans le serveur MCP** (N-04). Une promesse de sécurité
   non tenue est la pire catégorie d'erreur documentaire.
2. **§4** annonce une recherche sémantique. Elle n'a **jamais pu fonctionner** : aucun document
   n'a jamais d'embedding indexé (N-05).
3. **§2** annonce la configuration du SSL SMTP. **L'interface ne comporte aucun champ pour cela**
   (FS-01, voir le rapport frontend) : un serveur SMTP en clair est inconfigurable depuis l'application.

---

## §1 — Vue d'ensemble

| Affirmation | Verdict | Réalité |
|---|---|---|
| « un moteur de synchronisation en arrière-plan » | **INCOMPLET** | Le moteur existe mais est resté **bloqué trois mois** (`last_sync_at` du compte principal : 2026-05-20, constaté le 2026-08-22). La cause a été identifiée et corrigée pendant la campagne, mais les correctifs n'ont pas pu être validés à l'exécution. |
| « un bot Telegram et un serveur d'outils IA (MCP) » | OK | Les deux existent. |

**Reformulation proposée** — rien à changer sur le fond ; la §12 doit porter la nuance.

---

## §2 — Comptes et authentification

| Affirmation | Verdict | Réalité |
|---|---|---|
| « inscription (réservée à un admin — pas d'auto-inscription publique) » | OK | Vérifié : `POST /auth/register` sans jeton admin → 403. |
| « lien de réinitialisation valable 30 minutes » | OK | `src/security.py:29` : `timedelta(minutes=30)`. |
| « réponse volontairement identique que l'email existe ou non » | OK | Vérifié : `{"status":"ok"}` sur une adresse inexistante. |
| **« Pour chaque compte : hôte/port/SSL IMAP et SMTP »** | **FAUX** | **Le SSL SMTP n'est configurable nulle part.** Le formulaire ne contient qu'une case `acc-imap-ssl` (`index.html:1235`) ; aucun équivalent SMTP. Le payload envoyé (`index.html:4578-4586`) omet `smtp_ssl`, à la création **comme** à la modification. Tout compte créé depuis l'interface reçoit `smtp_ssl=true` par défaut. Un serveur SMTP en clair est donc inutilisable. |
| **« test IMAP et test SMTP indépendants sur un compte existant »** | **INEXACT** | Le test IMAP fonctionne. Le test SMTP **via `test-credentials`** force STARTTLS sans condition (`accounts.py:212`) et le schéma `TestCredentials` n'a même pas de champ `smtp_ssl` : il échoue systématiquement sur un serveur en clair (N-02). Seul `POST /accounts/{id}/test-smtp`, sur un compte déjà enregistré, est correct. |
| « Suppression de compte avec suppression en cascade » | OK | Vérifié. |

**Reformulation proposée** :
> Pour chaque compte : hôte, port et SSL **IMAP**, hôte, port et identifiants SMTP, activation de
> la synchronisation. *Limite actuelle : le chiffrement SMTP (SSL/STARTTLS) n'est pas configurable
> depuis l'interface — un serveur SMTP en clair nécessite une intervention en base ou par l'API.*

---

## §3 — Consultation et gestion des emails

| Affirmation | Verdict | Réalité |
|---|---|---|
| Arbre des dossiers, décodage IMAP UTF-7 | OK | Vérifié (`QA &AMk-l&AOk-ments` → `QA Éléments`). |
| Pagination, compteurs par dossier | OK | Vérifié. |
| « corps HTML dans un cadre isolé (sandbox, sans exécution de script) » | OK | Vérifié : `sandbox="allow-same-origin allow-popups allow-popups-to-escape-sandbox"`, `allow-scripts` bien absent. |
| Pièces jointes téléchargeables | OK | Vérifié (`Content-Disposition`, `nosniff`, contenu conforme). |
| **« Filtres de colonnes : … plage de dates »** | **INCOMPLET** | Le filtre de date porte sur l'**INTERNALDATE** (`accounts.py:1292-1296`) alors que la colonne **affiche** l'en-tête `Date:` depuis le correctif F-05. Preuve mesurée : `filter_date=2026-08-15` ne renvoie pas le message qui affiche pourtant `2026-08-15` (N-01). |
| **« Tri par colonne (de, objet, date) »** | **INCOMPLET** | Le tri par date utilise l'INTERNALDATE (`accounts.py:1289`), pas la date affichée. Sur un jeu réel, la liste « triée par date » présente des ruptures visibles (N-01). |
| « par expéditeur » | **INCOMPLET** | Le filtre s'appuie sur `IMAP SEARCH FROM` : la granularité dépend entièrement du serveur. Sur un serveur ne gérant que la correspondance exacte d'adresse, filtrer par nom ou par domaine ne renvoie rien. |
| **« déplacer vers un autre dossier/compte »** | **FAUX** (partie « /compte ») | Aucun déplacement inter-comptes n'existe. `MoveRequest` ne porte qu'un `target_folder`, et aucun paramètre `target_account` n'existe nulle part dans le code. |
| « épingler » | **INCOMPLET** | L'épinglage est stocké en `localStorage` (`index.html:5409-5410`), donc **local au navigateur** : il n'est ni synchronisé entre appareils ni conservé après effacement des données du site. |
| **« Export : export d'un dossier (ou de résultats de recherche) et export d'emails individuels »** | **FAUX** (deux tiers) | Seul l'export de dossier existe (`GET /accounts/{id}/folder-export`, un unique point d'appel : `index.html:5045`). Aucun export de résultats de recherche, aucun export d'email individuel. |
| « Raccourcis clavier : Échap, N, R, Suppr — inactifs pendant la saisie » | OK | Les quatre sont implémentés (`index.html:7630-7645`) avec le garde `isTyping()`. |

**Reformulations proposées** :
> - **Actions sur un email** : … déplacer vers un autre dossier **du même compte**, supprimer.
> - **Épingler** : marquage local au navigateur (non synchronisé entre appareils).
> - **Export** : export d'un dossier complet au format ZIP (fichiers `.eml`).
> - Retirer « plage de dates » des filtres tant que N-01 n'est pas corrigé, ou préciser que le
>   filtre et le tri s'appuient sur la date de réception serveur, qui peut différer de la date
>   affichée pour les emails importés ou réinjectés.

---

## §4 — Recherche

| Affirmation | Verdict | Réalité |
|---|---|---|
| Recherche plein texte avec filtres combinables, surlignage | OK | Vérifié après correction de F-01 (la recherche renvoyait un 500 sur tout résultat). |
| **« tri par pertinence »** | **FAUX** | `indexer.py:210` impose `"sort": [{"date": {"order": "desc"}}]` sans `track_scores`. Elasticsearch renvoie donc `_score: null` pour **tous** les résultats — mesuré : les scores valent tous `0.0`. Le tri est exclusivement chronologique décroissant, la pertinence n'est jamais calculée. |
| **« Recherche sémantique : recherche vectorielle par embeddings (`dense_vector` + requête kNN) — trouve des emails par proximité de sens »** | **FAUX** | L'outil existe mais n'a **jamais pu** fonctionner, pour deux raisons cumulées et indépendantes : (1) le modèle `all-MiniLM-L6-v2` produit 384 dimensions alors que le mapping ES en déclare 768 (`indexer.py:57-59`) — toute requête est rejetée en HTTP 400 ; (2) surtout, **aucun document n'a jamais d'embedding** : `index_email()` accepte le paramètre mais aucun appelant ne le renseigne, et `bulk_index_emails()` — le chemin réellement utilisé — ne construit même pas le champ. Mesuré : `_count` sur `exists: embedding` → **0 document sur 32**. Même dimensions alignées, le kNN renverrait 0 résultat. |
| Recherche multi-dossiers IMAP en direct | OK | Vérifié, avec remontée d'erreur par dossier. |
| **« Recherche croisée (via l'assistant IA / MCP) : recherche combinée à travers plusieurs comptes et dossiers en une seule requête »** | **INEXACT** | `search_cross_folder` (`mcp/server.py:1447-1448`) prend un `account_id: int` **obligatoire et unique**. La recherche est multi-**dossiers**, jamais multi-**comptes**. |

**Reformulation proposée** :
> - **Recherche plein texte** (Elasticsearch) : par mot-clé avec filtres combinables (compte,
>   dossier, expéditeur, plage de dates, présence de pièce jointe), surlignage des termes trouvés,
>   **tri chronologique décroissant**, pagination.
> - **Recherche multi-dossiers IMAP** : recherche en direct sur le serveur à travers plusieurs
>   dossiers d'un même compte, avec remontée des erreurs dossier par dossier.
> - Supprimer entièrement les entrées « Recherche sémantique » et « Recherche croisée
>   multi-comptes », et les déplacer en §20.

---

## §5 — Rédaction et envoi

| Affirmation | Verdict | Réalité |
|---|---|---|
| Éditeur riche TinyMCE, autocomplétion, pièces jointes | OK | Vérifié. |
| Répondre / répondre à tous / transférer avec citation | OK | Vérifié ; `In-Reply-To` et `References` corrects côté API. |
| **« Brouillons : sauvegarde, mise à jour, liste, suppression »** | **INEXACT** | Seule la **sauvegarde** est accessible depuis l'application : un unique appel `save-draft` dans l'interface, et aucun endpoint HTTP pour les trois autres. `update_draft`, `list_drafts` et `delete_draft` n'existent que comme **outils MCP**, donc uniquement pilotables par l'assistant IA. |
| Bulle de chat IA de composition | OK | Présente et conforme à la description. |
| Liens cliquables dans le corps | OK | |

**Reformulation proposée** :
> - **Brouillons** : sauvegarde dans le dossier Drafts du serveur. *La modification, le listage et
>   la suppression d'un brouillon ne sont disponibles que via l'assistant IA (outils MCP), pas
>   depuis l'interface.*

---

## §6 — Stockage local

| Affirmation | Verdict | Réalité |
|---|---|---|
| Créer/supprimer des dossiers locaux | OK | Vérifié, suppression en cascade sans email orphelin. |
| **« purge des dossiers locaux vides »** | **INEXACT** | `purge_empty_local_folders` est un **outil MCP** uniquement — aucun endpoint HTTP, aucun bouton. |
| Lister, lire les emails locaux (préfixe `L`) | OK | Vérifié. |
| Déplacer IMAP ↔ local | OK | Vérifié après correction de F-07 (500) et F-08 (date écrasée). *Avant la campagne, aucun des deux sens ne fonctionnait correctement.* |
| **« Détection de doublons entre stockage local et IMAP dans les deux sens »** | **INEXACT** | `find_duplicates_imap_vs_local` et `find_duplicates_local_vs_imap` sont des **outils MCP** uniquement. |
| **« Statistiques par dossier local »** | **INEXACT** | `local_folder_stats` est un **outil MCP** uniquement. |

**Reformulation proposée** : ajouter en tête de section —
> Les opérations de base (créer, supprimer, lister, lire, déplacer) sont disponibles dans
> l'interface. La purge des dossiers vides, la détection de doublons et les statistiques ne sont
> accessibles que via l'assistant IA.

---

## §7 — Import d'emails

| Affirmation | Verdict | Réalité |
|---|---|---|
| Import mbox/ZIP, upload ou chemin serveur, vers IMAP ou stockage local | OK | Vérifié dans les deux modes de destination. |
| Traitement en arrière-plan avec suivi de job | OK | Vérifié (progression détaillée, `folders_done`). |
| **Reprise en cas d'interruption** | OK | Vérifié : dossier déjà traité correctement sauté, refus propre sur un job terminé, 404 sur un job inconnu. |
| Historique des jobs consultable | OK | |

**Section entièrement conforme.** À noter comme point positif : le dédoublonnage par `Message-ID`
fonctionne (5 messages, 5 sautés au ré-import) et les dates d'origine sont préservées via
l'INTERNALDATE — ce que le déplacement local→IMAP ne faisait pas avant correction.

---

## §8 — Signatures

Section **entièrement conforme**. La chaîne de priorité contact > groupe > défaut a été vérifiée
dans les trois configurations, ainsi que l'unicité de la signature par défaut. *(Le détachement par
`signature_id: null` était cassé — F-12 — mais c'est un défaut d'API non décrit dans le document.)*

---

## §9 — Contacts et groupes

| Affirmation | Verdict | Réalité |
|---|---|---|
| CRUD contacts multi-adresses, CRUD groupes, membres | OK | Vérifié. |
| Autocomplétion par nom ou email | OK | Vérifié, avec dédoublonnage par adresse. |
| **« Un contact peut être créé directement à partir d'un email reçu »** | **NON VÉRIFIÉ** | L'outil MCP `contact_from_email` existe (et présente le défaut N-08 : sa regex prend les dates ISO pour des numéros de téléphone). Le parcours équivalent depuis l'interface n'a pas été testé. |

---

## §10 — Détection de spam

Section **entièrement conforme**, et c'est la mieux tenue du document. Vérifié : scan mono et
multi-dossiers, progression `ndjson` en temps réel, analyse des en-têtes standards
(`X-Spam-Flag`, `X-Spam-Status`, `Authentication-Results` → `spf_fail, dkim_fail, dmarc_fail`),
heuristiques d'objet, listes blanche et noire par adresse **et par domaine** avec effet réel
mesuré (3 spams → 1 après mise en liste blanche), affichage du score dans les listes et la
recherche multi-dossiers.

L'affirmation « sans envoyer le contenu à un service tiers » est exacte : l'analyse est purement
locale et se fonde sur les en-têtes.

*Réserve mineure (N-11) : les deux outils MCP `spam_analysis` et `scan_for_spam` calculent le score
de confiance différemment et se contredisent sur un même message.*

---

## §11 — Règles automatiques

| Affirmation | Verdict | Réalité |
|---|---|---|
| Conditions structurées (expéditeur, destinataire, objet, PJ, score spam, statut de réponse, taille, âge) | OK | Les 13 opérateurs vérifiés individuellement. *Le champ « destinataire » ne fonctionnait pas sur les emails multi-destinataires — F-09, corrigé.* |
| Actions : déplacer, marquer lu, marquer important, marquer spam, supprimer, transférer | OK | Toutes vérifiées, **y compris `forward`** (`{"forwarded":1,"failed":0}` avec livraison réelle constatée). *Avant correction, `forward` était validée mais silencieusement ignorée — F-10 — et les quatre actions de marquage/suppression échouaient sur serveur strict — F-02.* |
| **« Aperçu ("preview") d'une règle avant activation pour voir quels emails seraient concernés »** | **FAUX** | Aucun aperçu n'existe pour les règles **classiques** : le seul endpoint est `POST /rules/classic/{id}/apply`, qui **exécute toujours** les actions. Pour les règles **IA**, `POST /rules/{id}/preview` existe mais montre le *résultat du parsing markdown* (conditions, actions, `needs_ai`) — **pas** la liste des emails concernés. L'affirmation est donc fausse dans les deux cas. |
| Règles IA en langage naturel (Markdown) | **INCOMPLET** | La grammaire acceptée est **très restreinte** et non documentée : le parseur exige la syntaxe exacte `- **Si**:` / `- **Alors**:` (`rules/parser.py:60`) et ne reconnaît que quelques formulations figées. Une action non reconnue est **silencieusement ignorée** : la règle est enregistrée, comptée dans `parsed_count`, affichée comme active, et ne fait jamais rien. |
| Priorité, activation/désactivation, notification Telegram | **INCOMPLET** | Le mécanisme existe, mais la notification dépend du worker Celery, resté inopérant trois mois. |

**Reformulations proposées** :
> - **Règles classiques** : … Actions possibles : déplacer, marquer lu, marquer important, marquer
>   spam, supprimer, transférer. *Il n'existe pas d'aperçu : appliquer une règle exécute
>   immédiatement ses actions sur le dossier choisi.*
> - **Règles IA** : rédigées en Markdown selon une syntaxe précise (`- **Si**: …` / `- **Alors**: …`).
>   *Les formulations non reconnues par l'analyseur sont ignorées sans avertissement — utiliser
>   l'aperçu pour vérifier que conditions et actions ont bien été interprétées.*

---

## §12 — Synchronisation automatique

| Affirmation | Verdict | Réalité |
|---|---|---|
| **« Un service de fond (Celery) synchronise tous les comptes actifs toutes les 5 minutes »** | **FAUX au moment de la rédaction** | Le planificateur était configuré ainsi, mais la synchronisation était **totalement bloquée depuis le 2026-05-20**, soit trois mois : chaque email déclenchait un appel au fournisseur IA avec un délai d'attente de 600 s, saturant les deux emplacements du pool. Le document a été rédigé le 2026-08-21 en décrivant l'intention du code, pas son comportement. |
| Suivi du dernier UID par dossier pour ne récupérer que les nouveautés | **INCOMPLET** | Le mécanisme existe, mais l'avancement n'était persisté qu'à la toute fin du cycle (WK-01) : une interruption faisait tout recommencer. Corrigé depuis, non validé à l'exécution. |
| Synchronisation manuelle immédiate | **INEXACT** | L'endpoint répond `200 {"status":"sync_started"}` **même si aucun worker n'est disponible** — la tâche est simplement mise en file, sans retour à l'utilisateur. C'est précisément ce qui a rendu le blocage invisible trois mois durant. |
| **« Chaque action automatique (règle appliquée, synchronisation) est journalisée et consultable dans les logs de traitement »** | **FAUX** | `ProcessingLog` n'est créé qu'à un seul endroit du code : `worker/tasks.py:353`, dans `_execute_actions`, c'est-à-dire **pour les règles IA uniquement**. Les règles **classiques** (`api/routes/rules.py`) ne journalisent rien, et **aucune synchronisation** n'est journalisée. Constaté : `/admin/status` renvoie `processing_logs: {"total": 0}` et l'outil `get_processing_logs` une liste vide. |

**Reformulation proposée** :
> - Un service de fond (Celery) synchronise les comptes actifs toutes les 5 minutes …
> - **Journalisation** : les actions déclenchées par les **règles IA** sont enregistrées et
>   consultables dans les logs de traitement. Les règles classiques et les cycles de
>   synchronisation ne sont pas journalisés.
> - Mentionner que la synchronisation manuelle est asynchrone : la réponse confirme la mise en
>   file, pas l'exécution.

---

## §13 — Assistant IA

| Affirmation | Verdict | Réalité |
|---|---|---|
| **« Claude Native (CLI Claude Code en sandbox, sans clé API) »** | **INEXACT** | Le fournisseur envoie **toujours** un en-tête `Authorization: Bearer …` (`claude_native_provider.py:58` et `:84`). Sans clé, il émettait un en-tête vide et plantait avant même la requête (`Illegal header value b'Bearer '` — F-16) ; depuis correction, la requête part et le proxy répond `401 Invalid API key`. Une clé est donc bien requise. |
| **« ~74 outils »** | **INEXACT** | **67** exactement (`grep -c "@mcp.tool"` → 67, confirmé par l'énumération du serveur). |
| Catégories d'outils listées | OK | Les huit catégories correspondent bien à ce qui existe. Les 67 outils ont été testés : **61 fonctionnels, 6 en échec**. |
| Chat en streaming, conversations parallèles, historique | OK | |
| Liens cliquables générés par l'IA | OK | Implémenté par marqueurs `[[email:…]]` / `[[attachment:…]]`. |
| Plans de tâches, bouton d'arrêt, reprise automatique, activité des outils en temps réel | OK | Code présent et conforme à la description. |
| « Listes d'emails … limitées à 20 lignes affichées » | **NON VÉRIFIÉ** | Consigne de prompt, non testable sans fournisseur IA opérationnel. |

**Reformulation proposée** :
> - **Fournisseurs IA configurables** : Claude (API Anthropic), Claude Native (via un proxy
>   exécutant le CLI Claude Code — **une clé d'accès au proxy reste nécessaire**), OpenAI/compatible,
>   Ollama, pont local WebSocket.
> - **Accès complet aux emails via des outils** (protocole MCP, **67 outils**) …

---

## §14 — Digest hebdomadaire

| Affirmation | Verdict | Réalité |
|---|---|---|
| Endpoint, plafond de 500 emails, 7 jours par défaut / 30 maximum, cache 1 heure | OK | Validation du paramètre `days` vérifiée (422 au-delà de 30) ; plafonds et cache présents dans le code. |
| La liste des 10 rubriques produites | **NON VÉRIFIÉ** | Le fournisseur IA n'a jamais été opérationnel pendant la campagne. L'endpoint échoue proprement (`502 AI analysis failed`), mais **aucun digest n'a jamais été produit de bout en bout**. Le contenu réel du rapport reste une promesse non démontrée. |

**Reformulation proposée** : conserver la section, mais préciser que la richesse du rapport dépend
entièrement du fournisseur IA configuré et de son modèle.

---

## §15 — Bot Telegram

**NON VÉRIFIÉ** dans son intégralité : aucun token de bot n'est configuré
(`telegram_bot_token` vide dans les paramètres système). Les six commandes décrites n'ont pas pu
être exercées. À signaler comme tel plutôt que de laisser croire à une fonctionnalité éprouvée.

---

## §16 — Notifications

**OK** — la section est exacte et, fait notable, elle est déjà rédigée sur le mode de la limite
(« Il n'existe pas aujourd'hui de notification par email ni de notification push navigateur »).
C'est le registre que devraient adopter les autres sections.

Une nuance à ajouter : le seul canal existant dépend du worker Celery, resté inopérant trois mois —
donc **aucune notification n'a été émise pendant cette période**.

---

## §17 — Personnalisation de l'interface

| Affirmation | Verdict | Réalité |
|---|---|---|
| « Thèmes : clair, sombre, violet, nord, dracula, emerald, lemon, ocean, rose » | OK | Les 9 thèmes existent, vérifiés dans le CSS et les attributs `data-theme`. |
| Réglages d'affichage, préférences mémorisées | OK | Stockage `localStorage`, conforme à la mention « localStorage / paramètres utilisateur ». |

**Section entièrement conforme.**

---

## §18 — Administration

| Affirmation | Verdict | Réalité |
|---|---|---|
| Paramètres système, valeurs chiffrées affichées masquées | OK | Vérifié : `sk-ant-QATEST123` relu `sk-***123`. |
| Gestion des utilisateurs, un admin ne peut retirer ses propres droits | OK | Vérifié (400). |
| Tableau de statut : synchro par compte/dossier, santé ES, état du worker, 50 derniers logs | OK | Vérifié. *À savoir : cet endpoint parcourt tous les comptes de l'instance et ouvre une connexion IMAP en lecture seule vers chacun — c'est lent et cela sollicite les serveurs de messagerie.* |
| **« Diagnostic des fournisseurs IA : test de connexion »** | **INCOMPLET** | Fonctionne, mais l'une des cinq vérifications est fausse : le contrôle « MCP Server » est rapporté `ok` alors que son propre détail indique `HTTP 404`. Un diagnostic qui affiche « ok » sur un 404 induit en erreur. |

---

## §19 — Sécurité

| Affirmation | Verdict | Réalité |
|---|---|---|
| JWT sur tous les points d'entrée sauf santé, connexion, mot de passe oublié/réinitialisation | OK | Vérifié : 403 sans jeton, 401 sur jeton invalide. |
| « 5 tentatives/min sur la connexion, 3/min sur l'inscription, 3/5 min sur la réinitialisation » | OK | Les trois valeurs sont exactes (`api/middleware.py:15-16`) ; connexion et réinitialisation vérifiées à l'exécution (429 au 6ᵉ et au 4ᵉ essai). |
| En-têtes de sécurité systématiques | OK | Les six présents. |
| Corps HTML affiché sans exécution de script | OK | Vérifié. |
| Mots de passe des comptes mail chiffrés en base | OK | `encrypt_value` sur tous les chemins d'écriture. |
| **« Chaque compte mail et chaque donnée associée est strictement rattaché à son propriétaire (aucun accès croisé entre utilisateurs) »** | **FAUX** | C'était faux sur **7 endpoints** au moment de la rédaction : toutes les branches `storage=local` de `list_messages`, `get_message`, `update_flags`, `move_message`, `delete_message`, `delete_bulk` et `download_attachment` retournaient avant l'appel à la fonction de contrôle de propriété. N'importe quel utilisateur authentifié pouvait lire les **1227 emails locaux** d'un autre. Corrigé côté API et re-vérifié. **Mais l'outil MCP `move_local_email` (`mcp/server.py:3055`) présente encore exactement le même défaut** : il valide le compte et le dossier cible, mais charge `LocalEmail.id == email_id` sans jointure au propriétaire — permettant de déplacer l'email d'un autre utilisateur vers son propre dossier, puis de le lire. |

**Manques de la section**, à ajouter :
- Le jeton JWT est stocké en clair dans le `localStorage` du navigateur.
- Aucune politique CSP n'est appliquée aux pages de l'application.
- Une **XSS stockée** existait dans l'affichage des résultats de recherche (les fragments de
  surlignage Elasticsearch, qui contiennent le corps de l'email, étaient injectés sans
  échappement). Corrigée côté client ; le serveur renvoie toujours la charge brute.

**Reformulation proposée** :
> - Chaque compte mail et chaque donnée associée est rattaché à son propriétaire ; tous les points
>   d'entrée de l'API vérifient cette appartenance avant tout accès.

(et ne réintroduire la formule absolue « aucun accès croisé » qu'une fois N-04 corrigé)

---

## §20 — Ce que MailIA ne fait pas

Les cinq limites déjà listées sont **exactes et vérifiées** (pas d'auto-inscription, pas de digest
par email, pas de notification hors Telegram, pas de 2FA, pas de calendrier intégré).

### Entrées à ajouter

1. **Pas de recherche sémantique fonctionnelle.** L'outil existe mais aucun embedding n'est indexé :
   il ne renvoie jamais de résultat.
2. **Pas de tri par pertinence** dans la recherche plein texte : le classement est exclusivement
   chronologique décroissant.
3. **Pas d'aperçu des règles classiques** : appliquer une règle exécute immédiatement ses actions.
   L'aperçu des règles IA ne montre que l'interprétation du markdown, pas les emails concernés.
4. **Pas de configuration du chiffrement SMTP depuis l'interface** : un serveur SMTP en clair
   n'est pas configurable sans intervention par l'API ou en base.
5. **Pas de déplacement d'emails entre comptes.**
6. **Pas de gestion des brouillons depuis l'interface** (modification, listage, suppression) :
   ces opérations ne sont accessibles qu'à l'assistant IA.
7. **Pas d'export de résultats de recherche ni d'email individuel** : seul l'export d'un dossier
   complet existe.
8. **Purge des dossiers locaux vides, détection de doublons et statistiques locales** : accessibles
   uniquement via l'assistant IA.
9. **Pas de journalisation des synchronisations ni des règles classiques** : seules les actions des
   règles IA alimentent les logs de traitement.
10. **Épinglage non synchronisé** : local au navigateur.
11. **Pas de politique de sécurité de contenu (CSP)** sur les pages de l'application.

---

## Recommandation transversale

Le document décrit fidèlement **l'intention du code**, pas son comportement observé — ce qui est
l'écueil naturel d'une documentation produite par lecture de source avant toute campagne de test.
Sept des neuf verdicts « FAUX » portent sur des fonctionnalités qui **n'ont jamais fonctionné** :
elles étaient présentes dans le code, appelables, et retournaient même un succès apparent.

Deux garde-fous pour la suite :

1. **Marquer chaque affirmation d'une provenance** — « vérifié par test » ou « lu dans le code ».
   L'écart entre les deux est exactement ce qu'a révélé cette campagne.
2. **Distinguer explicitement les fonctionnalités accessibles depuis l'interface de celles réservées
   à l'assistant IA.** Six écarts sur les vingt-trois recensés viennent de cette confusion : le
   document présente comme des fonctionnalités de l'application des outils que seule l'IA peut
   invoquer.

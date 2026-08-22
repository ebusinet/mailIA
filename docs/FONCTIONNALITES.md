# Inventaire fonctionnel — MailIA

> Ce document liste **toutes les fonctionnalités** de MailIA telles qu'utilisables par un
> utilisateur final, sans détail d'implémentation. Pour l'architecture technique (couches,
> flux de données, modèle de données, décisions de conception), voir le
> [Guide Développeur](GUIDE-DEVELOPPEUR.md).
>
> Rédigé le 2026-08-21 par lecture du code source, puis **corrigé le 2026-08-22 à l'issue d'une
> campagne de test complète**. Les affirmations déduites du code mais démenties par les tests ont
> été rectifiées : une fonctionnalité présente dans le code n'est pas nécessairement opérationnelle.

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Comptes et authentification](#2-comptes-et-authentification)
3. [Consultation et gestion des emails](#3-consultation-et-gestion-des-emails)
4. [Recherche](#4-recherche)
5. [Rédaction et envoi](#5-rédaction-et-envoi)
6. [Stockage local (dossiers hors IMAP)](#6-stockage-local-dossiers-hors-imap)
7. [Import d'emails](#7-import-demails)
8. [Signatures](#8-signatures)
9. [Contacts et groupes](#9-contacts-et-groupes)
10. [Détection de spam](#10-détection-de-spam)
11. [Règles automatiques](#11-règles-automatiques)
12. [Synchronisation automatique](#12-synchronisation-automatique)
13. [Assistant IA](#13-assistant-ia)
14. [Digest hebdomadaire](#14-digest-hebdomadaire)
15. [Bot Telegram](#15-bot-telegram)
16. [Notifications](#16-notifications)
17. [Personnalisation de l'interface](#17-personnalisation-de-linterface)
18. [Administration](#18-administration)
19. [Sécurité](#19-sécurité)
20. [Ce que MailIA ne fait pas (limites actuelles)](#20-ce-que-mailia-ne-fait-pas-limites-actuelles)

---

## 1. Vue d'ensemble

MailIA est un client email web multi-comptes (IMAP/SMTP) avec un assistant IA intégré capable
de lire, organiser, rédiger et automatiser le traitement des emails. L'application se compose
d'une interface web (SPA), d'une API, d'un moteur de synchronisation en arrière-plan, d'un
moteur de recherche (Elasticsearch), d'un bot Telegram et d'un serveur d'outils IA (MCP).

## 2. Comptes et authentification

- **Compte utilisateur MailIA** : inscription (réservée à un admin — pas d'auto-inscription
  publique), connexion par email/mot de passe (JWT), mot de passe oublié par email (lien de
  réinitialisation valable 30 minutes, réponse volontairement identique que l'email existe ou
  non pour éviter l'énumération de comptes).
- **Comptes mail multiples** : chaque utilisateur peut connecter plusieurs comptes IMAP/SMTP
  (ex. plusieurs adresses professionnelles). Pour chaque compte : hôte, port et SSL pour l'IMAP
  comme pour le SMTP, identifiants (mot de passe chiffré en base), activation/désactivation de la
  synchronisation.
- **Test de connexion** : test des identifiants avant enregistrement, test IMAP et test SMTP
  indépendants sur un compte existant.
- **Suppression de compte** avec suppression en cascade des données associées.

## 3. Consultation et gestion des emails

- **Explorateur** : arbre des dossiers IMAP (décodage IMAP UTF-7 pour les noms accentués),
  liste des emails par dossier avec pagination, compteurs par dossier (dont non-lus).
- **Lecture d'un email** : affichage du corps HTML dans un cadre isolé (sandbox, sans exécution
  de script), en-têtes (de/à/cc/date/objet), pièces jointes téléchargeables, mode lecture plein
  écran.
- **Filtres de colonnes** : par expéditeur, objet, plage de dates (recherche IMAP côté serveur),
  présence de pièce jointe, email déjà répondu, email suspecté spam. Tri par colonne (de, objet,
  date).
- **Actions sur un email** : répondre, répondre à tous, transférer, marquer lu/non lu, marquer
  important (flag), épingler, déplacer vers un autre dossier du même compte, supprimer.
  (Il n'existe pas de déplacement d'un compte vers un autre.)
- **Actions groupées (sélection multiple)** : marquer lu/non lu, marquer important, déplacer,
  supprimer, marquer comme spam, créer une règle automatique à partir de la sélection.
- **Gestion des dossiers** : créer, renommer, supprimer, vider un dossier IMAP.
- **Export** : export d'un dossier complet au format ZIP. Il n'existe pas d'export des résultats
  de recherche ni d'export d'un email isolé.
- **Raccourcis clavier** : Échap (fermer la modale/le panneau actif), N (nouveau message),
  R (répondre), Suppr (supprimer l'email courant) — inactifs pendant la saisie de texte.

## 4. Recherche

- **Recherche plein texte** (Elasticsearch) : par mot-clé avec filtres combinables (compte,
  dossier, expéditeur, plage de dates, présence de pièce jointe), surlignage des termes trouvés,
  pagination. Le tri est **exclusivement chronologique décroissant** : le score de pertinence
  n'est jamais calculé.
- **Recherche sémantique** : ⚠️ **non fonctionnelle**. L'outil `semantic_search` existe côté MCP,
  mais aucun email n'a jamais d'embedding indexé (le champ n'est pas construit à l'indexation) et
  les dimensions du modèle (384) ne correspondent pas au mapping Elasticsearch (768). Toute requête
  échoue. La rendre opérationnelle suppose de générer les embeddings et de réindexer.
- **Recherche multi-dossiers IMAP** : recherche en direct sur le serveur IMAP à travers plusieurs
  dossiers sélectionnés simultanément (utile quand l'indexation Elasticsearch n'est pas à jour).
- **Recherche croisée** (via l'assistant IA / MCP) : recherche combinée à travers plusieurs
  comptes et dossiers en une seule requête.

## 5. Rédaction et envoi

- **Composition** : éditeur HTML riche (TinyMCE), destinataires à/cc/cci avec autocomplétion sur
  les contacts, pièces jointes.
- **Répondre / répondre à tous / transférer** : citation automatique du message d'origine
  (bloc de citation dédié).
- **Brouillons** : sauvegarde, mise à jour, liste, suppression.
- **Signatures** : voir [section dédiée](#8-signatures).
- **Bulle de chat IA de composition** : assistant conversationnel flottant intégré à la fenêtre
  de composition, capable de poser des questions de clarification avant de générer un texte,
  insère le résultat avant la signature et la citation. Historique conservé par brouillon,
  fenêtre redimensionnable (taille mémorisée).
- **Liens cliquables** : les URLs dans le corps des emails sont détectées et rendues cliquables.

## 6. Stockage local (dossiers hors IMAP)

En plus des dossiers synchronisés avec le serveur IMAP, chaque compte dispose de dossiers
**locaux**, stockés uniquement en base de données PostgreSQL (pas envoyés au serveur mail) :

- Créer/supprimer des dossiers locaux, purge des dossiers locaux vides.
- Lister, lire les emails locaux (identifiants préfixés `L`, ex. `L42`, pour les distinguer des
  UID IMAP).
- Déplacer un email d'IMAP vers le stockage local (retiré du serveur) et inversement (copier un
  email local vers IMAP en le ré-injectant sur le serveur).
- Détection de doublons entre stockage local et IMAP dans les deux sens.
- Statistiques par dossier local.

Usage typique : archivage d'emails sans consommer de quota IMAP, ou zone de travail avant
classement définitif.

## 7. Import d'emails

- **Import mbox / ZIP** : dépôt d'un fichier (upload) ou import depuis un chemin déjà présent
  sur le serveur, import direct vers un compte IMAP ou vers le stockage local.
- **Traitement en arrière-plan** avec suivi de job (statut, progression) et **reprise** en cas
  d'interruption (l'import ne repart pas de zéro).
- **Historique des jobs d'import** consultable.

## 8. Signatures

- Création/modification/suppression de signatures HTML, une signature par défaut possible.
- **Résolution automatique** de la signature à utiliser selon le(s) destinataire(s), par ordre
  de priorité : signature associée au **contact** précis > signature associée au **groupe** de
  contacts > signature **par défaut** de l'utilisateur.
- Sélection manuelle possible pour outrepasser la résolution automatique lors de la composition.

## 9. Contacts et groupes

- Carnet de contacts : création, modification, suppression, plusieurs adresses email par contact.
- Groupes de contacts : création, modification, suppression, ajout/retrait de membres.
- Autocomplétion des destinataires lors de la composition (recherche par nom ou email).
- Un contact peut être créé directement à partir d'un email reçu.

## 10. Détection de spam

- **Scan de spam** sur un ou plusieurs dossiers (ou tous), avec progression en temps réel
  (flux `ndjson`) : analyse des en-têtes standards (`X-Spam-Status`, `X-Spam-Flag`,
  `X-Spam-Score`, `Authentication-Results`, `X-VR-SPAMSCORE`, etc.) pour calculer un score et
  des raisons de suspicion, sans envoyer le contenu à un service tiers.
- **Listes blanche et noire** par compte, par adresse expéditeur : un expéditeur en liste
  blanche n'est jamais classé spam, un expéditeur en liste noire l'est toujours.
- Le score de spam est aussi affiché dans les résultats de recherche multi-dossiers et dans les
  listes d'emails (colonne filtrable).

## 11. Règles automatiques

Deux moteurs de règles distincts, appliqués automatiquement à chaque synchronisation (et
déclenchables manuellement) :

- **Règles classiques** (conditions/actions structurées, sans IA) : conditions sur expéditeur,
  destinataire, objet, présence de pièce jointe, score de spam, statut de réponse, taille,
  âge/date de l'email. Actions possibles : déplacer, marquer lu, marquer important, marquer
  spam, supprimer, transférer. ⚠️ Il n'existe **pas** d'aperçu listant les emails qui seraient
  concernés : appliquer une règle classique exécute directement les actions.
- **Règles IA** : rédigées en langage naturel (Markdown), interprétées par le fournisseur IA
  choisi pour décider de l'action à appliquer — utile pour des critères non exprimables par de
  simples conditions (ex. "classer comme urgent les emails qui parlent d'un incident client").
- Chaque règle a une **priorité**, peut être activée/désactivée, et peut déclencher une
  **notification Telegram** lorsqu'elle s'applique.
- Création rapide d'une règle depuis une sélection d'emails dans l'explorateur.

## 12. Synchronisation automatique

- Un service de fond (Celery) est planifié pour synchroniser **tous les comptes actifs toutes les
  5 minutes** :
  récupère les nouveaux emails de chaque dossier IMAP (par lots, avec suivi du dernier UID par
  dossier pour ne récupérer que les nouveautés), les indexe dans Elasticsearch, puis applique
  les règles IA actives et les règles classiques actives.
- Synchronisation manuelle immédiate possible pour un compte donné (bouton dans l'interface ou
  outil IA `trigger_sync`).
- Seules les actions issues des **règles IA** sont journalisées dans les logs de traitement.
  Les règles classiques et les synchronisations ne journalisent rien.

## 13. Assistant IA

- **Fournisseurs IA configurables** : Claude (API Anthropic), Claude Native (CLI Claude Code en
  sandbox, sans clé API), OpenAI/compatible OpenAI, Ollama (modèle local), pont local via
  WebSocket (`/ai-bridge`, permet d'utiliser un LLM tournant sur la machine de l'utilisateur).
  Plusieurs fournisseurs peuvent être configurés, un par défaut.
- **Chat conversationnel** : plusieurs conversations en parallèle (onglets), réponse en
  streaming (SSE), historique par conversation.
- **Accès complet aux emails via des outils** (protocole MCP, ~74 outils) organisés par
  catégorie : recherche (texte, sémantique, multi-dossiers, statistiques par dossier/expéditeur,
  analytique), lecture (email, fil de discussion, pièces jointes, en-têtes, brouillons),
  actions (déplacer, marquer, supprimer, archiver, créer/renommer/supprimer un dossier — avec
  variantes en masse), envoi (envoyer, répondre, transférer, gérer les brouillons), IA
  spécialisée (résumer un email/fil, classifier, extraire une information, poser une question
  sur les emails), règles (lister, créer, prévisualiser, déclencher une synchronisation),
  administration (comptes, statut de synchronisation, logs, test SMTP), et un jeu d'outils
  dédié au **stockage local** (lister, lire, déplacer, détecter les doublons, copier vers IMAP).
- **Liens cliquables générés par l'IA** : quand l'assistant référence un email ou une pièce
  jointe dans sa réponse, il insère un marqueur qui devient un lien cliquable dans l'interface
  (ouverture directe de l'email ou téléchargement de la pièce jointe).
- **Listes d'emails formatées** : présentées systématiquement en tableau (date/objet/dossier/
  lien), limitées à 20 lignes affichées avec indication du total si plus de résultats existent.
- **Plans de tâches multi-étapes** : pour une demande complexe (ex. "trie tous mes emails de
  2023"), l'assistant propose un plan à cocher, exécute une étape à la fois, affiche la
  progression, et peut reprendre un plan interrompu.
- **Activité des outils en temps réel** : pendant que l'IA travaille, un panneau affiche les
  outils en cours d'exécution (nom, statut, temps écoulé).
- **Bouton d'arrêt** : interruption d'une réponse en cours de génération, avec conservation de
  l'historique partiel.
- **Reprise automatique** : si une tâche multi-étapes semble bloquée sans progression,
  l'assistant relance automatiquement jusqu'à 3 fois avant d'abandonner.
- **Bulle de chat contextuelle** flottante disponible directement dans l'explorateur d'emails
  (indépendante du chat principal), et une variante dédiée à la composition (voir section 5).

## 14. Digest hebdomadaire

Accessible dans l'onglet Dashboard de l'interface (généré à la demande, **non envoyé par
email**) : l'IA analyse jusqu'à 500 emails des N derniers jours (7 par défaut, 30 maximum) et
produit un rapport structuré comprenant :

- Actions en attente et leur niveau d'urgence
- Relances à faire (emails envoyés sans réponse)
- Engagements pris et leur échéance
- Tâches détectées automatiquement dans les emails
- Synthèse de la semaine (résumé libre)
- Principaux fils de discussion avec résumé de chacun
- Nouveaux contacts détectés (non présents dans le carnet)
- Alertes (ton négatif détecté, échéance manquée, élément important manqué)
- Analytique : volumes reçus/envoyés, jour le plus chargé, moyenne quotidienne, top
  correspondants, répartition par catégorie, observations sur les délais de réponse
- Suggestions de regroupement de nouveaux contacts en groupes (par domaine ou type d'échange)

Le résultat est mis en cache (1 heure) pour éviter de renvoyer la même analyse à chaque
ouverture du tableau de bord.

## 15. Bot Telegram

- `/start` : aide.
- `/link <email>` : associe le chat Telegram au compte MailIA de l'utilisateur.
- `/search <requête>` : recherche plein texte (5 résultats) dans les emails indexés.
- `/ask <question>` : répond à une question sur les emails de l'utilisateur (recherche
  Elasticsearch puis question posée au fournisseur IA de l'utilisateur avec le contexte trouvé).
- `/status` : affiche le compte MailIA actuellement lié.
- Un message libre (hors commande) est automatiquement routé vers `/ask` s'il ressemble à une
  question, sinon vers `/search`.
- Reçoit aussi les **notifications push** déclenchées par les règles automatiques (voir
  section suivante).

## 16. Notifications

Le seul canal de notification proactive actuel est **Telegram**, déclenché quand une règle
automatique configurée avec l'option "notifier" s'applique à un email. Il n'existe pas
aujourd'hui de notification par email ni de notification push navigateur — le digest
hebdomadaire, en particulier, est consulté à la demande et n'est pas poussé à l'utilisateur.

## 17. Personnalisation de l'interface

- **Thèmes** : clair, sombre, violet, nord, dracula, emerald, lemon, ocean, rose.
- **Réglages d'affichage** : mode lecture, fond de l'email, largeur de mise en page.
- Préférences mémorisées par utilisateur (localStorage / paramètres utilisateur).

## 18. Administration

Réservé aux utilisateurs administrateurs :

- **Paramètres système** : clés API (Anthropic, OpenAI), token du bot Telegram (valeurs
  chiffrées, affichées masquées), fournisseur et modèle IA par défaut, nom de l'application,
  nombre maximum d'utilisateurs autorisés.
- **Gestion des utilisateurs** : activer/désactiver un compte, promouvoir/rétrograder un
  administrateur (un admin ne peut pas retirer ses propres droits admin).
- **Tableau de statut système** : état de synchronisation par compte et par dossier (comparaison
  entre nombre d'emails côté IMAP et nombre indexé dans Elasticsearch), santé du cluster
  Elasticsearch (statut, taille, nombre de documents), état du worker Celery (tâches actives,
  planifiées, enregistrées), 50 derniers logs de traitement.
- **Diagnostic des fournisseurs IA** : test de connexion à un fournisseur configuré.

## 19. Sécurité

- Authentification par JWT sur tous les points d'entrée de l'API (sauf santé, connexion,
  mot de passe oublié/réinitialisation).
- Limitation de débit (Redis) : 5 tentatives/min sur la connexion, 3/min sur l'inscription,
  3 tentatives/5 min sur la demande de réinitialisation de mot de passe.
- En-têtes de sécurité systématiques (HSTS, anti-clickjacking, anti-sniffing MIME,
  anti-XSS, politique de référent, politique de permissions).
- Corps HTML des emails affiché dans un cadre isolé sans exécution de script possible.
- Mots de passe des comptes mail chiffrés en base (jamais en clair).
- Chaque compte mail et chaque donnée associée est rattaché à son propriétaire, vérifié à
  l'entrée de chaque endpoint et par jointure sur les objets du stockage local.
  *Historique : une faille de cloisonnement (IDOR) affectait 7 endpoints du stockage local
  jusqu'au 2026-08-22 ; elle a été corrigée et re-vérifiée.*

## 20. Ce que MailIA ne fait pas (limites actuelles)

Pour éviter toute ambiguïté, vérifié par test au 2026-08-22 :

- Pas d'auto-inscription publique (l'inscription nécessite un jeton admin).
- **Recherche sémantique inopérante** : l'outil existe mais aucun email n'a d'embedding indexé.
- **Pas de tri par pertinence** dans la recherche : uniquement chronologique décroissant.
- **Pas d'aperçu des règles** avant application : les actions s'exécutent directement.
- **Pas de déplacement d'emails entre comptes**, ni d'export des résultats de recherche ou d'un
  email isolé (seul l'export d'un dossier complet existe).
- **Gestion des brouillons partielle côté application** : seule la sauvegarde a un endpoint API ;
  lister, modifier et supprimer un brouillon ne sont accessibles qu'aux outils MCP de l'assistant IA.
- **Journalisation partielle** : seules les actions des règles IA sont tracées.
- Pas d'envoi du digest hebdomadaire par email — consultation à la demande uniquement.
- Pas de notification push navigateur ni de notification par email sur événement (nouvel
  email, règle appliquée) — seul Telegram est câblé pour cela.
- Pas d'authentification à deux facteurs (2FA).
- Pas de calendrier propre — seule une extraction d'événements *depuis* le contenu des emails
  existe côté outils IA (`extract_calendar_events`), sans calendrier intégré pour les gérer.

---

*Document initialement produit par analyse du code source, puis corrigé après une campagne de
test ayant couvert l'API, les 67 outils MCP et l'interface. Les écarts constatés entre le code et
son comportement réel ont été intégrés. En cas de doute, se reporter au
[Guide Développeur](GUIDE-DEVELOPPEUR.md).*

# Guide Developpeur — MailIA

> **MailIA** est un assistant email intelligent qui indexe, trie et analyse vos emails grace a l'intelligence artificielle.
> Ce guide est destine aux debutants : chaque concept est explique de zero.

---

## Table des matieres

1. [Vue d'ensemble du projet](#1-vue-densemble-du-projet)
2. [Architecture](#2-architecture)
3. [Demarrage rapide](#3-demarrage-rapide)
4. [Workflow de developpement](#4-workflow-de-developpement)
5. [Visite guidee du code](#5-visite-guidee-du-code)
6. [Patterns recurrents](#6-patterns-recurrents)
7. [Depannage](#7-depannage)
8. [Concepts cles](#8-concepts-cles)
9. [Guide de maintenance](#9-guide-de-maintenance)
10. [Ressources externes](#10-ressources-externes)

---

## 1. Vue d'ensemble du projet

### Ce que fait MailIA

MailIA est une application web auto-hebergee qui se connecte a vos comptes email via le protocole IMAP (le protocole standard pour lire les emails a distance), indexe tous vos messages dans un moteur de recherche (Elasticsearch), puis applique des regles de tri intelligentes grace a l'IA (Claude, OpenAI ou un modele local via Ollama).

En resume : vous connectez vos boites mail, ecrivez des regles en francais dans un format Markdown (un langage de balisage simple), et MailIA trie automatiquement vos emails, les deplace dans les bons dossiers, et vous notifie sur Telegram si besoin.

### A qui il s'adresse

- Utilisateurs avances qui gerent plusieurs comptes email et veulent automatiser le tri
- Entreprises qui souhaitent un outil interne de gestion d'emails avec IA

### Technologies principales

| Technologie | Version | Role |
|---|---|---|
| Python | 3.12 | Langage principal |
| FastAPI | 0.115 | Framework web (le serveur qui repond aux requetes HTTP) |
| SQLAlchemy | 2.0 | ORM — couche d'abstraction pour la base de donnees (permet d'ecrire du Python au lieu du SQL) |
| PostgreSQL | 16 | Base de donnees relationnelle (stocke les utilisateurs, comptes, regles) |
| Elasticsearch | 8.13 | Moteur de recherche full-text (indexe et recherche dans les emails) |
| Celery | 5.4 | Gestionnaire de taches en arriere-plan (synchronise les emails periodiquement) |
| Redis | 7 | File d'attente pour Celery et cache |
| Docker Compose | 3.9 | Orchestration de conteneurs (lance tous les services ensemble) |
| Anthropic SDK | 0.40 | Client API pour Claude (IA d'Anthropic) |
| OpenAI SDK | 1.58 | Client API pour GPT (IA d'OpenAI) |
| python-telegram-bot | 21.9 | Bot Telegram pour les notifications et la recherche |
| Nginx | - | Reverse proxy (intermediaire entre Internet et l'application) |
| Apache Tika | latest | Extraction de texte depuis les pieces jointes (PDF, Word, etc.) |

### Etat du projet

Projet actif, en developpement. Deploye en production sur `mailia.expert-presta.com`.

---

## 2. Architecture

### 2.1 Vue d'ensemble de l'architecture

MailIA suit une architecture **orientee services** : plusieurs processus independants collaborent via une base de donnees partagee et une file de messages (Redis).

```mermaid
graph TD
    USER["Utilisateur / Navigateur"]
    TG["Bot Telegram"]
    NGINX["Nginx (reverse proxy)"]
    API["API FastAPI"]
    WORKER["Celery Worker"]
    BEAT["Celery Beat (planificateur)"]
    PG["PostgreSQL"]
    ES["Elasticsearch"]
    REDIS["Redis"]
    IMAP["Serveur IMAP (OVH, Gmail...)"]
    AI["Fournisseur IA (Claude / OpenAI / Ollama)"]
    TIKA["Apache Tika"]

    USER -->|HTTPS| NGINX
    NGINX -->|proxy_pass :8100| API
    NGINX -->|WebSocket| API
    TG -->|Polling| API

    API -->|async| PG
    API -->|async| ES
    API -->|JWT auth| USER

    BEAT -->|planifie toutes les 5 min| REDIS
    REDIS -->|tache sync| WORKER
    WORKER -->|IMAP| IMAP
    WORKER -->|indexe| ES
    WORKER -->|persiste| PG
    WORKER -->|evalue regles| AI
    WORKER -->|notifie| TG

    API -->|chat IA| AI
    TIKA -.->|extraction PJ| WORKER
```

| Composant | Role | Technologies | Fichiers cles |
|---|---|---|---|
| Nginx | Recoit les requetes HTTPS, les redirige vers l'API. Gere le SSL et les WebSockets. | Nginx, Let's Encrypt | `docker/nginx-mailia.conf` |
| API FastAPI | Serveur web principal. Gere l'authentification, les comptes mail, la recherche, les regles IA, le chat IA et l'administration. | FastAPI, Uvicorn, SQLAlchemy | `src/api/main.py`, `src/api/routes/` |
| Celery Worker | Processus en arriere-plan qui synchronise les emails depuis les serveurs IMAP, les indexe dans Elasticsearch et applique les regles IA. | Celery, imaplib | `src/worker/tasks.py`, `src/worker/app.py` |
| Celery Beat | Planificateur qui declenche la synchronisation toutes les 5 minutes. | Celery Beat | `src/worker/app.py:15-19` |
| PostgreSQL | Stocke les donnees structurees : utilisateurs, comptes mail, regles, logs de traitement. | PostgreSQL 16 | `src/db/models.py` |
| Elasticsearch | Indexe le contenu des emails pour la recherche full-text et la recherche semantique (par vecteurs). | Elasticsearch 8.13 | `src/search/indexer.py` |
| Redis | Sert de broker (intermediaire de messages) entre Beat, Worker et l'API. | Redis 7 | Configuration dans `src/config.py` |
| Bot Telegram | Permet de rechercher ses emails et poser des questions a l'IA depuis Telegram. Recoit aussi les notifications. | python-telegram-bot | `src/telegram_bot/main.py` |
| Fournisseur IA | Evalue les regles complexes (detection de spam, urgence...) et repond aux questions des utilisateurs. | Anthropic, OpenAI, Ollama | `src/ai/` |
| Apache Tika | Extrait le texte des pieces jointes (PDF, Word, Excel) pour l'indexation. | Apache Tika | Configure dans `docker-compose.yml` |

### 2.2 Couches de l'application

#### Couche Presentation / API

La couche API recoit les requetes HTTP des utilisateurs et renvoie des reponses JSON.

```mermaid
graph LR
    REQ["Requete HTTP"]
    CORS["Middleware CORS"]
    AUTH["Middleware Auth (JWT)"]
    ROUTE["Route / Endpoint"]
    PYDANTIC["Validation Pydantic"]
    DB["Couche Donnees"]

    REQ --> CORS --> AUTH --> ROUTE --> PYDANTIC --> DB
```

| Composant | Role | Fichier |
|---|---|---|
| Middleware CORS | Autorise les requetes depuis le domaine de l'application et localhost | `src/api/main.py:12-18` |
| Middleware Auth (JWT) | Verifie le token JWT (jeton d'authentification) dans l'en-tete `Authorization` | `src/api/deps.py` |
| Routes | Chaque fichier dans `src/api/routes/` gere un groupe d'endpoints | `src/api/routes/*.py` |
| Validation Pydantic | Les modeles Pydantic valident automatiquement les donnees entrantes | Definis dans chaque fichier de route |

**Responsabilites** :
- Recevoir et valider les requetes HTTP
- Authentifier les utilisateurs via JWT
- Deleguer le traitement aux couches inferieures
- Formater et renvoyer les reponses JSON

**Ne fait PAS** :
- Ne contient pas de logique metier complexe
- N'accede pas directement au systeme de fichiers ni a IMAP

**Routes disponibles** :

| Prefixe | Module | Endpoints principaux |
|---|---|---|
| `/api/auth` | `auth.py` | `POST /register`, `POST /login`, `GET /me` |
| `/api/accounts` | `accounts.py` | CRUD comptes mail, liste dossiers, lecture/envoi email, gestion drapeaux |
| `/api/search` | `search.py` | `GET /` — recherche full-text dans les emails indexes |
| `/api/rules` | `rules.py` | CRUD regles IA, preview des regles parsees |
| `/api/ai` | `ai.py` | CRUD providers IA, `POST /chat` — chat avec l'IA |
| `/api/admin` | `admin.py` | Parametres systeme, gestion utilisateurs, statut du systeme |
| `/ws/ai-bridge` | `websocket.py` | WebSocket pour le pont IA local (connecte Ollama de votre PC) |
| `/api/health` | `main.py` | Health check — verifie que l'API est en ligne |

#### Couche Logique Metier

Cette couche contient les regles de gestion : le parseur de regles Markdown, le moteur d'evaluation, et le routeur IA.

```mermaid
graph TD
    PARSER["Parseur Markdown"]
    ENGINE["Moteur de regles"]
    ROUTER["Routeur IA"]
    PROVIDERS["Fournisseurs IA"]

    PARSER -->|ParsedRule| ENGINE
    ENGINE -->|si needs_ai| ROUTER
    ROUTER --> PROVIDERS
```

| Composant | Role | Fichier |
|---|---|---|
| Parseur Markdown | Transforme les regles ecrites en Markdown en objets `ParsedRule` exploitables par le moteur | `src/rules/parser.py` |
| Moteur de regles | Evalue chaque regle contre un email : d'abord les patterns simples (expediteur, sujet, mots-cles), puis l'IA si necessaire | `src/rules/engine.py` |
| Routeur IA | Determine quel fournisseur IA utiliser (provider de l'utilisateur > provider systeme > fallback) | `src/ai/router.py` |
| Fournisseurs IA | Implementations concretes pour Claude, OpenAI, Ollama et le pont local WebSocket | `src/ai/providers/` |

**Responsabilites** :
- Parser les regles Markdown en structures exploitables
- Evaluer les regles (pattern matching + IA) contre les emails
- Router les appels IA vers le bon fournisseur
- Classifier, resumer et extraire des informations des emails

**Ne fait PAS** :
- N'accede pas directement a la base de donnees
- Ne gere pas l'authentification

#### Couche Acces aux Donnees

```mermaid
graph LR
    MODELS["Modeles SQLAlchemy"]
    SESSION["Session Async"]
    ENGINE["AsyncEngine (asyncpg)"]
    PG["PostgreSQL"]
    ES_CLIENT["Client Elasticsearch"]
    ES["Elasticsearch"]

    MODELS --> SESSION --> ENGINE --> PG
    ES_CLIENT --> ES
```

| Composant | Role | Fichier |
|---|---|---|
| Modeles SQLAlchemy | Definissent la structure des tables (User, MailAccount, AIRule, etc.) | `src/db/models.py` |
| Session async | Gere les connexions a PostgreSQL de facon asynchrone | `src/db/session.py` |
| Client Elasticsearch | Indexe et recherche les emails | `src/search/indexer.py` |

#### Couche Infrastructure

```mermaid
graph LR
    CONFIG["Configuration (Pydantic Settings)"]
    SECURITY["Securite (JWT + Fernet)"]
    IMAP_MGR["IMAP Manager"]
    WORKER_SYS["Systeme Worker (Celery)"]
    TELEGRAM["Bot Telegram"]

    CONFIG --> SECURITY
    CONFIG --> IMAP_MGR
    CONFIG --> WORKER_SYS
    CONFIG --> TELEGRAM
```

| Composant | Role | Fichier |
|---|---|---|
| Configuration | Charge les variables d'environnement via Pydantic Settings. Cache le resultat avec `@lru_cache`. | `src/config.py` |
| Securite | Hachage bcrypt des mots de passe, generation/verification JWT, chiffrement Fernet des secrets (mots de passe IMAP, cles API) | `src/security.py` |
| IMAP Manager | Gere toutes les operations IMAP : connexion, liste des dossiers, lecture/deplacement/suppression d'emails | `src/imap/manager.py` |
| Systeme Worker | Configuration Celery : broker Redis, planification Beat, taches de synchronisation | `src/worker/app.py`, `src/worker/tasks.py` |

### 2.3 Flux de donnees commentes

#### Flux 1 : Synchronisation automatique des emails

C'est le flux principal de MailIA : toutes les 5 minutes, les emails sont recuperes depuis les serveurs IMAP, indexes dans Elasticsearch, et les regles IA sont appliquees.

```mermaid
sequenceDiagram
    participant BEAT as Celery Beat
    participant REDIS as Redis
    participant WORKER as Celery Worker
    participant PG as PostgreSQL
    participant IMAP as Serveur IMAP
    participant ES as Elasticsearch
    participant AI as Fournisseur IA
    participant TG as Telegram

    BEAT->>REDIS: Publie tache sync_all_accounts (toutes les 5 min)
    REDIS->>WORKER: Distribue la tache
    WORKER->>PG: Charge les comptes avec sync_enabled=True
    loop Pour chaque compte
        WORKER->>IMAP: Connexion SSL + login
        WORKER->>IMAP: list_folders()
        loop Pour chaque dossier
            WORKER->>IMAP: get_uids(folder, since_uid=last_uid)
            loop Par lot de 50 UIDs
                WORKER->>IMAP: fetch_email(uid, folder)
                WORKER->>ES: bulk_index_emails(batch)
                WORKER->>AI: evaluate_rules(email, rules, llm)
                alt Regle correspond
                    WORKER->>IMAP: move_email / flag_email
                    WORKER->>PG: Enregistre ProcessingLog
                    WORKER->>TG: Notification si regle.notify=True
                end
                WORKER->>PG: Met a jour sync_state[folder]=last_uid
            end
        end
        WORKER->>IMAP: Deconnexion
        WORKER->>PG: Met a jour last_sync_at
    end
```

**Pas a pas** :

1. **Celery Beat** publie la tache `sync_all_accounts` dans Redis toutes les 5 minutes (`src/worker/app.py:16-19`)
2. Le **Worker** recupere la tache et cree une session DB independante (`src/worker/tasks.py:34-47`)
3. Il charge tous les comptes avec `sync_enabled=True` (`src/worker/tasks.py:58-61`)
4. Pour chaque compte, il se connecte au serveur IMAP avec les identifiants dechiffres (`src/worker/tasks.py:85-91`)
5. Il cree l'index Elasticsearch s'il n'existe pas (`src/worker/tasks.py:93-94`)
6. Il charge les regles IA actives de l'utilisateur et les parse (`src/worker/tasks.py:97-106`)
7. Pour chaque dossier IMAP, il recupere les UIDs des messages non encore synchronises, en utilisant le `sync_state` (JSON stockant le dernier UID traite par dossier) (`src/worker/tasks.py:134-135`)
8. Les emails sont traites par lots de 50 pour optimiser l'indexation ES (`src/worker/tasks.py:144-146`)
9. Chaque lot est indexe en bulk dans Elasticsearch (`src/worker/tasks.py:157-159`)
10. Les regles sont evaluees : d'abord pattern matching simple, puis IA si necessaire (`src/worker/tasks.py:170-177`)
11. Le `sync_state` est sauvegarde apres chaque lot pour permettre la reprise en cas d'interruption (`src/worker/tasks.py:180`)

#### Flux 2 : Authentification et recherche d'emails

```mermaid
sequenceDiagram
    participant U as Navigateur
    participant API as FastAPI
    participant AUTH as Middleware JWT
    participant ES as Elasticsearch

    U->>API: POST /api/auth/login {email, password}
    API->>API: verify_password(bcrypt)
    API->>U: {access_token: "eyJ..."}
    U->>API: GET /api/search?q=facture (Authorization: Bearer eyJ...)
    API->>AUTH: decode_access_token(token)
    AUTH->>API: User(id=1)
    API->>ES: search_emails(user_id=1, query="facture")
    ES->>API: {hits: [...]}
    API->>U: {total: 42, results: [...]}
```

**Pas a pas** :

1. L'utilisateur envoie ses identifiants a `POST /api/auth/login` (`src/api/routes/auth.py:60-68`)
2. Le mot de passe est verifie avec bcrypt (`src/security.py:12-13`)
3. Un token JWT (valable 24h) est genere et renvoye (`src/security.py:16-21`)
4. Pour la recherche, le token est envoye dans l'en-tete `Authorization: Bearer <token>`
5. Le middleware JWT decode le token et charge l'utilisateur (`src/api/deps.py:14-31`)
6. La recherche est executee dans l'index ES de l'utilisateur (`mailia-{user_id}`) avec isolation totale (`src/search/indexer.py:158`)
7. Les resultats sont renvoyes avec les highlights (extraits pertinents surlignés) (`src/api/routes/search.py:38-72`)

#### Flux 3 : Pont IA local (WebSocket)

Ce flux permet d'utiliser un modele IA local (Ollama sur votre PC) sans exposer de port.

```mermaid
sequenceDiagram
    participant AGENT as local_agent.py (votre PC)
    participant WS as WebSocket /ws/ai-bridge
    participant API as FastAPI
    participant OLLAMA as Ollama (local)

    AGENT->>WS: Connexion + {token: "eyJ..."}
    WS->>API: decode_access_token -> user_id
    WS->>AGENT: {status: "connected", user_id: 1}
    Note over WS: Agent enregistre dans _local_agents

    API->>WS: Requete IA {type: "chat", messages: [...]}
    WS->>AGENT: Forward la requete
    AGENT->>OLLAMA: POST /api/chat
    OLLAMA->>AGENT: Reponse
    AGENT->>WS: {content: "...", model: "mistral"}
    WS->>API: AIResponse
```

**Pas a pas** :

1. Le script `scripts/local_agent.py` se connecte au WebSocket du serveur (`scripts/local_agent.py:48-53`)
2. Il s'authentifie avec un token JWT (`src/api/routes/websocket.py:22-38`)
3. L'agent est enregistre dans le dictionnaire `_local_agents` (`src/ai/providers/local_bridge.py:16-17`)
4. Quand une requete IA arrive, elle est forwardee a l'agent via le WebSocket (`src/ai/providers/local_bridge.py:39-46`)
5. L'agent forward a Ollama local et renvoie la reponse (`scripts/local_agent.py:28-45`)

### 2.4 Modele de donnees

```mermaid
erDiagram
    User ||--o{ MailAccount : possede
    User ||--o{ AIProvider : configure
    User ||--o{ AIRule : definit
    User ||--o{ ProcessingLog : genere
    MailAccount ||--o{ ProcessingLog : concerne
    AIRule ||--o| AIProvider : utilise
    AIRule ||--o{ ProcessingLog : declenche

    User {
        int id PK
        string email UK
        string username UK
        string password_hash
        boolean is_active
        boolean is_admin
        string telegram_chat_id
        json settings
        datetime created_at
    }

    MailAccount {
        int id PK
        int user_id FK
        string name
        string imap_host
        int imap_port
        boolean imap_ssl
        string imap_user
        text imap_password_encrypted
        string smtp_host
        int smtp_port
        boolean smtp_ssl
        string smtp_user
        text smtp_password_encrypted
        boolean sync_enabled
        string last_sync_uid
        datetime last_sync_at
        json sync_state
        datetime created_at
    }

    AIProvider {
        int id PK
        int user_id FK
        string name
        string provider_type
        string endpoint
        text api_key_encrypted
        string model
        boolean is_default
        boolean is_local
        datetime created_at
    }

    AIRule {
        int id PK
        int user_id FK
        string name
        int priority
        boolean is_active
        text rules_markdown
        int ai_provider_id FK
        datetime created_at
        datetime updated_at
    }

    ProcessingLog {
        int id PK
        int user_id FK
        int mail_account_id FK
        string mail_uid
        string folder
        int rule_id FK
        string action_taken
        json action_detail
        text ai_response
        datetime created_at
    }

    SystemSetting {
        int id PK
        string key UK
        text value
        boolean is_encrypted
        string description
        datetime updated_at
    }
```

| Entite | Role metier | Relations cles | Fichier modele |
|---|---|---|---|
| **User** | Utilisateur inscrit. Peut etre admin. | Possede des `MailAccount`, `AIProvider`, `AIRule` | `src/db/models.py:12-28` |
| **MailAccount** | Compte email connecte via IMAP/SMTP. | Appartient a un `User`. Le champ `sync_state` (JSON) stocke `{dossier: dernier_uid}` pour le suivi de synchronisation par dossier. | `src/db/models.py:30-56` |
| **AIProvider** | Configuration d'un fournisseur IA (Claude, OpenAI, Ollama, local). | Appartient a un `User`. La cle API est chiffree avec Fernet. `is_local=True` indique le pont WebSocket. | `src/db/models.py:58-72` |
| **AIRule** | Jeu de regles ecrit en Markdown. Contient les conditions (Si...) et actions (Alors...). | Appartient a un `User`. Peut etre lie a un `AIProvider` specifique. Les regles sont parsees a la volee par `parse_rules_markdown()`. | `src/db/models.py:75-89` |
| **ProcessingLog** | Journal de chaque action executee par le moteur de regles. | Lie a un `User`, `MailAccount` et optionnellement `AIRule`. Stocke l'action (`move`, `flag`, `mark_read`) et le detail en JSON. | `src/db/models.py:103-116` |
| **SystemSetting** | Parametres globaux de l'application (cles API systeme, nom de l'app, provider IA par defaut). | Independant. Les valeurs sensibles sont chiffrees avec Fernet. | `src/db/models.py:92-101` |

**Choix notables** :
- Les mots de passe IMAP et cles API sont **chiffres avec Fernet** (chiffrement symetrique reversible), pas hashes, car ils doivent etre dechiffres pour etre utilises.
- Le `sync_state` est un champ JSON plutot qu'une table separee, pour simplifier le code et eviter les jointures lors de la synchronisation.
- Les IDs sont des entiers auto-incrementes (suffisant pour une app mono-instance).

### 2.5 Integrations externes

| Service | Role | Comment il est appele | Configuration | Fichiers |
|---|---|---|---|---|
| **Serveur IMAP** (OVH, Gmail...) | Source de verite pour les emails | Connexion SSL via `imaplib`, protocole IMAP4rev1 | `imap_host`, `imap_port`, `imap_user`, `imap_password_encrypted` sur `MailAccount` | `src/imap/manager.py` |
| **Serveur SMTP** | Envoi d'emails | Via `smtplib` (dans `accounts.py`) | `smtp_host`, `smtp_port`, `smtp_user`, `smtp_password_encrypted` sur `MailAccount` | `src/api/routes/accounts.py` |
| **API Claude** (Anthropic) | IA pour l'evaluation des regles et le chat | SDK async `anthropic.AsyncAnthropic` | `ANTHROPIC_API_KEY` (env ou SystemSetting) | `src/ai/providers/claude.py` |
| **API OpenAI** | IA alternative | SDK async `openai.AsyncOpenAI` | `OPENAI_API_KEY` (env ou SystemSetting) | `src/ai/providers/openai_provider.py` |
| **Ollama** | IA locale, sans cloud | API REST via `httpx` | Endpoint configurable (defaut: `http://localhost:11434`) | `src/ai/providers/ollama.py` |
| **Telegram Bot API** | Notifications et interface conversationnelle | SDK `python-telegram-bot` en mode polling | `TELEGRAM_BOT_TOKEN` | `src/telegram_bot/main.py` |
| **Apache Tika** | Extraction de texte depuis les pieces jointes | API REST | `TIKA_URL` (defaut: `http://tika:9998`) | Configure dans `docker-compose.yml` |

**Degradation** : si un fournisseur IA est indisponible, le routeur essaie dans l'ordre : provider utilisateur > provider systeme Claude > provider systeme OpenAI. Si Tika est indisponible, les pieces jointes ne sont simplement pas indexees. Si le serveur IMAP est temporairement injoignable, le worker tente une reconnexion automatique.

### 2.6 Decisions d'architecture

#### Decision 1 : Celery + Redis pour les taches en arriere-plan

- **Contexte** : la synchronisation IMAP est lente (des milliers d'emails a telecharger). Elle ne peut pas bloquer l'API.
- **Choix** : Celery avec Redis comme broker, Beat pour la planification periodique (toutes les 5 min).
- **Consequences** : robuste et bien documente, mais ajoute 3 conteneurs Docker (worker, beat, redis). Chaque tache cree son propre event loop asyncio et sa propre connexion DB pour eviter les conflits.
- **Alternative** : `asyncio.create_task()` dans le meme processus FastAPI (plus simple mais pas resilient aux crashes).

#### Decision 2 : Elasticsearch plutot qu'une recherche SQL

- **Contexte** : les utilisateurs veulent une recherche full-text rapide dans des dizaines de milliers d'emails.
- **Choix** : Elasticsearch avec un index par utilisateur (`mailia-{user_id}`), incluant recherche full-text, filtres et recherche vectorielle.
- **Consequences** : recherche tres rapide et puissante, mais ajoute un service gourmand en memoire (512 Mo minimum). L'isolation par index garantit qu'un utilisateur ne peut pas voir les emails d'un autre.
- **Alternative** : `tsvector` de PostgreSQL (plus simple mais moins performant pour de gros volumes).

#### Decision 3 : Regles en Markdown plutot qu'un DSL ou une interface graphique

- **Contexte** : les regles de tri doivent etre flexibles et pouvoir combiner des conditions simples avec des evaluations IA.
- **Choix** : un format Markdown structure (`## Nom`, `- **Si**: ...`, `- **Alors**: ...`) parse a la volee.
- **Consequences** : facile a ecrire et lire pour les humains, versionnable dans git. Mais pas de validation en temps reel dans l'interface et erreurs de syntaxe possibles.
- **Alternative** : interface drag-and-drop (plus accessible mais plus complexe a developper).

#### Decision 4 : Pont WebSocket pour l'IA locale

- **Contexte** : certains utilisateurs veulent utiliser Ollama sur leur PC sans exposer de port.
- **Choix** : un WebSocket persistant (`/ws/ai-bridge`) ou l'agent local se connecte et recoit les requetes IA.
- **Consequences** : fonctionne derriere un NAT sans configuration reseau. Mais necessite que le script local reste en execution.
- **Alternative** : tunnel SSH ou VPN (plus stable mais plus complexe a configurer).

---

## 3. Demarrage rapide

### Prerequis

| Outil | Version minimale | Installation |
|---|---|---|
| Docker | 24+ | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Docker Compose | 2.20+ | Inclus avec Docker Desktop |
| Git | 2.30+ | `apt install git` ou equivalent |

Aucun autre outil n'est necessaire : Python, PostgreSQL, Elasticsearch, Redis sont tous dans les conteneurs Docker.

### Installation pas a pas

1. **Cloner le depot**
   ```bash
   git clone <url-du-depot> /var/www/mailia
   cd /var/www/mailia
   ```

2. **Creer le fichier de configuration**
   ```bash
   cp .env.example .env
   ```

3. **Editer `.env`** — remplacer les valeurs par defaut :
   ```bash
   # Generer une cle Fernet (necessaire pour le chiffrement)
   python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

   # Generer une cle secrete pour JWT
   python3 -c "import secrets; print(secrets.token_hex(32))"
   ```
   Mettre les valeurs generees dans `ENCRYPTION_KEY` et `SECRET_KEY`.
   Changer aussi `POSTGRES_PASSWORD` et `REDIS_PASSWORD`.

4. **Lancer tous les services**
   ```bash
   docker compose up -d --build
   ```

5. **Executer les migrations de base de donnees**
   ```bash
   docker compose exec api alembic upgrade head
   ```

6. **Verifier que tout fonctionne**
   ```bash
   # L'API doit repondre
   curl http://localhost:8100/api/health
   # Attendu : {"status":"ok","app":"MailIA"}

   # Verifier que tous les conteneurs tournent
   docker compose ps
   ```

### Verification detaillee

| Service | Comment verifier | Resultat attendu |
|---|---|---|
| API | `curl http://localhost:8100/api/health` | `{"status":"ok","app":"MailIA"}` |
| PostgreSQL | `docker compose exec postgres pg_isready` | `/var/run/postgresql:5432 - accepting connections` |
| Elasticsearch | `curl http://localhost:9200/_cluster/health` (si port expose) | `{"status":"green"...}` ou `"yellow"` |
| Redis | `docker compose exec redis redis-cli -a <password> ping` | `PONG` |
| Worker | `docker compose logs worker --tail 10` | `celery@... ready.` |
| Beat | `docker compose logs beat --tail 10` | `beat: Starting...` |

### Problemes courants a l'installation

| Erreur | Cause | Solution |
|---|---|---|
| `POSTGRES_PASSWORD: Set POSTGRES_PASSWORD` | Variable manquante dans `.env` | Verifier que `.env` contient `POSTGRES_PASSWORD=...` |
| `connection refused` sur PostgreSQL | Le conteneur n'est pas encore pret | Attendre 10s et reessayer. Docker Compose attend le healthcheck. |
| `Elasticsearch exited with code 137` | Memoire insuffisante (OOM kill) | Augmenter la RAM ou reduire `ES_JAVA_OPTS` dans `docker-compose.yml` |
| `Invalid Fernet key` | `ENCRYPTION_KEY` mal formatee | Regenerer avec `Fernet.generate_key()` — c'est une chaine base64 de 44 caracteres |
| Port 8100 deja utilise | Un autre service ecoute sur ce port | Changer le port dans `docker-compose.yml` (`ports: "127.0.0.1:8101:8000"`) |

---

## 4. Workflow de developpement

### Lancer en local

```bash
# Lancer tous les services en arriere-plan
docker compose up -d

# Suivre les logs en temps reel (tous les services)
docker compose logs -f

# Suivre un service specifique
docker compose logs -f api
docker compose logs -f worker
```

L'API ecoute sur `http://localhost:8100`. Le hot reload n'est **pas** configure par defaut dans Docker. Pour developper avec rechargement automatique :

```bash
# Reconstruire et relancer apres un changement de code
docker compose up -d --build api
docker compose up -d --build worker
```

### Lancer les tests

Il n'y a pas encore de suite de tests formelle dans le projet. Pour tester manuellement :

```bash
# Tester l'API
curl http://localhost:8100/api/health

# Creer un utilisateur
curl -X POST http://localhost:8100/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@test.com","username":"test","password":"test123"}'
```

### Variables d'environnement

| Variable | Obligatoire | Description | Exemple |
|---|---|---|---|
| `POSTGRES_DB` | Oui | Nom de la base de donnees | `mailia` |
| `POSTGRES_USER` | Oui | Utilisateur PostgreSQL | `mailia` |
| `POSTGRES_PASSWORD` | Oui | Mot de passe PostgreSQL | `ChangeMe123!` |
| `DATABASE_URL` | Oui | URL de connexion complete | `postgresql+asyncpg://mailia:pwd@postgres:5432/mailia` |
| `ELASTICSEARCH_URL` | Oui | URL d'Elasticsearch | `http://elasticsearch:9200` |
| `REDIS_PASSWORD` | Oui | Mot de passe Redis | `ChangeMe456!` |
| `REDIS_URL` | Oui | URL Redis (pour les resultats Celery) | `redis://:pwd@redis:6379/0` |
| `CELERY_BROKER_URL` | Oui | URL Redis pour le broker Celery (DB differente) | `redis://:pwd@redis:6379/1` |
| `SECRET_KEY` | Oui | Cle secrete pour signer les tokens JWT | Chaine hex de 64 caracteres |
| `ENCRYPTION_KEY` | Oui | Cle Fernet pour chiffrer les secrets en DB | Generee par `Fernet.generate_key()` |
| `TIKA_URL` | Non | URL du service Tika | `http://tika:9998` |
| `TELEGRAM_BOT_TOKEN` | Non | Token du bot Telegram (via @BotFather) | `123456:ABC-DEF...` |
| `ANTHROPIC_API_KEY` | Non | Cle API Anthropic (Claude) par defaut | `sk-ant-...` |
| `OPENAI_API_KEY` | Non | Cle API OpenAI par defaut | `sk-...` |
| `APP_URL` | Non | URL publique de l'application | `https://mailia.example.com` |
| `APP_NAME` | Non | Nom affiche dans l'interface | `MailIA` |
| `LOG_LEVEL` | Non | Niveau de log | `info` |

### Strategie de branches

Le projet suit un workflow feature-branch :
- `main` : branche de production
- `feature/xxx` : nouvelles fonctionnalites
- `fix/xxx` : corrections de bugs
- `hotfix/xxx` : corrections urgentes en production

---

## 5. Visite guidee du code

### Arborescence

```
/var/www/mailia/
├── docker-compose.yml          # Orchestration de tous les services
├── docker/
│   ├── Dockerfile.api          # Image Docker pour l'API
│   ├── Dockerfile.worker       # Image Docker pour le Worker Celery
│   └── nginx-mailia.conf       # Configuration Nginx (reverse proxy)
├── .env.example                # Template des variables d'environnement
├── requirements.txt            # Dependances Python
├── alembic.ini                 # Configuration Alembic (migrations DB)
├── alembic/
│   ├── env.py                  # Setup des migrations async
│   └── versions/               # Fichiers de migration
├── rules/
│   └── example.md              # Exemple de regles en Markdown
├── scripts/
│   └── local_agent.py          # Agent local pour le pont IA (Ollama)
└── src/
    ├── config.py               # Configuration centralisee (Pydantic Settings)
    ├── security.py             # JWT, bcrypt, chiffrement Fernet
    ├── api/
    │   ├── main.py             # Point d'entree FastAPI, montage des routes
    │   ├── deps.py             # Dependencies d'injection (auth)
    │   └── routes/
    │       ├── auth.py         # Inscription, connexion, profil
    │       ├── accounts.py     # CRUD comptes mail, operations IMAP
    │       ├── search.py       # Recherche Elasticsearch
    │       ├── rules.py        # CRUD regles IA
    │       ├── ai.py           # Providers IA, chat
    │       ├── admin.py        # Administration, statut systeme
    │       └── websocket.py    # Pont WebSocket pour IA locale
    ├── db/
    │   ├── models.py           # Modeles SQLAlchemy (schema DB)
    │   └── session.py          # Moteur async + fabrique de sessions
    ├── ai/
    │   ├── base.py             # Classes abstraites LLMProvider, EmbeddingProvider
    │   ├── router.py           # Routeur IA (choix du provider)
    │   └── providers/
    │       ├── claude.py       # Implementation Claude (Anthropic)
    │       ├── openai_provider.py  # Implementation OpenAI
    │       ├── ollama.py       # Implementation Ollama (local)
    │       └── local_bridge.py # Pont WebSocket vers agent local
    ├── imap/
    │   └── manager.py          # Operations IMAP (connexion, fetch, move, flag)
    ├── rules/
    │   ├── parser.py           # Parseur Markdown -> ParsedRule
    │   └── engine.py           # Moteur d'evaluation des regles
    ├── search/
    │   └── indexer.py          # Indexation et recherche Elasticsearch
    ├── worker/
    │   ├── app.py              # Configuration Celery + Beat
    │   └── tasks.py            # Taches de synchronisation
    ├── telegram_bot/
    │   └── main.py             # Bot Telegram (commandes, notifications)
    ├── mbox/
    │   └── importer.py         # Import de fichiers mbox (Thunderbird)
    └── web/
        └── static/
            └── index.html      # Interface web (SPA en vanilla JS)
```

### Module par module

#### `src/api/` — API REST

- **But** : expose toutes les fonctionnalites via des endpoints HTTP
- **Fichiers cles** :
  - `main.py` : cree l'application FastAPI, monte les routes et les fichiers statiques
  - `deps.py` : fournit `get_current_user` et `get_current_admin` via l'injection de dependances FastAPI
  - `routes/accounts.py` : le plus gros fichier, gere le CRUD comptes + toutes les operations IMAP (envoi mail, lecture, drapeaux, dossiers)
- **Connexions** : depend de `db`, `search`, `imap`, `ai`, `security`
- **Point d'entree** : commencer par `main.py` puis `routes/auth.py`

#### `src/db/` — Base de donnees

- **But** : definit le schema et gere les connexions PostgreSQL
- **Fichiers cles** :
  - `models.py` : toutes les tables (6 modeles)
  - `session.py` : moteur async + fabrique de sessions (3 lignes de code)
- **Connexions** : utilise par `api`, `worker`, `telegram_bot`
- **Point d'entree** : `models.py`

#### `src/ai/` — Intelligence artificielle

- **But** : abstrait l'acces aux differents fournisseurs IA derriere une interface commune
- **Fichiers cles** :
  - `base.py` : classe abstraite `LLMProvider` avec `chat()`, `classify()`, `summarize()`, `evaluate_rule()`
  - `router.py` : logique de selection du provider (utilisateur > systeme > fallback)
  - `providers/` : 4 implementations concretes
- **Connexions** : utilise par `worker` (regles) et `api` (chat)
- **Point d'entree** : `base.py` pour comprendre l'interface, puis `router.py`

#### `src/imap/` — Operations email

- **But** : encapsule toutes les operations IMAP (le protocole de lecture d'emails)
- **Fichiers cles** :
  - `manager.py` : classe `IMAPManager` avec ~25 methodes (connect, list_folders, fetch, move, flag, delete, etc.)
- **Connexions** : utilise par `worker` (sync) et `api` (operations en temps reel)
- **Point d'entree** : `manager.py`

#### `src/rules/` — Moteur de regles

- **But** : parse les regles Markdown et les evalue contre les emails
- **Fichiers cles** :
  - `parser.py` : transforme le Markdown en `ParsedRule` (condition + actions)
  - `engine.py` : evalue les conditions (pattern matching + IA) et renvoie les correspondances
- **Connexions** : utilise par `worker`. Le parseur est aussi utilise par `api/routes/rules.py` pour la preview.
- **Point d'entree** : `parser.py` puis `engine.py`

#### `src/search/` — Recherche

- **But** : indexe les emails dans Elasticsearch et fournit la recherche full-text et semantique
- **Fichiers cles** :
  - `indexer.py` : creation d'index, indexation individuelle et en bulk, recherche avec filtres, recherche vectorielle
- **Connexions** : utilise par `worker` (indexation) et `api` (recherche)
- **Point d'entree** : `indexer.py`

#### `src/worker/` — Taches en arriere-plan

- **But** : synchronisation periodique des emails, indexation, application des regles
- **Fichiers cles** :
  - `app.py` : configuration Celery, planification Beat
  - `tasks.py` : logique de synchronisation (le fichier le plus complexe du projet)
- **Connexions** : utilise `imap`, `search`, `rules`, `ai`, `db`
- **Point d'entree** : `app.py` puis `tasks.py`

```mermaid
graph TD
    API["api/"] --> DB["db/"]
    API --> SEARCH["search/"]
    API --> AI["ai/"]
    API --> IMAP["imap/"]
    API --> SECURITY["security.py"]
    WORKER["worker/"] --> DB
    WORKER --> SEARCH
    WORKER --> AI
    WORKER --> IMAP
    WORKER --> RULES["rules/"]
    TELEGRAM["telegram_bot/"] --> DB
    TELEGRAM --> SEARCH
    TELEGRAM --> AI
    RULES --> AI
    MBOX["mbox/"] --> SEARCH
    CONFIG["config.py"] -.-> API
    CONFIG -.-> WORKER
    CONFIG -.-> TELEGRAM
    SECURITY -.-> API
    SECURITY -.-> WORKER
```

| Fleche | Signification |
|---|---|
| `api/ -> db/` | L'API lit et ecrit les donnees via SQLAlchemy |
| `worker/ -> imap/` | Le worker se connecte aux serveurs IMAP pour telecharger les emails |
| `worker/ -> search/` | Le worker indexe les emails telecharges dans Elasticsearch |
| `worker/ -> rules/` | Le worker evalue les regles IA sur chaque email |
| `rules/ -> ai/` | Le moteur de regles appelle l'IA pour les conditions complexes |
| `config.py -.-> *` | La configuration est injectee partout (lignes pointillees) |

---

## 6. Patterns recurrents

### Comment ajouter un nouvel endpoint API

1. **Creer ou ouvrir le fichier de route** dans `src/api/routes/`. Suivre le modele de `src/api/routes/rules.py`.
2. **Definir les modeles Pydantic** pour la requete et la reponse (voir `src/api/routes/rules.py:14-38`).
3. **Ecrire la fonction de route** avec les decorateurs FastAPI et les dependances d'injection :
   - `user: User = Depends(get_current_user)` pour les endpoints authentifies
   - `db: AsyncSession = Depends(get_db)` pour l'acces a la base
4. **Enregistrer la route** dans `src/api/main.py:20-26` si c'est un nouveau module :
   ```python
   app.include_router(nouveau.router, prefix="/api/nouveau", tags=["nouveau"])
   ```

### Comment ajouter un nouveau modele / table en base

1. **Definir la classe** dans `src/db/models.py` en suivant le pattern existant (voir `src/db/models.py:75-89` pour `AIRule`).
2. **Creer une migration Alembic** :
   ```bash
   docker compose exec api alembic revision --autogenerate -m "add_new_table"
   docker compose exec api alembic upgrade head
   ```
3. Si la migration automatique ne detecte pas le changement, ecrire le SQL manuellement dans le fichier genere sous `alembic/versions/`.

### Comment ajouter un nouveau fournisseur IA

1. **Creer un fichier** dans `src/ai/providers/`. Suivre le modele de `src/ai/providers/claude.py`.
2. **Heriter de `LLMProvider`** (`src/ai/base.py:19`) et implementer `chat()`.
3. **Enregistrer** le nouveau type dans le routeur `src/ai/router.py:66-92` (ajouter un `if ptype == "nouveau":`)
4. Le type sera disponible via l'API `POST /api/ai/providers` avec `provider_type: "nouveau"`.

### Comment ajouter une nouvelle action de regle

1. **Ajouter le parsing** dans `src/rules/parser.py:132-161` (fonction `_parse_actions`, ajouter un `elif`).
2. **Ajouter l'execution** dans `src/worker/tasks.py:204-232` (fonction `_execute_actions`, ajouter un `elif`).
3. L'action sera automatiquement disponible dans les regles Markdown.

### Comment ajouter un nouveau job worker

1. **Definir la tache Celery** dans `src/worker/tasks.py` en suivant le pattern :
   ```python
   @app.task(name="src.worker.tasks.mon_job")
   def mon_job(arg: int):
       _run_async(_async_mon_job(arg))

   async def _async_mon_job(arg: int):
       async with _worker_session() as db:
           # logique async ici
           pass
   ```
2. **Pour une tache periodique**, ajouter dans `src/worker/app.py:15-21` :
   ```python
   "mon-job": {
       "task": "src.worker.tasks.mon_job",
       "schedule": crontab(minute="*/30"),
   }
   ```
3. **Reconstruire le worker** : `docker compose up -d --build worker beat`

---

## 7. Depannage

| Symptome | Cause probable | Solution |
|---|---|---|
| `asyncpg.InterfaceError: cannot perform operation: another operation is in progress` | Session DB partagee entre plusieurs taches Celery | Utiliser `_worker_session()` (session par tache) au lieu de `async_session` global. Voir `src/worker/tasks.py:34-47`. |
| `imaplib.IMAP4.error: BAD Command Argument Error` sur UNKEYWORD | Le serveur IMAP (ex: OVH) ne supporte pas les flags custom | Utiliser `get_uids(folder, since_uid=...)` avec `sync_state` au lieu de `get_unprocessed_uids()`. |
| `ssl.SSLEOFError: EOF occurred` pendant la sync | Connexion IMAP coupee par le serveur (timeout, instabilite reseau) | Le worker gere ca automatiquement : reconnexion sur `ConnectionError, OSError, imaplib.IMAP4.abort`. Voir `src/worker/tasks.py:182-189`. |
| `elasticsearch.BadRequestError: failed to parse date field` | Format de date invalide (`2025-10-25 10:15` au lieu de `2025-10-25T10:15:00`) | Conversion automatique dans `src/search/indexer.py:83-85`. Si l'index a ete cree avec un mauvais mapping, le supprimer et le receer. |
| `Fielddata is disabled on [folder]` dans les aggregations ES | Le champ `folder` est de type `text` au lieu de `keyword` | Supprimer l'index (`DELETE /mailia-{user_id}`) et laisser le worker le recreer avec le bon mapping. |
| Sync bloquee a un nombre fixe d'emails | L'ancien code prenait les 100 derniers UIDs et marquait tout comme synchronise | Verifier que `uids[:MAX_PER_FOLDER]` est utilise (plus ancien en premier). Reset du `sync_state` en DB si necessaire. |
| `RuntimeError: No AI provider configured` | Pas de cle API configuree | Aller dans l'onglet Admin > Settings et configurer `anthropic_api_key` ou `openai_api_key`. |
| `Invalid Fernet key` | La `ENCRYPTION_KEY` a change ou est incorrecte | Regenerer la cle **et** re-chiffrer tous les secrets en DB (mots de passe IMAP, cles API). |
| Conteneur Elasticsearch OOM killed (code 137) | Pas assez de RAM | Reduire `ES_JAVA_OPTS` a `-Xms256m -Xmx256m` dans `docker-compose.yml:104` |
| Le bot Telegram ne repond pas | Token invalide ou bot pas demarre | Verifier `TELEGRAM_BOT_TOKEN` et que le conteneur `telegram` tourne : `docker compose logs telegram` |
| `docker compose build` ne prend pas les changements | Cache Docker | Utiliser `docker compose build --no-cache api worker` |

---

## 8. Concepts cles

| Terme | Definition |
|---|---|
| **IMAP** | *Internet Message Access Protocol* — protocole standard pour lire les emails sur un serveur distant. Contrairement a POP3, les emails restent sur le serveur. |
| **SMTP** | *Simple Mail Transfer Protocol* — protocole pour envoyer des emails. |
| **UID** | *Unique Identifier* — identifiant numerique unique attribue a chaque email par le serveur IMAP dans un dossier. Permet de reprendre la synchronisation la ou on s'est arrete. |
| **sync_state** | Dictionnaire JSON `{dossier: dernier_uid}` stocke sur chaque `MailAccount`. Permet de savoir quels emails ont deja ete synchronises par dossier. |
| **JWT** | *JSON Web Token* — jeton d'authentification signe. L'utilisateur recoit un token a la connexion et l'envoie dans l'en-tete `Authorization` de chaque requete. Valable 24h. |
| **Fernet** | Algorithme de chiffrement symetrique (de la librairie `cryptography`). Utilise pour chiffrer les mots de passe IMAP et cles API en base de donnees. Reversible (contrairement a bcrypt). |
| **bcrypt** | Algorithme de hachage pour les mots de passe utilisateur. Irreversible — on ne peut que verifier si un mot de passe correspond. |
| **ORM** | *Object-Relational Mapping* — SQLAlchemy traduit les classes Python en tables SQL. Au lieu d'ecrire `SELECT * FROM users`, on ecrit `select(User)`. |
| **Migration** | Modification du schema de base de donnees de maniere versionnee. Alembic genere des scripts Python qui appliquent les changements (ajout de colonne, creation de table, etc.). |
| **Broker** | Intermediaire de messages. Redis sert de broker entre Celery Beat (qui planifie les taches) et le Worker (qui les execute). |
| **Endpoint** | Une URL specifique de l'API qui repond a une methode HTTP. Ex: `GET /api/search` est un endpoint. |
| **Middleware** | Code qui s'execute entre la reception de la requete et le traitement par la route. Ex: le middleware CORS et le middleware d'authentification. |
| **Provider IA** | Configuration d'un fournisseur d'intelligence artificielle : Claude (Anthropic), GPT (OpenAI), Ollama (local) ou le pont WebSocket. |
| **Pont WebSocket** | Connexion persistante entre le serveur MailIA et un script local (`local_agent.py`). Permet d'utiliser Ollama sur votre PC sans ouvrir de port. |
| **Bulk indexing** | Technique d'indexation par lots dans Elasticsearch. Au lieu d'indexer un email a la fois, on en envoie 50 d'un coup pour optimiser les performances. |
| **Index ES** | Dans Elasticsearch, un index est l'equivalent d'une table. Chaque utilisateur a son propre index (`mailia-{user_id}`) pour l'isolation des donnees. |
| **ParsedRule** | Structure de donnees representant une regle parsee : nom, condition (`RuleCondition`), actions (`RuleAction`), et options de notification. |
| **needs_ai** | Drapeau sur une condition de regle indiquant qu'elle necessite une evaluation par l'IA (ex: "detecte un ton urgent"). Sans ce drapeau, seul le pattern matching simple est utilise. |

---

## 9. Guide de maintenance

### Mise a jour des dependances

```bash
# Mettre a jour requirements.txt
# Puis reconstruire les images
docker compose build --no-cache api worker
docker compose up -d
```

**Attention** : tester localement avant de deployer en production. Les mises a jour de `sqlalchemy`, `elasticsearch` et `celery` peuvent contenir des changements incompatibles.

### Migrations de base de donnees

```bash
# Generer une migration automatique
docker compose exec api alembic revision --autogenerate -m "description_du_changement"

# Appliquer les migrations
docker compose exec api alembic upgrade head

# Voir l'historique
docker compose exec api alembic history

# Revenir en arriere
docker compose exec api alembic downgrade -1
```

Pour les changements non detectes par Alembic (ex: ajout d'un index, changement de type JSON), ecrire le SQL manuellement dans le fichier de migration.

### Deploiement en production

Le projet est deploye sur un serveur distant via SSH + Docker Compose :

```bash
# Sur le serveur de production
cd /var/www/mailia
git pull
docker compose up -d --build
docker compose exec api alembic upgrade head
```

La configuration Nginx (`docker/nginx-mailia.conf`) gere :
- HTTPS avec certificats Let's Encrypt
- Proxy vers l'API sur le port 8100
- Support WebSocket pour le pont IA
- Fichiers statiques (interface web)

### Monitoring

- **Logs** : `docker compose logs -f <service>` pour chaque service
- **Statut du systeme** : endpoint `GET /api/admin/status` (admin requis) retourne :
  - Etat du worker Celery (en ligne/hors ligne, taches actives)
  - Statistiques Elasticsearch (nombre de docs, taille des index)
  - Progression de la synchronisation par compte et par dossier (IMAP vs ES)
  - 50 derniers logs de traitement

### Sauvegarde

- **PostgreSQL** :
  ```bash
  docker compose exec postgres pg_dump -U mailia mailia > backup_$(date +%Y%m%d).sql
  ```
- **Elasticsearch** : les donnees peuvent etre re-indexees depuis les serveurs IMAP. Une sauvegarde n'est pas critique mais possible via l'API Snapshot d'Elasticsearch.
- **Volumes Docker** : `postgres_data`, `elastic_data`, `attachments_data` sont des volumes nommes. Les sauvegarder avec `docker volume` ou un outil comme `docker-volume-backup`.

### Performance

- **Goulot d'etranglement principal** : la synchronisation IMAP. Le telechargement de milliers d'emails est lent (~600 docs/min en bulk indexing).
- **Optimisations en place** :
  - Batch de 50 emails par cycle IMAP
  - Bulk indexing Elasticsearch
  - Max 2000 emails par dossier par cycle de sync
  - Sauvegarde du `sync_state` apres chaque batch (reprise en cas de crash)
- **Montee en charge** : pour plus d'utilisateurs, augmenter `--concurrency` du worker Celery et la `pool_size` de la connexion PostgreSQL.

---

## 10. Ressources externes

### Documentation des frameworks

| Technologie | Documentation |
|---|---|
| FastAPI | [fastapi.tiangolo.com](https://fastapi.tiangolo.com) |
| SQLAlchemy 2.0 (async) | [docs.sqlalchemy.org](https://docs.sqlalchemy.org/en/20/) |
| Alembic | [alembic.sqlalchemy.org](https://alembic.sqlalchemy.org) |
| Celery | [docs.celeryq.dev](https://docs.celeryq.dev) |
| Elasticsearch Python | [elasticsearch-py.readthedocs.io](https://elasticsearch-py.readthedocs.io) |
| Anthropic SDK | [docs.anthropic.com](https://docs.anthropic.com) |
| OpenAI SDK | [platform.openai.com/docs](https://platform.openai.com/docs) |
| python-telegram-bot | [python-telegram-bot.readthedocs.io](https://python-telegram-bot.readthedocs.io) |
| Pydantic Settings | [docs.pydantic.dev/latest/concepts/pydantic_settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) |
| Docker Compose | [docs.docker.com/compose](https://docs.docker.com/compose/) |

### References protocoles

| Protocole | Specification |
|---|---|
| IMAP4rev1 | [RFC 3501](https://datatracker.ietf.org/doc/html/rfc3501) |
| JWT | [RFC 7519](https://datatracker.ietf.org/doc/html/rfc7519) |
| SMTP | [RFC 5321](https://datatracker.ietf.org/doc/html/rfc5321) |

### Projet

| Ressource | URL |
|---|---|
| Application en production | `https://mailia.expert-presta.com` |
| Interface web | `https://mailia.expert-presta.com/static/index.html` |
| Health check | `https://mailia.expert-presta.com/api/health` |

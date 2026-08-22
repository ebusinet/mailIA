# Campagne de test MailIA — synthèse

**Date** : 2026-08-22 (nuit du 21 au 22)
**Méthode** : trois agents — un testeur, un correcteur, un chef de projet vérifiant et orchestrant
**Périmètre** : API REST, 67 outils MCP, interface web, moteur de synchronisation

---

## 1. Ce qu'il faut retenir

Environ **60 défauts** ont été trouvés et corrigés, dont **trois failles de sécurité** et
**trois bugs de perte ou de corruption de données**. Le plus grave n'était pas dans le code
applicatif : 26 940 tâches de synchronisation étaient armées dans Redis, prêtes à s'exécuter sur le
compte professionnel au premier démarrage du worker.

Le projet **n'avait aucun test automatisé**. C'est la cause profonde : aucun des défauts majeurs
n'aurait survécu à une suite de tests, même minimale. Une suite de non-régression a donc été
construite et livrée dans `tests/`.

### Aucune donnée du compte professionnel n'a été altérée

Le compte `pro OVH` (`e.pimienta@ebusinet.fr`) a été relevé **cinq fois** pendant la campagne :

| Indicateur | Au départ | À la fin |
|---|---|---|
| Dossiers IMAP | 216 | 216 |
| Emails locaux | 1227 | 1227 |
| `last_sync_at` | 2026-05-20 10:05:51 | 2026-05-20 10:05:51 |

Seule variation sur l'ensemble de la campagne : `INBOX` passée de 7012 à 7015 messages, soit trois
emails entrants arrivés naturellement en trois heures et demie. **Aucun autre dossier n'a bougé
d'une seule unité.** Si une règle automatique s'était appliquée, INBOX aurait au contraire diminué
au profit d'un autre dossier.

Deux accès au compte réel ont eu lieu, tous deux sans effet et signalés ici par transparence :
- l'endpoint d'administration `/api/admin/status` ouvre une connexion IMAP **en lecture seule**
  (commande `STATUS`, qui compte les messages) vers tous les comptes de l'instance ;
- une tentative de lecture et de déplacement d'un email local, **refusée par le correctif**, pour
  prouver que la faille de cloisonnement était bien fermée autrement que par lecture de code.

---

## 2. Les failles de sécurité

### IDOR — accès aux emails d'autres utilisateurs (critique)

Tout utilisateur authentifié pouvait **lire, modifier, déplacer, supprimer et télécharger les
pièces jointes** des emails du stockage local de n'importe quel autre utilisateur, en devinant un
identifiant numérique. Les 1227 emails locaux du compte professionnel étaient exposés — y compris à
l'utilisateur non-administrateur `x@x.com`.

Sept endpoints étaient concernés (le rapport initial n'en avait identifié que quatre) : les branches
`storage=local` retournaient toutes **avant** l'appel à la fonction de contrôle de propriété. La
même faille existait dans l'outil MCP `move_local_email`.

Corrigé par une double barrière : validation du compte à l'entrée de chaque endpoint, et résolution
des objets locaux par jointure jusqu'au propriétaire. Vérifié par tentative de contournement réelle.

### XSS stockée, déclenchable par un simple email

Les fragments de surlignage renvoyés par la recherche étaient injectés bruts dans la page. Un email
contenant une charge malveillante l'exécutait dès qu'il apparaissait dans un résultat de recherche.

Point notable : la faille était **masquée** par un autre bug (la recherche plantait dès qu'elle
renvoyait un résultat). La corriger l'aurait réveillée. Les deux ont été traitées ensemble.

Une seconde surface XSS a été fermée dans la foulée : la sortie de l'IA était rendue en HTML sans
assainissement. Un assainisseur par liste blanche a été écrit et validé contre 15 charges, dont des
vecteurs mXSS. Un second agent a cherché à le contourner sans y parvenir.

### `trigger_sync` — déclenchement sur le compte d'autrui

L'outil MCP acceptait un identifiant de compte **jamais validé**, et sans argument appelait la
synchronisation de **tous les comptes de tous les utilisateurs**.

---

## 3. Les bugs de données

### Duplication à chaque déplacement et suppression

Les commandes IMAP `STORE` étaient envoyées sans parenthèses autour des flags, ce qu'un serveur
strict rejette. Conséquence : la copie réussissait, la suppression de l'original échouait,
l'utilisateur voyait une erreur — et l'email existait désormais **en double**.

Parenthéser ne suffisait pas : `imaplib` *lève une exception* sur un rejet au lieu de renvoyer un
statut, donc le contrôle prévu n'était jamais atteint. Le correctif ajoute une annulation de la
copie orpheline, pour qu'un échec futur ne puisse plus jamais dupliquer.

### Perte définitive d'un email sur dossier cible vide

Trouvé par la suite de tests à sa **première exécution**. Un déplacement avec un dossier cible vide
détruit le message : ni dans la source, ni ailleurs sur le serveur, et l'API répond « déplacé avec
succès ».

Cause : le serveur IMAP répond `OK` à une copie vers une destination vide **tout en jetant le
message**. La copie « réussit » donc du point de vue du client, la suppression de l'original
s'exécute, et le courrier disparaît. Aucun contrôle de statut en aval ne pouvait l'intercepter — il
fallait refuser la valeur avant d'émettre la moindre commande. Corrigé sur les six points d'entrée
qui acceptent un nom de dossier.

### Synchronisation morte depuis trois mois

Le moteur de synchronisation était bloqué depuis le **20 mai 2026**. Cause : l'évaluation des règles
IA appelle le fournisseur d'IA **pour chaque email**, avec un délai d'attente de 600 secondes. Le
fournisseur étant en panne, les deux emplacements du pool ont été saturés indéfiniment, pendant que
le planificateur continuait d'empiler des tâches — d'où les 26 940 en attente.

Rien, dans l'interface, ne pouvait le signaler : la demande de synchronisation répondait
« démarrée » même sans worker. C'est corrigé (erreur explicite désormais).

---

## 4. La méthode : ce que la vérification croisée a rapporté

Quatre défauts n'ont existé **que par interaction entre correctifs**, invisibles pour l'agent qui
les avait écrits :

1. Le correctif des flags IMAP oubliait le serveur MCP — trouvé par le chef de projet.
2. Le premier garde-fou du worker transformait un blocage visible en boucle d'échec silencieuse —
   trouvé par le testeur, qui a démoli le correctif du chef de projet sur cinq points.
3. La persistance de progression rendait **définitive** une perte d'email jusque-là réversible —
   trouvé par le correcteur en relisant l'interaction entre son travail et celui du chef de projet.
4. Le garde-fou de la suite de tests vérifiait l'identité par email tandis que les tests
   s'exécutaient sous un identifiant numérique codé en dur, sans que les deux soient confrontés —
   trouvé par le correcteur en relisant le travail du testeur.

Cinq familles de bugs se sont révélées **plus larges que le cas signalé** : IDOR (7 endpoints au
lieu de 4), `smtp_ssl` (6 points d'appel au lieu de 4, dont l'authentification), flags IMAP,
`SELECT` non vérifié (31 sites), imports locaux masquant un nom global. À chaque fois, corriger la
famille plutôt que l'instance a évité une réapparition.

---

## 5. La suite de non-régression

Livrée dans `tests/`. Une commande : `python3 tests/run.py`.

- **77 PASS, 0 FAIL, 1 SKIP** (137 s) — résultat constaté par exécution directe, après correction
  du bug de perte d'email que la suite avait elle-même découvert
- Le SKIP est honnête et documenté : il porte sur une propriété que le worker, volontairement
  arrêté, rend inobservable
- **Bibliothèque standard uniquement** : `httpx` n'est installé que dans l'image Docker, `pytest`
  nulle part. Une suite qu'il faut installer est une suite qu'on ne lance pas.
- Un test par bug de la campagne, référencé par son identifiant d'origine

### Le garde-fou

La suite refuse de démarrer si les comptes accessibles ne sont pas ceux de l'environnement de test.
La vérification porte sur **tous les comptes visibles**, pas seulement la cible : un jeton qui *voit*
un compte de production fait tout échouer. Double barrière (liste blanche + motifs interdits), et le
garde-fou est testé, y compris dans son cas nominal.

Six scénarios d'attaque ont été tentés par un second agent — six refus, avec le message
« aucun test n'a été exécuté, aucune donnée n'a été touchée ».

---

## 6. État du système

| Élément | État |
|---|---|
| `mailia-api`, `mailia-mcp` | reconstruits et démarrés |
| `mailia-worker`, `mailia-beat` | **arrêtés volontairement** |
| File Celery active | **0** |
| Sauvegarde `celery_backup_20260822` | 26 940 tâches, intacte et restaurable |
| `mailia-greenmail` | serveur mail jetable ajouté pour les tests |

Rien n'a été commité : ~60 correctifs sur 19 fichiers, dont trois contenaient déjà des
modifications antérieures à la campagne.

---

---

# Phase 2 — Plans de test complémentaires (22 août, journée)

La première campagne était une chasse aux régressions : elle a corrigé ce qui était cassé. La
seconde a cherché ce que personne n'avait encore regardé. Elle a trouvé **cinq injections de
commande IMAP**, dont une exploitable par un tiers extérieur.

## Ce que le Plan A a trouvé

Le raisonnement était simple : le défaut le plus grave de la première campagne — la destruction
d'un email — avait été trouvé **par accident**, en passant une valeur vide. La famille n'avait
jamais été explorée. Elle l'a été.

| Défaut | Nature | Gravité |
|---|---|---|
| Renommer un dossier nommé `.` ou `..` | détruit toute l'arborescence | destruction de données |
| `uid` acceptant une plage IMAP (`1:*`) | vide un dossier en un appel | destruction de données |
| Injection via les critères et champs de recherche (9 surfaces) | commande arbitraire | sécurité |
| Injection via le drapeau IMAP (4 chemins) | commande arbitraire | sécurité |
| **Injection via le `Message-ID`** | **commande arbitraire, déclenchée par un email reçu** | **sécurité** |
| Lecture de fichier arbitraire (import par chemin) | exfiltration, administrateurs seulement | sécurité |
| Action de règle perdue silencieusement | règle acceptée qui ne fait rien | fonctionnel |
| Recherche par objet accentué | échec systématique (antérieur) | fonctionnel |

**Le plus grave est l'injection par `Message-ID`.** La reconstitution d'un fil de discussion
construit sa requête à partir d'en-têtes **écrits par l'expéditeur**. Il suffit donc d'envoyer un
email pour armer la charge — aucun compte, aucun accès à l'application. Et la fonction concernée
est un outil que l'assistant IA appelle spontanément.

Tous sont corrigés, déployés, et vérifiés par au moins deux mesures indépendantes.

## Ce que le Plan B a démontré, et qui contredit son hypothèse

Le second serveur IMAP (Dovecot) avait été ajouté sur l'idée que GreenMail, permissif, masquait des
défauts qu'un serveur strict révélerait. **C'est l'inverse qui s'est produit, et dans les deux
sens le même jour :**

| Défaut | Serveur permissif | Serveur strict |
|---|---|---|
| Injection `Message-ID` | **prouve** l'exploit | conclut à tort « non exploitable » |
| Recherche accentuée | dément à tort le correctif | **prouve** qu'il fonctionne |

> La valeur d'un second serveur n'est pas qu'il soit plus strict — c'est qu'il soit **différent**.
> On ne teste pas la conformité du serveur, on teste ce que fait l'application quand le serveur ne
> l'aide pas.

**Conséquence opérationnelle** : un test de sécurité qui conclut « aucun effet observé » doit
tourner sur le serveur le plus **permissif** ; un test fonctionnel qui conclut « la requête
fonctionne » sur le plus **strict**. La suite de tests faisait l'inverse par défaut.

## Trois règles qui survivront à ce projet

Les six désaccords de la journée entre les trois intervenants — y compris ceux où le chef de projet
avait tort — ont tous été tranchés par une mesure, jamais par un argument. Ils se ramènent à trois
formules :

- **Une absence d'effet n'est pas un refus.** Une injection qui ne produit rien peut être bloquée
  par accident. Trois occurrences.
- **Une absence de signal n'est pas une absence de contrôle.** Une recherche qui ne trouve pas la
  dépendance attendue ne prouve pas que le contrôle manque. Trois occurrences.
- **Un état observé n'est pas un état courant.** Des données invalides en base peuvent être les
  vestiges d'avant le correctif.

Une quatrième s'y est ajoutée en fin de journée : **vérifier qu'une protection existe n'est pas
vérifier qu'elle est employée** — un test interrogeait la fonction de protection, qui refusait
correctement, pendant que le site d'appel ne l'utilisait pas.

## Un refus qui vaut d'être noté

Pour rendre testable le chemin TLS, un mécanisme installait un certificat de test dans le conteneur
applicatif. Il a été **abandonné** : ce conteneur détient les identifiants IMAP du compte
professionnel, et la clé privée du certificat est versionnée dans le dépôt. Le bénéfice aurait été
de faire passer un test au vert ; le coût, de permettre à quiconque possède cette clé de se faire
passer pour n'importe quel serveur. Le test reste marqué « non exécutable », avec sa raison.

## État de la suite de tests

100 tests au total, dont 22 issus du Plan A. Chaque défaut trouvé a son test.

## 7. Décisions qui vous appartiennent

### 7.1 Redémarrer le worker et le planificateur

Les correctifs anti-blocage sont prêts et déployés dans l'image, mais **le worker n'a pas été
redémarré** : il synchroniserait le compte professionnel et y appliquerait les règles automatiques,
qui déplacent des emails.

Avant de le rallumer, le testeur recommande de **chronométrer une synchronisation complète sans
limite de temps**, puis de calibrer. Motif : 3 mois de retard, 180 dossiers, 45 000 emails, et une
récupération email par email. Toute valeur choisie sans cette mesure est un pari.

Rappel : la file a été mise de côté, pas purgée. Pour la restaurer :
`RENAME celery_backup_20260822 celery`. Pour la supprimer définitivement :
`DEL celery_backup_20260822`. Ces tâches n'ont aucune valeur — ce sont des cycles périmés.

### 7.2 Le curseur de synchronisation : bloquer ou rejouer

Aujourd'hui, un email impossible à récupérer était **silencieusement sauté et perdu**. Le correctif
inverse le compromis : le dossier se bloque, visiblement dans les logs, et aucun courrier n'est
perdu.

L'alternative — faire avancer le curseur en conservant une liste d'identifiants à rejouer — est plus
juste, mais change la structure de `sync_state`. Elle n'a délibérément pas été implémentée.

### 7.3 La recherche sémantique

Elle est exposée comme une fonctionnalité mais **n'a jamais pu fonctionner** : aucun email n'a
d'embedding indexé, et les dimensions du modèle ne correspondent pas au mapping. La rendre réelle
suppose de générer les embeddings et de réindexer 45 000 emails — coût CPU et choix de modèle.

En attendant, l'outil échoue désormais explicitement au lieu de renvoyer une erreur incompréhensible.

### 7.4 Le mail de réinitialisation de mot de passe

L'expéditeur est le premier compte administrateur disposant d'un SMTP — c'est-à-dire, sur cette
instance, **la messagerie professionnelle réelle**. Une demande non authentifiée déclenche donc un
envoi depuis cette adresse. Le comportement n'a pas été modifié : c'est un choix de conception.

### 7.5 Le proxy IA

Votre question initiale. Le conteneur `myopenai-proxy` n'est plus authentifié
(`loggedIn: false`) : les jetons OAuth du CLI Claude Code ont expiré, ce qui fait échouer toute
requête IA. La correction demande une action interactive de votre part :

```bash
ssh expert-presta "docker exec -it myopenai-proxy claude auth login"
```

C'est aussi la cause indirecte du blocage de trois mois de la synchronisation.

---

*Rapports détaillés disponibles dans le répertoire de travail de la session : rapports de test des
trois itérations, rapports de correction des cinq lots, audit du frontend et du worker, audit de
véracité de la documentation, revue de la suite de tests.*

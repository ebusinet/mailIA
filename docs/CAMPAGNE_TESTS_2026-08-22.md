# Campagne de test MailIA — synthèse

**Date** : 2026-08-22 (nuit du 21 au 22)
**Méthode** : trois agents — un testeur, un correcteur, un chef de projet vérifiant et orchestrant
**Périmètre** : API REST, 67 outils MCP, interface web, moteur de synchronisation

---

## 1. Ce qu'il faut retenir

Deux campagnes en deux jours. La première a corrigé **une soixantaine de défauts** et refermé
deux failles de sécurité ; la seconde, qui cherchait ce que personne n'avait encore regardé, en a
trouvé **huit de plus dont cinq injections de commande IMAP**.

Trois choses dominent le reste :

- **Une injection déclenchée par un email reçu.** La seule faille de la campagne qu'un tiers
  extérieur puisse atteindre : ni compte, ni accès, ni manipulation — il suffit d'écrire à la
  victime. Elle n'a été trouvée qu'au second jour, en relisant du travail déjà déclaré terminé.
- **Trois façons de détruire du courrier** sans que rien ne le signale : la duplication à chaque
  déplacement, le dossier cible vide, et la plage d'identifiants qui vide un dossier en un appel.
- **26 940 tâches de synchronisation armées dans Redis**, qui auraient toutes visé le compte
  professionnel au premier démarrage du worker. Ce n'était pas un défaut du code applicatif.

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

## Ce que la suite de tests ne protège pas

Elle compte 100 tests, un par défaut trouvé. Ce qu'elle ne fait pas mérite d'être écrit, parce
qu'une suite dont on croit qu'elle prouve tout est plus dangereuse qu'une suite dont on connaît
les limites.

- **Sur les 14 tests de sécurité et de concurrence, six seulement ont été observés dans l'état
  qu'ils surveillent** — rouges sur le défaut réel, verts après correction. Les huit autres passent
  sans que rien ne prouve qu'ils échoueraient si la propriété se cassait. Deux méritent une
  mention : l'un surveille une protection **positionnelle**, qui tient à l'ordre des vérifications
  et non à un contrôle explicite ; l'autre surveille une propriété fournie par la bibliothèque
  standard, donc restera vert quoi qu'il arrive à l'application.
- **Un seul des tests de la première suite a été observé passant du rouge au vert** sur le correctif attendu. C'est
  la seule preuve qu'un test discrimine. Les autres sont crus, pas prouvés. D'où la règle inscrite
  dans `tests/README.md` : *un test de sécurité doit avoir été vu rouge au moins une fois.*
- **Elle ne détecte pas une exécution concurrente.** Un redéploiement, elle le voit — réponse sans
  corps JSON, conteneur absent, horodatage du build. Mais une seconde suite travaillant sur le même
  compte reste invisible : chaque test opère dans ses propres dossiers, tout passerait, et la
  mesure serait fausse en paraissant bonne. C'est arrivé une fois pendant la campagne. La parade
  est organisationnelle — un seul intervenant mesure à la fois — et non technique.
- **La classe des injections n'est pas close.** Cinq ont été trouvées et fermées ; la dernière est
  sortie d'un endroit qui ne figurait dans aucune des trois listes établies. Dire « la famille est
  fermée » serait la même erreur que celles que la campagne a passé deux jours à corriger.
- **Une divergence reste non mesurée là où elle compte** : la création d'un dossier passe par deux
  chemins qui traitent le séparateur différemment. Sur le serveur de test strict le cas est
  impossible, sur le permissif il est observable — mais le seul serveur qui réunit exactement les
  conditions du compte professionnel est celui qu'on s'interdit de toucher. Un dossier créé depuis
  l'interface et un dossier créé par l'assistant sous le même nom pourraient y être deux dossiers
  distincts. Ni prouvé, ni écarté.

## Le défaut qui a failli être effacé par sa propre détection

Le dernier défaut trouvé — la suppression qui détruit quand il n'y a pas de corbeille — mérite
d'être raconté, parce que sa découverte a tenu à dix minutes.

Trois tests étaient rouges. Le testeur les a analysés, a trouvé dans le code un repli documenté et
intentionnel, a conclu par écrit que **ce n'était pas un défaut produit mais un test mal
conditionné**, et s'apprêtait à « corriger » les tests en créant la corbeille manquante avant leur
exécution. Le rouge aurait disparu, définitivement, sans que personne ne sache pourquoi il avait
existé.

Son diagnostic du raisonnement qui a lâché vaut mieux que le défaut lui-même :

> J'ai cherché à **expliquer** le rouge plutôt qu'à le **valider**. Dès que j'ai eu une explication
> plausible et bénigne, j'ai cessé de chercher. Un échec qu'on sait expliquer n'est pas un échec
> qu'on a compris.

La question qu'il n'a pas posée était la sienne, celle qu'il appliquait depuis deux jours : *ce que
l'API répond correspond-il à ce qu'elle a fait ?* Elle répondait « supprimé » aussi bien pour un
déplacement récupérable que pour une destruction définitive. **Un repli documenté n'est pas un repli
honnête si la réponse ne le distingue pas.**

Deux enseignements en découlent.

**Le premier tient à la forme du test.** Ces tests surveillaient une conservation — *le total ne
doit pas changer* — écrite pour attraper une duplication. Elle a attrapé une perte, que personne ne
cherchait. Un test qui aurait guetté spécifiquement une duplication n'aurait rien vu. Surveiller un
invariant attrape plus que chercher un symptôme.

**Le second tient à qui regarde.** Le testeur avait toutes les informations et la bonne méthode ; il
lui manquait seulement de ne pas être celui qui venait d'écrire l'explication. C'est la même raison
qui a fait trouver quatre autres défauts pendant la campagne : la relecture par un tiers n'ajoute
pas de compétence, elle retire l'attachement à une conclusion déjà formée.

## Un refus qui vaut d'être noté

Pour rendre testable le chemin TLS, un mécanisme installait un certificat de test dans le conteneur
applicatif. Il a été **abandonné** : ce conteneur détient les identifiants IMAP du compte
professionnel, et la clé privée du certificat est versionnée dans le dépôt. Le bénéfice aurait été
de faire passer un test au vert ; le coût, de permettre à quiconque possède cette clé de se faire
passer pour n'importe quel serveur. Le test reste marqué « non exécutable », avec sa raison.

## État de la suite de tests

100 tests, un par défaut trouvé. **Mesure finale sur le build portant tous les correctifs, deux
passages par serveur, identiques test par test :**

| Serveur | Résultat |
|---|---|
| Dovecot (strict) | **87 PASS · 0 FAIL · 12 SKIP** |
| GreenMail (permissif) | **97 PASS · 0 FAIL · 2 SKIP** |

Zéro échec des deux côtés. Les ignorés sont les tests hors profil de serveur, les six chemins
d'envoi bloqués par le certificat auto-signé, et la propriété que le worker arrêté rend
inobservable — chacun avec sa raison affichée, jamais un succès silencieux.

La couverture complète exige les deux exécutions : aucun test n'est ignoré sur les deux serveurs à
la fois, ce que la suite vérifie explicitement.

---

# Phase 3 — Audit de sécurité méthodique (Plan G)

Les sept failles trouvées lors des phases 1 et 2 l'avaient toutes été **au passage**, en testant
autre chose. Aucune ne venait d'une recherche délibérée. Le Plan G est le premier à chercher
méthodiquement — et il a trouvé en quelques heures trois failles dont deux critiques.

| Faille | Nature | État |
|---|---|---|
| Le lien de réinitialisation ouvrait une session **administrateur complète** | critique | fermée |
| La limitation de débit se contournait par un en-tête HTTP | élevée | fermée |
| Un conteneur voisin contournait la limitation en évitant nginx | élevée | fermée |
| **Écriture de fichier arbitraire, en root, par tout utilisateur authentifié** | **critique** | en cours |

## Le lien de réinitialisation était une session administrateur

La fonction qui valide les jetons de réinitialisation vérifiait leur nature ; celle qui valide les
jetons d'accès ne le faisait pas. Les deux sont signés par la même clé. **L'asymétrie est ce qui
trahit l'oubli** : l'auteur savait que ce contrôle comptait, dans un seul sens.

Trois facteurs aggravaient : le jeton voyageait dans une URL — donc historique, journaux, email —
il survivait à son usage, et son émission n'exigeait aucune authentification.

Corrigé par une **séparation positive** : chaque jeton déclare sa nature, et l'absence de
déclaration ne vaut plus autorisation. Conséquence visible : tous les jetons émis avant sont
refusés, les utilisateurs doivent se reconnecter.

## L'écriture de fichier arbitraire

Le nom du fichier téléversé lors d'un import part tel quel vers le système de fichiers. Deux formes
fonctionnent — chemin absolu et remontée par `..` — et le piège du correctif est contre-intuitif :
assembler un chemin absolu à un répertoire **abandonne silencieusement le répertoire**. Interdire
les `..` seuls ne fermerait rien.

Ce qui en fait la faille la plus grave de la campagne : aucun garde administrateur, **conteneur
exécuté en root**, sources de l'application accessibles en écriture. Écraser un fichier Python
donnerait l'exécution de code dans le conteneur qui détient la clé chiffrant les mots de passe IMAP.

Mesuré avec un compte non-administrateur créé pour l'occasion. La chaîne complète a quatre maillons ;
trois sont mesurés, le dernier est déduit et ne sera pas exécuté.

## Ce qui s'est révélé sain, et mesuré plutôt que supposé

Six en-têtes de sécurité corrects sur neuf types de réponse, erreurs comprises. Aucune injection SQL
sur cinq surfaces et onze charges, `pg_sleep` inclus. Élévation de privilège fermée, vérifiée avec un
vrai compte non-administrateur. Aucun oracle distinguant un compte inexistant d'un compte non
possédé. Onze variantes de jeton forgé toutes refusées.

## Deux protections héritées, donc invisibles

L'extraction d'archives ne permet pas d'échapper au répertoire — mais **parce que la bibliothèque
Python réécrit les noms**, pas parce que le code s'en assure. De même, l'API REST résistait à
l'injection IMAP parce qu'un champ retirait les guillemets, sans intention.

Ces protections ne se voient nulle part dans le code, donc rien n'avertit celui qui les retire un
jour pour une raison sans rapport. Deux tests ont été écrits pour chacune : un pour la propriété,
un pour la **forme du code**.

## Une décision d'infrastructure à prendre

Le conteneur applicatif s'exécute en **root**, avec les sources accessibles en écriture. Ce n'est
pas un défaut du code — c'est ce qui transforme une écriture hors périmètre en exécution de code.
Faire tourner le processus sous un utilisateur non privilégié réduirait la gravité de toute une
classe de défauts, connus et à venir.

---

# Phase 4 — Volume et concurrence (Plans F et E)

## Ce que le volume a donné : le chiffre qui débloquait tout

La décision de redémarrer le moteur de synchronisation était bloquée faute d'une mesure. Elle
existe :

| | Avant correctif | Après |
|---|---|---|
| Allers-retours IMAP par email | 2,00 | **1,00** |
| Synchronisation de 7 562 emails | 15,5 s | **3,8 s** |
| Rattrapage complet (45 000 emails) | 48,8 min | **24,5 min** |
| Premier cycle | 24 min 55 s | **12 min 28 s** |

Un des deux allers-retours était inutile : le code redemandait au serveur d'ouvrir le dossier avant
**chaque** message, alors qu'il en enchaîne des centaines dans le même.

Les limites de temps avaient été posées au jugé la première nuit : elles laissaient **cinq secondes
de marge**. Elles reposent désormais sur la mesure, et **la marge vient du travail supprimé plutôt
que du réglage** — le pire cycle occupe le quart du budget au lieu de le dépasser.

Le volume a aussi révélé un blocage : **un seul email au sujet mal encodé arrêtait définitivement la
synchronisation de son dossier**, rejouée à l'identique à chaque cycle. Le message d'erreur
accusait une bibliothèque sans rapport — quelqu'un qui l'aurait suivi n'aurait jamais trouvé.

### Une optimisation construite pour ne pas pouvoir corrompre

Le raccourci évident était de mémoriser le dossier sélectionné. Il a été écarté : **les
identifiants de message sont propres à chaque dossier**, donc une lecture au mauvais endroit
renvoie un message parfaitement valide — et faux. Rien ne le signalerait.

La réutilisation est donc explicite, demandée par le seul appelant qui peut garantir que rien ne
change de dossier pendant qu'il travaille. Prouvé avec deux dossiers contenant chacun un message
portant le même identifiant.

## Ce que la concurrence a donné

Trois défauts, une même cause : **déplacer un email n'est pas une opération atomique** — trois
commandes successives, aucune transaction.

- Deux déplacements simultanés du même message produisent **deux copies**.
- Deux suppressions simultanées, de même.

Et la formulation exacte compte, parce qu'elle est plus sévère que « il existe une course » :

> La duplication n'est pas un aléa rare : c'est **l'issue garantie** dès que deux sessions se
> recouvrent au niveau du protocole. Ce qui est accidentel, c'est qu'on ne l'observe pas par l'API
> — le coût d'établissement des connexions désaligne les requêtes. Une machine plus rapide ou un
> pool de connexions ferait disparaître cette protection fortuite.

La commande atomique du protocole (`UID MOVE`) réduit la fenêtre de trois commandes à une, mais ne
ferme pas la course : **mesurée à 0/6 duplications sur un serveur et 6/6 sur l'autre**. Atomique
comme commande, pas comme transaction — chaque session garde sa vue jusqu'à la notification de
suppression.

### La fenêtre, bornée après quatre modèles dont trois faux

Quatre explications successives ont été proposées pour ce même défaut. **Les trois premières étaient
fausses**, et chacune était cohérente avec les données disponibles au moment où elle a été formulée
— c'est ce qui les rendait crédibles, et trois d'entre elles ont été transmises avant d'être
réfutées.

| Modèle | Sort |
|---|---|
| Les commandes sont désalignées | réfuté |
| La fenêtre est l'intervalle entre l'ouverture du dossier et l'opération | réfuté par un essai direct : une session gardant son instantané **dix secondes** ne duplique pas |
| C'est l'espacement des requêtes qui protège (~200 ms) | réfuté : les envois sont espacés de **0,06 à 0,24 ms**, plus serré que la fenêtre |
| **C'est la variance du préambule de chaque requête** | **mesuré** |

**La fenêtre fait moins d'une milliseconde** — duplication en dessous de 0,5 ms, jamais au-delà
de 1 ms.

Et ce qui protège n'est ni l'espacement ni une marge, mais la **dispersion** introduite par le
préambule de chaque requête — connexion, TLS, authentification, ouverture de session IMAP :

| Clients simultanés | Préambule médian | Écart-type | Écart minimal entre deux voisins |
|---|---|---|---|
| 6 | 12 ms | 5,0 ms | 2,06 ms |
| 12 | 20 ms | 9,0 ms | 1,86 ms |
| **24** | 35 ms | 18,1 ms | **0,93 ms** |
| 48 | 65 ms | 35,1 ms | 1,00 ms |

**À 24 clients simultanés, l'écart minimal touche le bord exact de la fenêtre.** Aucune duplication
n'a été observée sur une centaine de courses, mais la marge réelle est d'un **facteur deux à trois**,
non de trente comme estimé d'abord.

Le point qui rend la situation tenable n'a été vu par personne avant d'être mesuré : **la protection
s'auto-régule.** Plus il y a de clients, plus le préambule ralentit, ce qui ré-étale les requêtes.
L'écart minimal ne s'effondre donc pas sous la charge — il plafonne autour d'une milliseconde.

Ce n'est pas une marge de sécurité. C'est un équilibre que personne n'a conçu.

D'où la formulation exacte du risque, et de son déclencheur : **un pool de connexions ne diviserait
pas la marge, il supprimerait la seule chose qui protège** — le préambule, donc sa variance. Le même
changement casserait aussi la sûreté de la réutilisation de sélection.

**Une réserve à connaître** : le moteur de synchronisation n'a pas ce préambule à chaque opération,
contrairement à l'API. Ce chemin n'est donc **pas couvert** par la protection décrite ici, et il n'a
pas pu être mesuré puisque le worker est arrêté.

## Le contrôle qui a retourné un constat

Trois scénarios échouaient douze fois sur douze. Cette régularité a servi d'alarme plutôt que de
preuve : *une vraie course ne se déclenche presque jamais à tous les coups.*

Un témoin séquentiel — les mêmes opérations, l'une après l'autre — a montré qu'un des trois cas se
comportait déjà ainsi hors concurrence. Le constat a été retiré, avec l'explication laissée dans le
fichier de test pour que personne ne le réécrive.

C'est l'erreur symétrique de celle commise la veille, où un défaut du produit avait été imputé au
test. Même cause dans les deux sens : **conclure d'une observation sans le contrôle qui
l'interprète.**

---

# Ce que la vérification croisée a produit, et qu'aucune rigueur individuelle n'aurait donné

Le dernier défaut de la campagne a donné lieu à **quatre modèles successifs, dont trois faux**. Ils
méritent d'être listés, parce qu'ils disent mieux que le décompte des défauts ce que cette méthode
apporte.

| Modèle | Fenêtre supposée | Ce qui l'a démoli |
|---|---|---|
| 1 — la commande n'est pas atomique | durée de la commande | la commande atomique du protocole duplique quand même |
| 2 — intervalle entre l'ouverture du dossier et l'opération | ~6 ms | une session gardant son instantané **dix secondes** ne duplique pas |
| 3 — les requêtes sont trop espacées | marge ×30 puis ×10 | les envois sont à 0,06 ms, **plus serrés que la fenêtre** |
| 4 — la variance du préambule de chaque requête | **< 1 ms, marge ×2 à 3** | tient |

Les trois faux étaient **cohérents et appuyés sur de vraies mesures**. C'est ce qui les rendait
crédibles, et trois d'entre eux ont été transmis avant d'être réfutés.

**Aucun n'est tombé grâce aux données de son auteur.** Le second par une question venue de l'autre
intervenant, le troisième par une métrique que la mise en garde de l'autre avait fait ajouter, le
premier par une mesure de l'autre. Chacun a été redressé par la règle du voisin.

La conclusion des deux intervenants, formulée séparément et dans les mêmes termes :

> Je n'ai jamais eu, dans mes propres mesures, de quoi me détromper.

Ce n'est ni la prudence ni la compétence individuelle qui a corrigé ces modèles — c'est qu'une
question extérieure soit venue heurter une explication qui, de l'intérieur, ne présentait aucune
faille. C'est le résultat le plus transposable de ces trois jours, et il vaut plus que la liste des
défauts qui précède.

## 7. Décisions qui vous appartiennent

### 7.0 L'angle mort qui grandit : le worker n'a jamais tourné

À mettre avant tout le reste. Depuis deux jours, des correctifs de synchronisation s'accumulent —
persistance de la progression, disjoncteur du fournisseur d'IA, verrou par compte, validation des
identifiants — et **aucun n'a jamais été exécuté une seule fois**. Ils sont écrits, déployés dans
l'image, couverts par un test qui s'annonce lui-même comme non exécutable.

Un test ignoré ne protège rien, et cet angle mort s'élargit à chaque lot. C'est aujourd'hui le
principal écart entre ce qui est corrigé et ce qui est vérifié.

### 7.0bis Où atterrissent vos suppressions ? (question ouverte, réponse gratuite)

Deux dossiers de corbeille coexistent sur le compte professionnel :

| Dossier | Messages |
|---|---|
| `Trash` | **18 992** |
| `Éléments supprimés` | 1 568 |

MailIA cherchait la corbeille dans une liste de noms codés en dur qui contient `Trash` mais **pas**
`Éléments supprimés`. Il déposait donc dans `Trash`. Si votre client de messagerie affiche
`Éléments supprimés` comme corbeille — ce que le nom français sur un serveur OVH rend probable —
alors les emails supprimés depuis MailIA atterrissent depuis toujours dans un dossier que vous ne
regardez jamais. Ce qui expliquerait l'écart entre les deux volumes.

Ce n'est pas un effet du correctif : c'est un défaut antérieur que le correctif **révèle**.
Désormais le drapeau protocolaire `\Trash` prime sur la liste de noms, donc :

- si `Éléments supprimés` porte ce drapeau, vos suppressions y iront — c'est-à-dire là où vous les
  cherchez. Correction d'un défaut ancien, pas régression ;
- sinon, rien ne change et `Trash` reste utilisé. Le défaut ancien subsiste et demande un
  traitement séparé.

**Comment trancher sans rien installer** : supprimer un email depuis MailIA, puis lire les logs.
`move_email` journalise déjà la destination réelle.

```bash
ssh expert-presta "docker logs mailia-api --tail 50 | grep 'Moved UID'"
```

La ligne indiquera le dossier de destination effectif. Une suppression, une commande, et la
question est réglée.

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

## 8. Plan C exécuté — l'interface, testée dans un navigateur

Les cinq plans précédents attaquaient l'API. Ce sixième a piloté Chrome sur l'interface réelle,
en session `qa_bot`. Rapport complet : `campagne-2026-08-22/PLAN_C_INTERFACE_NAVIGATEUR.md`.

**Une alerte antérieure est rectifiée.** La recherche globale renvoie bien la charge XSS brute
dans son JSON — mais le rendu l'échappe : 0 alerte déclenchée, 0 `<img>`, 0 `onerror`, charge
présente en texte littéral. **Non exploitable.** Une charge brute dans une réponse d'API n'est pas
une XSS ; seul le rendu tranche, et seul un navigateur pouvait le dire.

**Trois défauts que seule l'interface pouvait révéler :**

| # | Défaut | Gravité |
|---|---|---|
| IT3-02 | Dashboard entièrement vide sans fournisseur IA — aucun mode dégradé pour les statistiques qui n'en ont pas besoin, message technique anglais, identifiant interne exposé | moyen |
| IT3-03 | Page Statut : `/api/admin/status` met **19,2 s** et renvoie 52 Ko ; les cartes s'affichent vides pendant ce temps, sans indicateur — la page paraît cassée alors qu'elle charge | moyen |
| IT3-04 | Compte professionnel indexé à **110 %** (49 894 documents Elasticsearch pour 45 331 messages) : ≈ 4 563 entrées orphelines, donc des résultats de recherche fantômes | à investiguer |

IT3-04 n'a pas été creusé : le diagnostic exigerait de **lire** le compte professionnel.

**Ce qui a été vérifié à l'écran** : envoi complet depuis l'interface (autocomplétion → éditeur →
SMTP → réception), déplacement groupé **sans duplication** (3 messages exactement, 2 + 1), filtres
de colonne côté serveur, les 9 thèmes, le raccourci `N`, et **zéro message de console** sur sept
pages et une vingtaine d'actions.

**Le cloisonnement tient sur quatre surfaces indépendantes** — sélecteur de comptes, page Comptes,
page Contacts, et l'historique d'autocomplétion des destinataires. Cette dernière compte : elle se
nourrit de l'historique d'envoi, et c'est typiquement par là qu'une fuite passe inaperçue. Aucune
adresse `@ebusinet.fr` nulle part.

Réserve : `qa_bot` est administrateur et voit donc légitimement les métadonnées du compte
professionnel via la page Admin. Ce droit ne lui sert à rien d'autre — **le retirer est gratuit**.

Enfin, le formulaire de connexion s'est présenté pré-rempli par le gestionnaire de mots de passe
avec `contact@ebusinet.fr`. Il n'a pas été soumis, aucun mot de passe n'a été saisi ; la session a
été ouverte par injection du jeton QA.

---

*Rapports détaillés disponibles dans le répertoire de travail de la session : rapports de test des
trois itérations, rapports de correction des cinq lots, audit du frontend et du worker, audit de
véracité de la documentation, revue de la suite de tests.*

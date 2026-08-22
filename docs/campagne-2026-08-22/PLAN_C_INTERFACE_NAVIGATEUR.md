# Plan C — Test de l'interface par pilotage navigateur

Exécuté le 2026-08-22, Chrome piloté, un seul onglet, utilisateur `qa_bot` (id 5).
Tous les comptes visibles sont des comptes QA jetables. Aucun compte `@ebusinet.fr` n'a été
ouvert, ni aucun mot de passe saisi.

## Pourquoi ce plan existe

Les cinq plans précédents attaquaient l'API. Ils ne pouvaient rien dire de ce qui ne vit que
dans le navigateur : l'échappement au moment de l'insertion dans le DOM, l'état visible d'une
page pendant qu'elle charge, et le comportement d'un composant tiers (TinyMCE) dans un thème
sombre. Les quatre défauts ci-dessous sont tous de cette nature — aucun n'était atteignable
depuis un test d'API.

## Note de méthode : refus de connexion

Le formulaire de connexion s'est présenté **pré-rempli par le gestionnaire de mots de passe avec
`contact@ebusinet.fr`**. Il n'a pas été soumis, et aucun mot de passe n'a été saisi. La session
a été ouverte en injectant le jeton QA dans `localStorage`. C'est le seul chemin qui garantit
qu'aucune identité réelle n'est engagée.

Les boîtes de dialogue bloquantes ont été neutralisées avant tout test XSS
(`window.alert/confirm/prompt` remplacés par des collecteurs), sans quoi une alerte figeait
l'extension et rendait la mesure impossible.

---

## Résultats

### IT3-05 — XSS par surlignage de recherche : **non exploitable** (rectifie une alerte antérieure)

L'API renvoie bien la charge brute dans le JSON de recherche globale — c'est ce qui avait fait
craindre une injection. **Mesuré dans le DOM après rendu réel** :

| Indicateur | Valeur |
|---|---|
| Alertes déclenchées | 0 |
| Éléments `<img>` injectés | 0 |
| Attributs `onerror` | 0 |
| `<script>` supplémentaires | 0 |
| Charge présente en **texte littéral** | oui |

Le frontend échappe à l'insertion. **Une charge brute dans une réponse API n'est pas une XSS** ;
seul le rendu tranche. C'est exactement le genre de conclusion que l'analyse statique ne pouvait
pas donner, dans un sens comme dans l'autre.

### IT3-02 — Dashboard inutilisable sans fournisseur IA — *défaut, moyen*

Sans fournisseur IA configuré, la page Dashboard est **entièrement vide** et n'affiche qu'une
ligne :

```
Erreur: AI analysis failed: No AI provider configured for user 5 and no system fallback
```

Trois problèmes cumulés :

1. **Aucun mode dégradé.** Le Dashboard porte aussi des statistiques qui ne demandent aucune IA
   (volumes, non-lus, principaux expéditeurs). Une seule dépendance absente emporte la page entière.
2. **Message technique brut, en anglais**, dans une interface française.
3. **Fuite mineure** : l'identifiant interne de l'utilisateur (`user 5`) est exposé côté client.

### IT3-03 — Page Statut : 19,2 s de latence, sans indicateur — *défaut, moyen*

À l'ouverture, la page affiche quatre cartes réduites à leur titre, corps vides. Elle paraît
cassée. Elle ne l'est pas : `GET /api/admin/status` répond `200` avec **52 368 octets**, mais met
**19,2 secondes** (mesuré côté navigateur, deux appels concordants).

Le diagnostic initial « bug CSS » était faux : le contenu était simplement pas encore arrivé.
La preuve tient en une mesure — le corps de la carte « Synchronisation » contenait déjà 825
caractères au moment où l'écran paraissait vide.

Correctifs à envisager :
- un état de chargement **par carte**, pas seulement le discret « Chargement… » en coin ;
- l'endpoint interroge les totaux IMAP de tous les dossiers de tous les comptes à chaque appel —
  c'est ce qui coûte les 19 s. Un cache court, ou un chargement en deux temps (structure d'abord,
  totaux ensuite), supprimerait l'effet.

### IT3-04 — Indexation à 110 % sur le compte professionnel — *anomalie, à investiguer*

| Compte | Documents Elasticsearch | Messages IMAP | Ratio |
|---|---:|---:|---:|
| **pro OVH** | 49 894 | 45 331 | **110 %** |
| QA GreenMail | 85 | 145 | 59 % |
| QA Dovecot | 13 | 194 | 7 % |
| QA Volume Dovecot | 8 374 | 8 959 | 93 % |

**≈ 4 563 documents indexés de plus qu'il n'existe de messages.** Hypothèse la plus probable :
des messages supprimés ou déplacés côté IMAP dont l'entrée Elasticsearch n'a jamais été purgée.
Conséquence concrète : la recherche peut renvoyer des résultats fantômes, dont l'ouverture
échouera.

Non corrigé : le diagnostic exige de comparer les identifiants côté ES et côté IMAP sur le compte
professionnel, donc de **lire** ce compte. Écarté sans accord explicite.

### Ce qui fonctionne — vérifié à l'écran

| Test | Vérification | Résultat |
|---|---|---|
| **IT3-01** Envoi depuis l'interface | Autocomplétion → TinyMCE → SMTP → réception | **PASS** — message reçu, INBOX 44→45 |
| **IT3-06** Déplacement groupé | 2 messages déplacés vers un dossier en contenant 1 | **PASS** — 3 exactement, **aucune duplication** |
| **IT3-07** Filtres de colonne | Filtre objet `QA-SMTP03` | **PASS** — 6 résultats, tous conformes |
| **IT3-09** Thèmes | Les 9 thèmes, bascule Violet ↔ Dracula | **PASS** — appliqué globalement |
| **IT3-09** Raccourci clavier | `N` = nouveau message | **PASS** |
| **Console** | Tout le parcours (7 pages, ~25 actions) | **0 message**, 0 erreur |

### IT3-08 — Cloisonnement vérifié à quatre endroits distincts

L'isolation avait été mesurée côté API. L'interface la confirme sur quatre surfaces
indépendantes, toutes muettes sur les données réelles pour `qa_bot` :

- **sélecteur de compte** — seuls les comptes QA (3, 30, 60) ;
- **page Comptes** — 3 comptes QA, aucun compte professionnel ;
- **page Contacts** — un seul contact QA ;
- **historique d'autocomplétion** des destinataires — 4 adresses, toutes en `.local` /
  `qa-autotest.local`, **aucune adresse `@ebusinet.fr`**.

Cette dernière mérite d'être relevée : l'autocomplétion se nourrit de l'historique d'envoi, et
c'est typiquement par là qu'une fuite inter-utilisateurs passe inaperçue.

**Réserve.** `qa_bot` est administrateur, et `/api/admin/status` expose donc légitimement les
métadonnées du compte professionnel (nom IMAP, volumes, date de synchro). Ce n'est pas une faille
— c'est le rôle admin qui joue — mais **l'utilisateur QA n'a pas besoin de ce droit**. Le
retirer réduirait la surface sans rien coûter aux tests, hormis à la page Admin elle-même.

---

## Compte protégé — 10ᵉ vérification

Relevé après la campagne, identique au relevé de référence :

```
id 1 | pro OVH | e.pimienta@ebusinet.fr | last_sync_at = 2026-05-20 10:05:51.760324 | sync_enabled = t
emails locaux rattachés au compte 1 : 1227
```

Inchangé.

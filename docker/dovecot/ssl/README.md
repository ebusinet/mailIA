# Certificat de test Dovecot — NE PAS UTILISER EN PRODUCTION

Certificat auto-signe **volontairement versionne**, avec sa cle privee, pour que le
serveur de test et les conteneurs applicatifs partagent la meme autorite de facon
reproductible. Sans cela, le certificat etait regenere a chaque volume neuf et aucun
magasin de confiance ne pouvait le connaitre a l'avance : six chemins d'envoi (STARTTLS)
restaient non testables.

La cle privee ci-jointe est publique par construction. Elle ne protege rien : elle ne
sert qu'a chiffrer du trafic entre deux conteneurs jetables.

Il n'est installe dans l'image applicative **que** si le build passe
`TRUST_TEST_CERTS=1` (voir `docker/Dockerfile.api`). Un build normal ne le contient pas.

Regeneration : voir la commande dans `docker/dovecot/entrypoint.sh`.

## Pourquoi la paire n'est finalement pas versionnee

Le magasin de confiance partage a ete **abandonne** : `mailia-api` detient les identifiants
IMAP du compte professionnel reel, et y installer une autorite dont la cle privee est
publique rendrait triviale l'usurpation de n'importe quel serveur — pour le seul benefice
de faire passer un test de `SKIP` a `PASS`.

`dovecot-test.crt` et `dovecot-test.key` sont donc ignores par git. Sans eux,
`entrypoint.sh` genere un certificat au premier demarrage, ce qui suffit a un serveur de
test. Le mecanisme reste en place pour le jour ou un conteneur QA distinct, ne servant
aucun compte reel, justifiera `TRUST_TEST_CERTS=1` : il faudra alors generer la paire
localement.

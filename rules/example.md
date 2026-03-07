# Regles de tri MailIA - Exemple

## Factures et devis
- **Si**: le sujet contient "facture", "devis", "invoice" OU une piece jointe PDF contient "facture" dans le nom
- **Alors**: deplacer vers INBOX/Comptabilite
- **Et**: marquer comme important
- **Notifier**: non

## Clients urgents
- **Si**: l'IA detecte un ton urgent ou une demande d'action ET l'expediteur est un contact connu
- **Alors**: garder dans INBOX
- **Et**: flag comme important
- **Notifier**: oui, avec resume

## Newsletters
- **Si**: l'expediteur contient "newsletter", "noreply", "marketing" OU le sujet contient "unsubscribe"
- **Alors**: deplacer vers INBOX/Newsletters
- **Et**: marquer comme lu
- **Notifier**: non

## Administratif
- **Si**: l'expediteur contient "impots", "urssaf", "caf", "ameli", "cpam"
- **Alors**: deplacer vers INBOX/Admin
- **Et**: marquer comme important
- **Notifier**: oui, avec resume

## Spam intelligent
- **Si**: l'IA detecte un mail commercial non sollicite ou du spam deguise
- **Alors**: deplacer vers Junk
- **Et**: marquer comme lu
- **Notifier**: non

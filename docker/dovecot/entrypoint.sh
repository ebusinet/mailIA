#!/bin/sh
set -e
# Certificat : on prefere celui du depot (docker/dovecot/ssl, monte en lecture seule
# sur /etc/dovecot/ssl-seed). Il est fixe, donc les conteneurs applicatifs peuvent
# l'avoir dans leur magasin de confiance a l'avance. Sans lui, un certificat different
# etait genere a chaque volume neuf et STARTTLS restait non testable.
if [ ! -f /etc/dovecot/ssl/cert.pem ]; then
    mkdir -p /etc/dovecot/ssl
    if [ -f /etc/dovecot/ssl-seed/dovecot-test.crt ]; then
        cp /etc/dovecot/ssl-seed/dovecot-test.crt /etc/dovecot/ssl/cert.pem
        cp /etc/dovecot/ssl-seed/dovecot-test.key /etc/dovecot/ssl/key.pem
    else
        # Repli : certificat jetable, non reconnu par les clients.
        openssl req -new -x509 -days 3650 -nodes \
            -out /etc/dovecot/ssl/cert.pem -keyout /etc/dovecot/ssl/key.pem \
            -subj "/CN=dovecot" \
            -addext "subjectAltName=DNS:dovecot,DNS:mailia-dovecot,DNS:localhost,IP:127.0.0.1" \
            >/dev/null 2>&1
    fi
    chmod 600 /etc/dovecot/ssl/key.pem
fi
mkdir -p /srv/mail && chown -R 1000:1000 /srv/mail
exec dovecot -F

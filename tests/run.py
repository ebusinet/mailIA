#!/usr/bin/env python3
"""Suite de non-regression MailIA — point d'entree unique.

    python3 tests/run.py

Bibliotheque standard uniquement : aucune installation prealable, sur aucune machine.

AVANT toute execution, un garde-fou verifie que le jeton fourni appartient au compte QA et
que tous les comptes de messagerie accessibles pointent sur un serveur jetable. S'il refuse,
rien ne s'execute. Voir tests/qa/guard.py pour le detail et la raison d'etre.

Options :
    -k MOTIF     n'executer que les tests dont l'identifiant ou le titre contient MOTIF
    -g GROUPE    n'executer qu'un groupe (isolation, duplication, search, ...)
    -l           lister les tests sans les executer
    -v           afficher la trace complete des echecs
    --no-guard   NE PAS UTILISER. Reserve au diagnostic du garde-fou lui-meme sur une
                 instance sans API ; refuse de s'executer si un jeton est present.

Code de sortie : 0 si aucun echec, 1 sinon, 2 si le garde-fou a refuse de demarrer.
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from qa.core import (BOX, CFG, REGISTRY, Failure, GuardError,  # noqa: E402
                     Skip, nettoyage_final)
from qa.guard import run_guard  # noqa: E402
from qa.suites import (duplication, emails, guard_selftest, isolation,  # noqa: E402,F401
                       misc, rules_and_storage, search, smtp_paths)

VERT, ROUGE, JAUNE, GRIS, GRAS, RAZ = (
    ("\033[32m", "\033[31m", "\033[33m", "\033[90m", "\033[1m", "\033[0m")
    if sys.stdout.isatty() else ("", "", "", "", "", "")
)


def main() -> int:
    p = argparse.ArgumentParser(add_help=True, description="Suite de non-regression MailIA")
    p.add_argument("-k", dest="motif", default="", help="filtrer par identifiant ou titre")
    p.add_argument("-g", dest="groupe", default="", help="filtrer par groupe")
    p.add_argument("-l", dest="lister", action="store_true", help="lister sans executer")
    p.add_argument("-v", dest="verbeux", action="store_true", help="trace complete des echecs")
    p.add_argument("--no-guard", action="store_true", help=argparse.SUPPRESS)
    args = p.parse_args()

    cas = sorted(REGISTRY, key=lambda t: t.ident)
    if args.motif:
        m = args.motif.lower()
        cas = [t for t in cas if m in t.ident.lower() or m in t.title.lower()]
    if args.groupe:
        cas = [t for t in cas if args.groupe.lower() in t.group.lower()]

    if args.lister:
        for t in cas:
            ref = f" [{t.ref}]" if t.ref else ""
            print(f"  {t.ident:10} {t.group:18} {t.title}{ref}")
        print(f"\n  {len(cas)} test(s)")
        return 0

    print(f"\n{GRAS}Suite de non-regression MailIA{RAZ}")
    print(f"{GRIS}  API    : {CFG.api_url}{RAZ}")

    # ---- Garde-fou -------------------------------------------------------
    if args.no_guard:
        if CFG.token:
            print(f"\n{ROUGE}{GRAS}REFUS : --no-guard est interdit quand un jeton est present."
                  f"{RAZ}\n  Cette option ne sert qu'a diagnostiquer le garde-fou hors ligne.\n")
            return 2
        print(f"{JAUNE}  garde-fou desactive (aucun jeton) : seuls les tests hors ligne "
              f"s'executeront{RAZ}")
        cas = [t for t in cas if t.group == "guard_selftest"]
    else:
        try:
            info = run_guard()
        except GuardError as e:
            print(f"\n{ROUGE}{GRAS}LE GARDE-FOU A REFUSE DE DEMARRER LA SUITE{RAZ}\n")
            print(f"  {e}\n")
            print(f"{GRIS}  Aucun test n'a ete execute, aucune donnee n'a ete touchee.{RAZ}\n")
            return 2
        except Skip as e:
            print(f"\n{ROUGE}{GRAS}API injoignable{RAZ}\n\n  {e}\n")
            return 2
        print(f"{GRIS}  compte : {info['identite']} (id {info['user_id']}){RAZ}")
        print(f"{GRIS}  cible  : {info['compte_cible']}{RAZ}")
        print(f"{VERT}  garde-fou : OK — {info['nb_comptes']} compte(s) accessible(s), "
              f"tous sur un serveur de test{RAZ}")

    if not BOX.available:
        print(f"{JAUNE}  conteneur : indisponible — les tests MCP/IMAP/base rendront SKIP{RAZ}")

    print()
    resultats = []
    debut = time.time()
    groupe_courant = None

    for t in cas:
        if t.group != groupe_courant:
            groupe_courant = t.group
            print(f"{GRAS}{groupe_courant}{RAZ}", flush=True)
        t0 = time.time()
        try:
            t.fn()
            statut, detail = "PASS", ""
        except Skip as e:
            statut, detail = "SKIP", str(e)
        except (Failure, AssertionError) as e:
            # Le produit ne se comporte pas comme attendu : c'est une regression.
            statut, detail = "FAIL", str(e)
        except Exception as e:
            # Le test lui-meme est casse (NameError, IndexError, appel malforme...).
            # Le distinguer d'un FAIL evite d'afficher un defaut de la suite sous la
            # reference d'un bug produit — un lecteur en conclurait a tort a une regression.
            statut = "ERROR"
            detail = (f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                      if args.verbeux else f"{type(e).__name__}: {e}")
        duree = time.time() - t0
        resultats.append((t, statut, detail, duree))

        couleur = {"PASS": VERT, "FAIL": ROUGE, "ERROR": ROUGE, "SKIP": JAUNE}[statut]
        ref = f"{GRIS}[{t.ref}]{RAZ}" if t.ref else ""
        print(f"  {couleur}{statut:5}{RAZ} {t.ident:9} {t.title} {ref} "
              f"{GRIS}{duree:.1f}s{RAZ}", flush=True)
        if statut == "SKIP":
            print(f"       {JAUNE}> {detail}{RAZ}", flush=True)

    # ---- Menage ----------------------------------------------------------
    if not args.no_guard:
        reste = nettoyage_final()
        if reste:
            print(f"\n{GRIS}  menage : {reste}{RAZ}")

    # ---- Rapport ---------------------------------------------------------
    echecs = [r for r in resultats if r[1] == "FAIL"]
    erreurs = [r for r in resultats if r[1] == "ERROR"]
    passes = [r for r in resultats if r[1] == "PASS"]
    skips = [r for r in resultats if r[1] == "SKIP"]

    if echecs:
        print(f"\n{ROUGE}{GRAS}ECHECS — le produit ne se comporte pas comme attendu{RAZ}\n")
        for t, _, detail, _ in echecs:
            origine = f" — bug d'origine : {t.ref}" if t.ref else ""
            print(f"{ROUGE}  {t.ident} — {t.title}{RAZ}{origine}")
            for ligne in detail.splitlines():
                print(f"      {ligne}")
            print()

    if erreurs:
        print(f"\n{ROUGE}{GRAS}ERREURS — le test lui-meme est casse, ce n'est pas une "
              f"regression du produit{RAZ}\n")
        for t, _, detail, _ in erreurs:
            print(f"{ROUGE}  {t.ident} — {t.title}{RAZ}")
            for ligne in detail.splitlines():
                print(f"      {ligne}")
            print()

    total = time.time() - debut
    resume = f"{len(passes)} PASS   {len(echecs)} FAIL   {len(skips)} SKIP"
    if erreurs:
        resume += f"   {len(erreurs)} ERROR"
    print(f"\n{GRAS}{resume}{RAZ}   {GRIS}({total:.0f}s){RAZ}")
    if skips:
        print(f"{GRIS}  Les SKIP ne sont pas des succes : un test qui ne peut pas s'executer "
              f"le dit.{RAZ}")
    print()
    return 1 if (echecs or erreurs) else 0


if __name__ == "__main__":
    sys.exit(main())

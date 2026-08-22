"""Harnais de concurrence — Plan E.

Pourquoi un harnais separe
--------------------------
Le reste de la suite mesure par recensement avant/apres. **La concurrence invalide ce
motif** : quand deux operations se chevauchent, il n'existe pas d'instant « avant » ni
« apres » commun, et un recensement pris entre les deux ne decrit aucun etat coherent.

Quatre regles remplacent le recensement, et chacune repare un piege observe pendant la
campagne.

1. **La course doit etre reellement simultanee.** Deux requetes lancees a la suite dans une
   boucle ne se chevauchent pas forcement : le temps de construction de la requete suffit a
   les serialiser. Les fils partent donc d'une `threading.Barrier`.

2. **Le chevauchement se prouve, il ne se suppose pas.** On horodate depart et arrivee de
   chaque requete, et on verifie que les intervalles se recouvrent :
   `max(departs) < min(arrivees)`. Sans recouvrement, le test n'a pas teste la concurrence —
   il rend `Skip` avec la raison, **jamais** `PASS`. C'est le piege le plus dangereux du
   plan, parce qu'il se cache derriere un vert : un test qui ne fait que de la sequence
   passe toujours, et passera encore le jour ou le defaut apparaitra.

3. **L'invariant ne suppose aucun instant.** `total_avant == total_apres` demande un instant
   que la concurrence n'a pas. `chaque message existe exactement une fois` s'evalue a la
   fin, une fois les operations terminees, et attrape aussi bien la duplication que la
   perte. C'est l'invariant qui a trouve A-18 sans le chercher, porte sur des identites
   plutot que sur des comptes.

4. **Un defaut de concurrence ne se manifeste pas a tous les coups.** Une course jouee une
   fois ne prouve rien. Chaque test repete, et annonce **combien** de fois : « vert sur 20
   courses dont 20 avec chevauchement prouve » n'est pas la meme information que « vert ».
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class Tir:
    """Une requete d'une course : son resultat et ses horodatages."""
    nom: str
    debut: float = 0.0
    fin: float = 0.0
    resultat: object = None
    erreur: BaseException | None = None

    @property
    def duree(self) -> float:
        return self.fin - self.debut


@dataclass
class Course:
    """Le resultat d'une course : les tirs, et de quoi juger s'ils se sont chevauches."""
    tirs: list[Tir] = field(default_factory=list)

    @property
    def chevauchement(self) -> float:
        """Duree pendant laquelle TOUS les tirs etaient en vol, en secondes.

        Negative ou nulle : les requetes ne se sont pas recouvertes, la course n'a mesure
        qu'une sequence.
        """
        if len(self.tirs) < 2:
            return 0.0
        return min(t.fin for t in self.tirs) - max(t.debut for t in self.tirs)

    @property
    def recouvrement(self) -> float:
        """Part du tir le plus court pendant laquelle tous les tirs etaient en vol.

        Un chevauchement de quelques microsecondes est positif et ne prouve rien : les deux
        requetes se sont croisees sans jamais etre reellement ensemble dans la section
        critique du serveur. On rapporte donc le chevauchement a la duree du tir le plus
        court, qui est la fenetre pendant laquelle un recouvrement etait possible.
        """
        if len(self.tirs) < 2:
            return 0.0
        plus_court = min(t.duree for t in self.tirs)
        if plus_court <= 0.001:
            # Un tir quasi instantane ne peut pas courir contre quoi que ce soit : la
            # reponse etait deja partie. On ne peut rien conclure.
            return 0.0
        return self.chevauchement / plus_court

    # En deca, les requetes se sont croisees sans se superposer utilement. Le seuil est
    # volontairement exigeant : un faux « simultane » produit un vert qui ne prouve rien,
    # et c'est le piege principal de ce plan.
    SEUIL_RECOUVREMENT = 0.5

    @property
    def simultanee(self) -> bool:
        return self.chevauchement > 0 and self.recouvrement >= self.SEUIL_RECOUVREMENT

    def resultats(self) -> list:
        return [t.resultat for t in self.tirs]

    def __str__(self) -> str:
        return (f"{len(self.tirs)} tirs, chevauchement {self.chevauchement * 1000:.0f} ms "
                f"({self.recouvrement * 100:.0f} % du plus court), "
                + ", ".join(f"{t.nom}={t.duree * 1000:.0f}ms" for t in self.tirs))


def lancer(*operations, timeout: float = 120.0) -> Course:
    """Execute les operations en parallele, toutes liberees au meme instant.

    `operations` : des couples (nom, callable) ou des callables simples.

    La barriere est le point essentiel. Sans elle, le premier fil a souvent fini avant que
    le dernier n'ait commence — et l'on croit avoir teste la concurrence alors qu'on a
    mesure une sequence.
    """
    paires = []
    for i, op in enumerate(operations):
        if isinstance(op, tuple):
            paires.append(op)
        else:
            paires.append((getattr(op, "__name__", f"op{i}"), op))

    tirs = [Tir(nom) for nom, _ in paires]
    barriere = threading.Barrier(len(paires))

    def executer(indice, fonction):
        tir = tirs[indice]
        try:
            barriere.wait(timeout=timeout)
        except threading.BrokenBarrierError as e:
            tir.erreur = e
            tir.debut = tir.fin = time.time()
            return
        tir.debut = time.time()
        try:
            tir.resultat = fonction()
        except BaseException as e:          # noqa: BLE001 — on veut aussi les erreurs
            tir.erreur = e
        finally:
            tir.fin = time.time()

    fils = [threading.Thread(target=executer, args=(i, f), daemon=True)
            for i, (_, f) in enumerate(paires)]
    for f in fils:
        f.start()
    for f in fils:
        f.join(timeout=timeout)

    return Course(tirs)


def exiger_simultaneite(course: Course, contexte: str = "") -> None:
    """Rend Skip si les requetes ne se sont pas recouvertes.

    Un test de concurrence qui n'a pas eu lieu doit le dire. Le laisser passer au vert est
    pire que de ne pas l'ecrire : il rassure, et il rassurera encore quand le defaut
    apparaitra.
    """
    from .core import Skip
    if not course.simultanee:
        raise Skip(
            f"les requetes ne se sont pas chevauchees{' (' + contexte + ')' if contexte else ''} "
            f"— {course}. La course n'a mesure qu'une sequence : ce test n'a rien constate "
            "sur la concurrence. Cause possible : une requete beaucoup plus rapide que "
            "l'autre, ou une serialisation cote serveur.")


def repeter(n: int, faire_une_course) -> tuple[list[Course], list[str]]:
    """Joue la meme course `n` fois et rassemble les anomalies.

    `faire_une_course` doit rendre `(Course, anomalie_ou_None)`.

    Renvoie les courses effectivement simultanees et la liste des anomalies. Un defaut de
    concurrence n'apparait pas a chaque tentative : c'est le nombre de tentatives **avec
    chevauchement prouve** qui donne sa valeur a un resultat vert.
    """
    courses, anomalies = [], []
    for _ in range(n):
        course, anomalie = faire_une_course()
        if course.simultanee:
            courses.append(course)
        if anomalie:
            anomalies.append(anomalie)
    return courses, anomalies

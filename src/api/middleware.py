"""Security middleware: rate limiting and security headers."""
import time
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# --- Rate Limiting (Redis-backed) ---

RATE_LIMIT_RULES = {
    "/api/auth/login": {"max_requests": 5, "window_seconds": 60},
    "/api/auth/register": {"max_requests": 3, "window_seconds": 60},
    "/api/auth/forgot-password": {"max_requests": 3, "window_seconds": 300},
    "/api/auth/reset-password": {"max_requests": 5, "window_seconds": 300},
}


def _est_adresse_de_mandataire(valeur: str) -> bool:
    """Adresse appartenant a l'infrastructure : boucle locale ou reseau prive."""
    import ipaddress
    try:
        adr = ipaddress.ip_address(valeur)
    except ValueError:
        return False  # valeur non analysable : on la traite comme fournie par le client
    return adr.is_private or adr.is_loopback or adr.is_link_local or adr.is_reserved


def _get_client_ip(request: Request) -> str:
    """Adresse a utiliser comme cle de limitation de debit.

    `X-Forwarded-For` est ecrit par le client ET complete par nginx, qui **ajoute**
    l'adresse reelle en fin de liste (`$proxy_add_x_forwarded_for`). Retenir le PREMIER
    element revenait donc a laisser l'appelant choisir sa propre cle : une ligne
    `X-Forwarded-For: 203.0.113.<compteur>` suffisait a rendre la limite inoperante.

    On parcourt donc la liste **de droite a gauche** et on retient la premiere adresse
    qui n'appartient pas a l'infrastructure. Ce parcours ne depend pas du nombre de
    mandataires : ajouter un hop demain ne rouvre pas la faille.
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        elements = [p.strip() for p in forwarded.split(",") if p.strip()]
        for candidat in reversed(elements):
            if not _est_adresse_de_mandataire(candidat):
                return candidat
        if elements:
            # Tout est prive : appel interne. La derniere entree est celle qu'a ecrite
            # l'infrastructure, jamais celle que le client a fournie.
            return elements[-1]
    # nginx REMPLACE X-Real-IP (il ne l'ajoute pas), une valeur fournie par le client
    # est donc ecrasee. Utilisable en repli quand X-Forwarded-For est absent.
    real_ip = request.headers.get("x-real-ip")
    if real_ip and real_ip.strip():
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


_derniere_alerte = 0.0


def _signaler_protection_absente(motif: str) -> None:
    """Le limiteur laisse passer en cas de panne — c'est voulu, une panne Redis ne doit
    pas empecher toute connexion. Mais une protection qui disparait en silence est pire
    qu'une protection absente : on croit l'avoir. On journalise donc en ERROR, en clair,
    au plus une fois par minute pour ne pas noyer les journaux sous la charge."""
    global _derniere_alerte
    maintenant = time.time()
    if maintenant - _derniere_alerte < 60:
        return
    _derniere_alerte = maintenant
    logger.error(
        "LIMITATION DE DEBIT DESACTIVEE — %s. Les endpoints d'authentification "
        "acceptent un nombre illimite de tentatives tant que ce message se repete.",
        motif,
    )


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, redis_url: str = "redis://redis:6379/0"):
        super().__init__(app)
        self._redis = None
        self._redis_url = redis_url

    async def _get_redis(self):
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                from src.config import get_settings
                settings = get_settings()
                url = self._redis_url
                if settings.redis_password and "@" not in url.split("://", 1)[-1]:
                    url = url.replace("://", f"://:{settings.redis_password}@")
                client = aioredis.from_url(url, decode_responses=True)
                await client.ping()
                self._redis = client
            except Exception as e:
                _signaler_protection_absente(f"Redis indisponible ({e})")
                return None
        return self._redis

    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/")
        rule = RATE_LIMIT_RULES.get(path)
        if not rule or request.method not in ("POST", "PUT", "PATCH"):
            return await call_next(request)

        r = await self._get_redis()
        if r is None:
            return await call_next(request)

        ip = _get_client_ip(request)
        key = f"ratelimit:{path}:{ip}"
        window = rule["window_seconds"]
        max_req = rule["max_requests"]

        try:
            now = time.time()
            pipe = r.pipeline()
            pipe.zremrangebyscore(key, 0, now - window)
            pipe.zadd(key, {str(now): now})
            pipe.zcard(key)
            pipe.expire(key, window)
            results = await pipe.execute()
            count = results[2]

            if count > max_req:
                retry_after = int(window - (now - float((await r.zrange(key, 0, 0))[0])))
                return JSONResponse(
                    status_code=429,
                    content={"detail": f"Too many requests. Retry after {retry_after}s."},
                    headers={"Retry-After": str(retry_after)},
                )
        except Exception as e:
            _signaler_protection_absente(f"erreur du limiteur ({e})")

        return await call_next(request)


# --- Security Headers ---

SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
}


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        for header, value in SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response

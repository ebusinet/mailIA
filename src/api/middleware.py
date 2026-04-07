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


def _get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


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
                logger.warning(f"Rate limiter: Redis unavailable ({e}), skipping")
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
            logger.warning(f"Rate limiter error: {e}, allowing request")

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

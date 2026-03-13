from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from src.api.routes import auth, accounts, search, rules, ai, admin, websocket, contacts, signatures
from src.config import get_settings

settings = get_settings()

app = FastAPI(title=settings.app_name, version="0.1.0")


class SSEAwareGZipMiddleware:
    """GZip middleware that skips compression for SSE (text/event-stream) responses."""

    def __init__(self, app: ASGIApp, minimum_size: int = 1000):
        self.app = app
        self.gzip = GZipMiddleware(app, minimum_size=minimum_size)

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Check if this is a streaming AI endpoint — skip GZip entirely
        path = scope.get("path", "")
        if path.startswith("/api/ai/"):
            await self.app(scope, receive, send)
            return

        # For all other routes, use normal GZip
        await self.gzip(scope, receive, send)


app.add_middleware(SSEAwareGZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.app_url, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(accounts.router, prefix="/api/accounts", tags=["accounts"])
app.include_router(search.router, prefix="/api/search", tags=["search"])
app.include_router(rules.router, prefix="/api/rules", tags=["rules"])
app.include_router(ai.router, prefix="/api/ai", tags=["ai"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])
app.include_router(contacts.router, prefix="/api/contacts", tags=["contacts"])
app.include_router(signatures.router, prefix="/api/signatures", tags=["signatures"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])

app.mount("/static", StaticFiles(directory="src/web/static"), name="static")


@app.on_event("startup")
async def startup_event():
    try:
        from src.import_jobs import resume_interrupted_jobs
        resume_interrupted_jobs()
    except Exception:
        pass


@app.get("/")
async def root():
    return RedirectResponse(url="/static/index.html")


@app.get("/api/health")
async def health():
    return {"status": "ok", "app": settings.app_name}

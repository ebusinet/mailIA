from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from src.db.models import User
from src.api.deps import get_current_user
from src.search.indexer import get_es_client, search_emails, semantic_search

router = APIRouter()


class SearchResult(BaseModel):
    uid: str
    folder: str
    from_addr: str
    subject: str
    date: str
    has_attachments: bool
    highlight: dict | None = None
    score: float = 0


class SearchResponse(BaseModel):
    total: int
    results: list[SearchResult]


@router.get("/", response_model=SearchResponse)
async def search(
    q: str = Query("", description="Search query"),
    account_id: int | None = None,
    folder: str | None = None,
    from_addr: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    has_attachments: bool | None = None,
    page: int = 0,
    size: int = 20,
    user: User = Depends(get_current_user),
):
    es = await get_es_client()
    try:
        raw = await search_emails(
            es, user.id,
            query=q,
            account_id=account_id,
            folder=folder,
            from_addr=from_addr,
            date_from=date_from,
            date_to=date_to,
            has_attachments=has_attachments,
            page=page,
            size=size,
        )
        hits = raw["hits"]
        total = hits["total"]["value"] if isinstance(hits["total"], dict) else hits["total"]

        results = []
        for hit in hits["hits"]:
            src = hit["_source"]
            results.append(SearchResult(
                uid=src["uid"],
                folder=src.get("folder", ""),
                from_addr=src.get("from_addr", ""),
                subject=src.get("subject", ""),
                date=src.get("date", ""),
                has_attachments=src.get("has_attachments", False),
                highlight=hit.get("highlight"),
                score=hit.get("_score", 0),
            ))

        return SearchResponse(total=total, results=results)
    finally:
        await es.close()

"""
Elasticsearch indexer — indexes emails with user isolation.
Index pattern: mailia-{user_id}
"""
import logging
from elasticsearch import AsyncElasticsearch
from src.config import get_settings
from src.rules.engine import EmailContext

logger = logging.getLogger(__name__)


def _index_name(user_id: int) -> str:
    return f"mailia-{user_id}"


async def get_es_client() -> AsyncElasticsearch:
    return AsyncElasticsearch(get_settings().elasticsearch_url)


async def ensure_index(es: AsyncElasticsearch, user_id: int):
    """Create the user's email index if it doesn't exist."""
    index = _index_name(user_id)
    if await es.indices.exists(index=index):
        return

    await es.indices.create(
        index=index,
        body={
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "email_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding"],
                        }
                    }
                },
            },
            "mappings": {
                "properties": {
                    "account_id": {"type": "integer"},
                    "uid": {"type": "keyword"},
                    "folder": {"type": "keyword"},
                    "from_addr": {"type": "keyword"},
                    "from_name": {"type": "text", "analyzer": "email_analyzer"},
                    "to_addr": {"type": "keyword"},
                    "subject": {"type": "text", "analyzer": "email_analyzer"},
                    "body": {"type": "text", "analyzer": "email_analyzer"},
                    "date": {"type": "date"},
                    "has_attachments": {"type": "boolean"},
                    "attachment_names": {"type": "keyword"},
                    "attachment_content": {"type": "text", "analyzer": "email_analyzer"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 768,
                        "index": True,
                        "similarity": "cosine",
                    },
                },
            },
        },
    )
    logger.info(f"Created index {index}")


async def index_email(
    es: AsyncElasticsearch,
    user_id: int,
    account_id: int,
    email_ctx: EmailContext,
    attachment_text: str = "",
    embedding: list[float] | None = None,
):
    """Index a single email."""
    index = _index_name(user_id)
    doc_id = f"{account_id}-{email_ctx.folder}-{email_ctx.uid}"

    # Convert date to ISO 8601 for Elasticsearch (e.g. "2025-10-25 10:15" -> "2025-10-25T10:15:00")
    es_date = email_ctx.date
    if es_date and " " in es_date and "T" not in es_date:
        es_date = es_date.replace(" ", "T") + ":00"

    doc = {
        "account_id": account_id,
        "uid": email_ctx.uid,
        "folder": email_ctx.folder,
        "from_addr": email_ctx.from_addr,
        "to_addr": email_ctx.to_addr,
        "subject": email_ctx.subject,
        "body": email_ctx.body_text[:50000],  # limit body size
        "date": es_date or None,
        "has_attachments": email_ctx.has_attachments,
        "attachment_names": email_ctx.attachment_names,
        "attachment_content": attachment_text[:50000] if attachment_text else "",
    }
    if embedding:
        doc["embedding"] = embedding

    await es.index(index=index, id=doc_id, document=doc)


async def bulk_index_emails(
    es: AsyncElasticsearch,
    user_id: int,
    account_id: int,
    email_contexts: list,
) -> int:
    """Bulk index multiple emails. Returns count of successfully indexed docs."""
    if not email_contexts:
        return 0
    index = _index_name(user_id)
    operations = []
    for email_ctx in email_contexts:
        doc_id = f"{account_id}-{email_ctx.folder}-{email_ctx.uid}"
        es_date = email_ctx.date
        if es_date and " " in es_date and "T" not in es_date:
            es_date = es_date.replace(" ", "T") + ":00"
        operations.append({"index": {"_index": index, "_id": doc_id}})
        operations.append({
            "account_id": account_id,
            "uid": email_ctx.uid,
            "folder": email_ctx.folder,
            "from_addr": email_ctx.from_addr,
            "to_addr": email_ctx.to_addr,
            "subject": email_ctx.subject,
            "body": email_ctx.body_text[:50000],
            "date": es_date or None,
            "has_attachments": email_ctx.has_attachments,
            "attachment_names": email_ctx.attachment_names,
            "attachment_content": "",
        })

    result = await es.bulk(operations=operations)
    if result.get("errors"):
        error_count = sum(1 for item in result["items"] if item.get("index", {}).get("error"))
        logger.warning(f"Bulk index: {error_count} errors out of {len(email_contexts)} docs")
    return len(email_contexts)


async def search_emails(
    es: AsyncElasticsearch,
    user_id: int,
    query: str,
    account_id: int | None = None,
    folder: str | None = None,
    from_addr: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    has_attachments: bool | None = None,
    page: int = 0,
    size: int = 20,
) -> dict:
    """Full-text search with filters."""
    index = _index_name(user_id)
    must = []
    filters = []

    if query:
        must.append({
            "multi_match": {
                "query": query,
                "fields": ["subject^3", "body", "from_addr^2", "attachment_names^2", "attachment_content"],
                "fuzziness": "AUTO",
            }
        })

    if account_id:
        filters.append({"term": {"account_id": account_id}})
    if folder:
        # Support both UTF-8 display names and legacy IMAP UTF-7 in ES
        from src.imap.manager import _encode_imap_utf7
        utf7 = _encode_imap_utf7(folder)
        if utf7 != folder:
            filters.append({"bool": {"should": [
                {"term": {"folder": folder}},
                {"term": {"folder": utf7}},
            ], "minimum_should_match": 1}})
        else:
            filters.append({"term": {"folder": folder}})
    if from_addr:
        filters.append({"wildcard": {"from_addr": f"*{from_addr}*"}})
    if has_attachments is not None:
        filters.append({"term": {"has_attachments": has_attachments}})
    if date_from or date_to:
        date_range = {}
        if date_from:
            date_range["gte"] = date_from
        if date_to:
            date_range["lte"] = date_to
        filters.append({"range": {"date": date_range}})

    body = {
        "query": {
            "bool": {
                "must": must or [{"match_all": {}}],
                "filter": filters,
            }
        },
        "highlight": {
            "fields": {
                "subject": {},
                "body": {"fragment_size": 200, "number_of_fragments": 3},
                "attachment_content": {"fragment_size": 200, "number_of_fragments": 2},
            }
        },
        "from": page * size,
        "size": size,
        "sort": [{"date": {"order": "desc"}}],
    }

    return await es.search(index=index, body=body)


async def semantic_search(
    es: AsyncElasticsearch,
    user_id: int,
    query_embedding: list[float],
    size: int = 10,
) -> dict:
    """Vector similarity search."""
    index = _index_name(user_id)
    return await es.search(
        index=index,
        knn={
            "field": "embedding",
            "query_vector": query_embedding,
            "k": size,
            "num_candidates": size * 10,
        },
    )

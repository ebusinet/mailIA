"""
Migrate Elasticsearch folder fields from IMAP UTF-7 to UTF-8.

Run inside the API container:
  docker compose exec -T api python3 -m scripts.migrate_es_folders_utf8

Or pipe directly:
  cat scripts/migrate_es_folders_utf8.py | docker compose exec -T api python3 -
"""
import asyncio
import logging
from elasticsearch import AsyncElasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate_folders")


def decode_imap_utf7(s: str) -> str:
    """Decode IMAP modified UTF-7 to UTF-8 (standalone copy)."""
    import base64
    result = []
    i = 0
    while i < len(s):
        if s[i] == '&':
            end = s.index('-', i + 1)
            if end == i + 1:
                result.append('&')
            else:
                b64 = s[i + 1:end].replace(',', '/')
                while len(b64) % 4:
                    b64 += '='
                raw = base64.b64decode(b64)
                result.append(raw.decode('utf-16-be'))
            i = end + 1
        else:
            result.append(s[i])
            i += 1
    return ''.join(result)


def needs_decoding(folder: str) -> bool:
    """Check if a folder name contains IMAP UTF-7 sequences (&...-)."""
    import re
    return bool(re.search(r'&[A-Za-z0-9+,]+-', folder))


async def migrate():
    from src.config import get_settings
    settings = get_settings()
    es = AsyncElasticsearch(settings.elasticsearch_url)

    try:
        # Find all mailia-* indices
        indices = await es.indices.get(index="mailia-*")
        index_names = list(indices.keys())
        logger.info(f"Found {len(index_names)} indices: {index_names}")

        total_updated = 0
        total_skipped = 0

        for index in index_names:
            logger.info(f"\n--- Processing {index} ---")

            # Get all unique folder values
            agg_result = await es.search(
                index=index,
                body={
                    "size": 0,
                    "aggs": {"folders": {"terms": {"field": "folder", "size": 1000}}},
                },
            )
            buckets = agg_result.get("aggregations", {}).get("folders", {}).get("buckets", [])
            folders_to_fix = {}
            for b in buckets:
                raw = b["key"]
                if needs_decoding(raw):
                    decoded = decode_imap_utf7(raw)
                    folders_to_fix[raw] = decoded
                    logger.info(f"  Will convert: {raw} -> {decoded} ({b['doc_count']} docs)")

            if not folders_to_fix:
                logger.info(f"  No UTF-7 folders found, skipping.")
                total_skipped += 1
                continue

            # Update each folder using update_by_query
            for utf7_name, utf8_name in folders_to_fix.items():
                logger.info(f"  Updating '{utf7_name}' -> '{utf8_name}'...")
                result = await es.update_by_query(
                    index=index,
                    body={
                        "query": {"term": {"folder": utf7_name}},
                        "script": {
                            "source": "ctx._source.folder = params.new_folder",
                            "params": {"new_folder": utf8_name},
                        },
                    },
                    refresh=True,
                )
                updated = result.get("updated", 0)
                total_updated += updated
                logger.info(f"    Updated {updated} documents")

        logger.info(f"\n=== Migration complete: {total_updated} docs updated, {total_skipped} indices skipped ===")

    finally:
        await es.close()


if __name__ == "__main__":
    asyncio.run(migrate())

#!/usr/bin/env python3
"""
sync.py

Mirrors Adobe Lightroom Classic catalog Collections into Immich albums.

Each Lightroom Collection (standard or smart) is mapped to an Immich album.
Nested collection hierarchies are preserved in the album name using ' > ' as
a separator (e.g. '2023 > Family > Christmas').

Required environment variables:
  LIGHTROOM_CATALOG   Path to the .lrcat file
  IMMICH_API_URL      Immich API base URL (e.g. https://photos.example.com/api)
  IMMICH_API_KEY      Immich API key

Optional environment variables:
  REDIS_URL           Redis connection URL (default: redis://localhost:6379/0)
  REDIS_CACHE_TTL     Cache TTL in seconds; 0 = no expiry (default: 604800 = 7 days)
  BATCH_SIZE          Max assets per add-to-album API call (default: 500)
  DRY_RUN             Set to '1' to preview without making changes (default: 0)

The script requires a working Redis connection by default. Pass -f / --force to
allow it to run without Redis (every asset will be resolved via the Immich API
on every run, which is significantly slower for large catalogs).
"""

import os
import sys
import logging
import argparse
from typing import Optional

import requests

from lrtools.lrcat import LRCatDB, LRCatException
from lrtools.lrselectgeneric import LRSelectException
from lrtools.lrtoolconfig import LRToolConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Lightroom collection creation IDs
LR_COLLECTION_SET = "com.adobe.ag.library.group"
LR_COLLECTION = "com.adobe.ag.library.collection"
LR_SMART_COLLECTION = "com.adobe.ag.library.smart_collection"

# Redis key prefix for LR UUID → Immich asset UUID mappings
CACHE_KEY_PREFIX = "lr2immich:asset:"

# Separator used to build album names for nested collections
HIERARCHY_SEPARATOR = " > "


# ---------------------------------------------------------------------------
# Immich API client
# ---------------------------------------------------------------------------
class ImmichClient:
    """Thin wrapper around the Immich REST API."""

    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "x-api-key": api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------
    def _get(self, path: str, params: Optional[dict] = None) -> object:
        r = self._session.get(f"{self.base_url}{path}", params=params)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> object:
        r = self._session.post(f"{self.base_url}{path}", json=body)
        r.raise_for_status()
        return r.json()

    def _put(self, path: str, body: dict) -> object:
        r = self._session.put(f"{self.base_url}{path}", json=body)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------
    # Albums
    # ------------------------------------------------------------------
    def get_all_albums(self) -> list[dict]:
        """Return a list of all albums owned by the authenticated user."""
        return self._get("/albums")

    def get_album_info(self, album_id: str) -> dict:
        """Return full album info including asset list."""
        return self._get(f"/albums/{album_id}")

    def create_album(self, name: str) -> dict:
        """Create an empty album and return the created album object."""
        return self._post("/albums", {"albumName": name})

    def add_assets_to_album(self, album_id: str, asset_ids: list[str]) -> list[dict]:
        """Add assets (by UUID) to an album. Returns a bulk-id response list."""
        return self._put(f"/albums/{album_id}/assets", {"ids": asset_ids})

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def search_by_filename(self, filename: str, page_size: int = 100) -> list[dict]:
        """
        Return all non-deleted Immich assets whose originalFileName matches
        *filename*. Handles API pagination automatically.
        """
        assets: list[dict] = []
        page = 1
        while True:
            data = self._post(
                "/search/metadata",
                {
                    "originalFileName": filename,
                    "withDeleted": False,
                    "page": page,
                    "size": page_size,
                },
            )
            batch = data.get("assets", {})
            items = batch.get("items", [])
            assets.extend(items)
            # Stop when the API signals no further pages
            if not batch.get("nextPage") or not items:
                break
            page += 1

        return assets


# ---------------------------------------------------------------------------
# Redis cache (optional — the sync runs without it)
# ---------------------------------------------------------------------------
def build_redis_client(redis_url: str) -> tuple[Optional[object], Optional[str]]:
    """
    Attempt to connect to Redis.

    Returns (client, None) on success, or (None, error_message) on failure.
    """
    try:
        import redis as _redis  # optional dependency

        client = _redis.from_url(redis_url, socket_connect_timeout=3)
        client.ping()
        log.info("Redis cache connected at %s", redis_url)
        return client, None
    except Exception as exc:
        return None, str(exc)


def cache_get(client: Optional[object], lr_uuid: str) -> Optional[str]:
    """Return the cached Immich ID for *lr_uuid*, or None on cache miss."""
    if client is None:
        return None
    try:
        value = client.get(f"{CACHE_KEY_PREFIX}{lr_uuid}")
        return value.decode() if value else None
    except Exception:
        return None


def cache_set(
    client: Optional[object], lr_uuid: str, immich_id: str, ttl: int
) -> None:
    """Store the LR UUID → Immich ID mapping in Redis with an optional TTL."""
    if client is None:
        return
    try:
        client.set(
            f"{CACHE_KEY_PREFIX}{lr_uuid}",
            immich_id,
            ex=ttl if ttl > 0 else None,
        )
    except Exception:
        pass  # Non-fatal; sync will just re-resolve next time


# ---------------------------------------------------------------------------
# Lightroom helpers
# ---------------------------------------------------------------------------
def open_catalog(path: str) -> LRCatDB:
    """Open the Lightroom catalog in read-only mode and return a LRCatDB."""
    try:
        lrdb = LRCatDB(LRToolConfig(), path)
    except LRCatException as exc:
        log.error("Cannot open Lightroom catalog '%s': %s", path, exc)
        sys.exit(1)
    log.info("Opened catalog: %s", path)
    return lrdb


def list_collections(lrdb: LRCatDB) -> list[tuple[str, int]]:
    """
    Return [(album_name, lr_collection_id), ...] for every non-group
    collection in the catalog (both standard and smart collections).

    Album names encode the full hierarchy separated by HIERARCHY_SEPARATOR,
    e.g. ['2023', 'Family', 'Christmas'] → '2023 > Family > Christmas'.
    """
    result: list[tuple[str, int]] = []
    for hname, id_local, creation_id in lrdb.hierarchical_collections():
        # Skip collection sets (folders) — they contain no photos directly
        if creation_id == LR_COLLECTION_SET:
            continue
        result.append((HIERARCHY_SEPARATOR.join(hname), id_local))
    return result


def get_collection_assets(
    lrdb: LRCatDB, collection_id: int
) -> list[tuple[str, str]]:
    """
    Return [(filename_with_ext, lr_uuid), ...] for all photos/videos in a
    Lightroom collection identified by its local id.
    """
    try:
        rows = lrdb.lrphoto.select_generic(
            "name=basext,uuid",
            f"idcollection={collection_id}",
        ).fetchall()
    except LRSelectException as exc:
        log.warning("Error reading collection %d: %s", collection_id, exc)
        return []
    return [(row[0], row[1]) for row in rows if row[0] and row[1]]


# ---------------------------------------------------------------------------
# Asset resolution: Lightroom UUID → Immich asset UUID
# ---------------------------------------------------------------------------
def resolve_immich_id(
    lr_uuid: str,
    filename: str,
    immich: ImmichClient,
    redis_client: Optional[object],
    cache_ttl: int,
) -> Optional[str]:
    """
    Look up the Immich asset UUID for a Lightroom photo/video.

    Resolution order:
      1. Redis cache (keyed by LR UUID)
      2. Immich /search/metadata by originalFileName
         — stores result in cache for future runs
    Returns None if the asset cannot be found in Immich.
    """
    # 1. Cache lookup
    cached = cache_get(redis_client, lr_uuid)
    if cached:
        log.debug("Cache hit %s → %s", lr_uuid, cached)
        return cached

    # 2. Live Immich search
    assets = immich.search_by_filename(filename)
    if not assets:
        log.debug("Not found in Immich: '%s' (LR UUID %s)", filename, lr_uuid)
        return None

    if len(assets) > 1:
        log.warning(
            "Ambiguous: %d Immich assets match '%s'; using first result.",
            len(assets),
            filename,
        )

    immich_id: str = assets[0]["id"]
    cache_set(redis_client, lr_uuid, immich_id, cache_ttl)
    return immich_id


# ---------------------------------------------------------------------------
# Sync helpers
# ---------------------------------------------------------------------------
def _chunks(lst: list, n: int):
    """Yield successive chunks of *n* items from *lst*."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def sync_one_collection(
    album_name: str,
    collection_id: int,
    *,
    lrdb: LRCatDB,
    immich: ImmichClient,
    redis_client: Optional[object],
    cache_ttl: int,
    batch_size: int,
    dry_run: bool,
    existing_albums: dict[str, dict],
) -> None:
    """
    Sync a single Lightroom collection to the matching Immich album.

    Creates the album if it does not exist, then adds any assets that are
    not already present. Existing assets in the album are left untouched.
    """
    lr_assets = get_collection_assets(lrdb, collection_id)
    if not lr_assets:
        log.info("  (empty — skipping)")
        return

    log.info("  %d LR asset(s) in collection.", len(lr_assets))

    # ------------------------------------------------------------------
    # Resolve Lightroom filenames → Immich UUIDs
    # ------------------------------------------------------------------
    immich_ids: list[str] = []
    not_found = 0
    for filename, lr_uuid in lr_assets:
        iid = resolve_immich_id(lr_uuid, filename, immich, redis_client, cache_ttl)
        if iid:
            immich_ids.append(iid)
        else:
            not_found += 1

    # Deduplicate while preserving order (in case of LR virtual copies)
    immich_ids = list(dict.fromkeys(immich_ids))
    resolved = len(immich_ids)

    log.info(
        "  Resolved %d/%d asset(s).%s",
        resolved,
        len(lr_assets),
        f" ({not_found} not found in Immich)" if not_found else "",
    )

    if not immich_ids:
        log.warning("  No Immich assets resolved — skipping album.")
        return

    # ------------------------------------------------------------------
    # Dry-run output
    # ------------------------------------------------------------------
    if dry_run:
        verb = "update" if album_name in existing_albums else "create"
        log.info(
            "  [DRY RUN] Would %s album '%s' with %d asset(s).",
            verb,
            album_name,
            resolved,
        )
        return

    # ------------------------------------------------------------------
    # Create album if it doesn't already exist
    # ------------------------------------------------------------------
    if album_name not in existing_albums:
        log.info("  Creating album '%s'.", album_name)
        album = immich.create_album(album_name)
        existing_albums[album_name] = album
        album_id: str = album["id"]
    else:
        album_id = existing_albums[album_name]["id"]
        log.info("  Album exists (id=%s).", album_id)

    # ------------------------------------------------------------------
    # Skip assets already in the album
    # ------------------------------------------------------------------
    info = immich.get_album_info(album_id)
    existing_ids: set[str] = {a["id"] for a in info.get("assets", [])}
    new_ids = [x for x in immich_ids if x not in existing_ids]

    if not new_ids:
        log.info("  Album is already up to date.")
        return

    # ------------------------------------------------------------------
    # Add new assets in batches
    # ------------------------------------------------------------------
    log.info("  Adding %d new asset(s) in batch(es) of %d.", len(new_ids), batch_size)
    added = 0
    for chunk in _chunks(new_ids, batch_size):
        results = immich.add_assets_to_album(album_id, chunk)
        added += sum(1 for r in results if r.get("success"))

    log.info("  Done — %d asset(s) added.", added)


# ---------------------------------------------------------------------------
# Main sync loop
# ---------------------------------------------------------------------------
def run_sync(
    lrdb: LRCatDB,
    immich: ImmichClient,
    redis_client: Optional[object],
    *,
    cache_ttl: int,
    batch_size: int,
    dry_run: bool,
    collection_filter: Optional[str],
) -> None:
    """Iterate over all Lightroom collections and mirror each into Immich."""
    # Fetch existing albums once and keep a name → album dict for quick lookup
    existing_albums: dict[str, dict] = {
        a["albumName"]: a for a in immich.get_all_albums()
    }
    log.info("Immich: %d existing album(s).", len(existing_albums))

    collections = list_collections(lrdb)

    if collection_filter:
        fl = collection_filter.lower()
        before = len(collections)
        collections = [(n, c) for n, c in collections if fl in n.lower()]
        log.info(
            "Filter '%s': %d/%d collection(s) selected.",
            collection_filter,
            len(collections),
            before,
        )

    log.info("Syncing %d Lightroom collection(s).", len(collections))

    for album_name, collection_id in collections:
        log.info("┌─ '%s' (LR id=%d)", album_name, collection_id)
        try:
            sync_one_collection(
                album_name,
                collection_id,
                lrdb=lrdb,
                immich=immich,
                redis_client=redis_client,
                cache_ttl=cache_ttl,
                batch_size=batch_size,
                dry_run=dry_run,
                existing_albums=existing_albums,
            )
        except requests.HTTPError as exc:
            log.error("  HTTP error for '%s': %s", album_name, exc)
        except Exception as exc:  # noqa: BLE001 — log and continue
            log.error(
                "  Unexpected error for '%s': %s", album_name, exc, exc_info=True
            )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Lightroom Classic Collections → Immich albums",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--catalog",
        default=os.environ.get("LIGHTROOM_CATALOG", ""),
        metavar="PATH",
        help="Path to the Lightroom .lrcat file (overrides LIGHTROOM_CATALOG env var).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "0") == "1",
        help="Preview changes without applying them.",
    )
    parser.add_argument(
        "--collection",
        metavar="NAME",
        default=None,
        help=(
            "Only sync collections whose name contains NAME "
            "(case-insensitive substring match)."
        ),
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help=(
            "Run even if Redis is unavailable. "
            "Every asset will be resolved via the Immich API on every run, "
            "which is significantly slower for large catalogs."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug-level logging.",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ------------------------------------------------------------------
    # Resolve and validate required configuration
    # ------------------------------------------------------------------
    catalog = args.catalog
    immich_url = os.environ.get("IMMICH_API_URL", "").rstrip("/")
    immich_key = os.environ.get("IMMICH_API_KEY", "")
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    cache_ttl = int(os.environ.get("REDIS_CACHE_TTL", "604800"))
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))

    errors: list[str] = []
    if not catalog:
        errors.append("Lightroom catalog path is required (set LIGHTROOM_CATALOG or use --catalog)")
    if not immich_url:
        errors.append("IMMICH_API_URL environment variable is required")
    if not immich_key:
        errors.append("IMMICH_API_KEY environment variable is required")

    if errors:
        for err in errors:
            log.error(err)
        sys.exit(1)

    if args.dry_run:
        log.info("DRY RUN mode — no changes will be written to Immich.")

    # ------------------------------------------------------------------
    # Initialise clients and run
    # ------------------------------------------------------------------
    lrdb = open_catalog(catalog)
    immich = ImmichClient(immich_url, immich_key)
    redis_client, redis_error = build_redis_client(redis_url)
    if redis_client is None:
        if args.force:
            log.warning(
                "Redis unavailable (%s) — proceeding without cache (--force).",
                redis_error,
            )
        else:
            log.error(
                "Cannot connect to Redis (%s). "
                "Start Redis or pass --force to run without it.",
                redis_error,
            )
            sys.exit(1)

    run_sync(
        lrdb,
        immich,
        redis_client,
        cache_ttl=cache_ttl,
        batch_size=batch_size,
        dry_run=args.dry_run,
        collection_filter=args.collection,
    )

    log.info("Sync complete.")


if __name__ == "__main__":
    main()

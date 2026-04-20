#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import collections
import datetime as dt
import hashlib
import json
import math
import os
import plistlib
import sqlite3
import struct
import sys
import urllib.parse
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence

COCOA_EPOCH_OFFSET_SECONDS = 978_307_200
APPLE_NO_GPS_SENTINEL = -180
DEFAULT_LIBRARY_BASENAMES = [
    "Photos Library.photoslibrary",
    "Fotoğraflar Arşivi.photoslibrary",
]
DEFAULT_BATCH_SIZE = 1_000
OCR_CATEGORY = 1203
SCENE_SEARCH_CATEGORY = 1501

AESTHETIC_COLUMNS = [
    "ZFAILURESCORE",
    "ZHARMONIOUSCOLORSCORE",
    "ZIMMERSIVENESSSCORE",
    "ZINTERESTINGSUBJECTSCORE",
    "ZINTRUSIVEOBJECTPRESENCESCORE",
    "ZLIVELYCOLORSCORE",
    "ZLOWLIGHT",
    "ZNOISESCORE",
    "ZPLEASANTCAMERATILTSCORE",
    "ZPLEASANTCOMPOSITIONSCORE",
    "ZPLEASANTLIGHTINGSCORE",
    "ZPLEASANTPATTERNSCORE",
    "ZPLEASANTPERSPECTIVESCORE",
    "ZPLEASANTPOSTPROCESSINGSCORE",
    "ZPLEASANTREFLECTIONSSCORE",
    "ZPLEASANTSYMMETRYSCORE",
    "ZSHARPLYFOCUSEDSUBJECTSCORE",
    "ZTASTEFULLYBLURREDSCORE",
    "ZWELLCHOSENSUBJECTSCORE",
    "ZWELLFRAMEDSUBJECTSCORE",
    "ZWELLTIMEDSHOTSCORE",
]

BUSINESS_CACHE_PATHS = {
    "POI": "private/com.apple.photoanalysisd/caches/graph/CLSBusinessCategoryCache.POI.sqlite",
    "NATURE": "private/com.apple.photoanalysisd/caches/graph/CLSBusinessCategoryCache.Nature.sqlite",
    "AOI": "private/com.apple.photoanalysisd/caches/graph/CLSBusinessCategoryCache.AOI.sqlite",
    "ROI": "private/com.apple.photoanalysisd/caches/graph/CLSBusinessCategoryCache.ROI.sqlite",
}

PAYLOAD_TYPE_OVERRIDES = {
    "ZCHARACTERRECOGNITIONDATA": "structured_ocr",
    "ZMACHINEREADABLECODEDATA": "machine_readable_code",
    "ZTEXTUNDERSTANDINGDATA": "text_understanding",
    "ZOBJECTSALIENCYRECTSDATA": "object_saliency_rects",
    "ZREVERSELOCATIONDATA": "reverse_location",
    "ZSHIFTEDLOCATIONDATA": "shifted_location",
    "ZPLACEANNOTATIONDATA": "place_annotation",
    "ZFACEREGIONS": "face_regions",
    "ZDISTANCEIDENTITY": "distance_identity",
    "ZORIGINALHASH": "original_hash",
    "ZLIBRARYSCOPEASSETCONTRIBUTORSTOUPDATE": "library_scope_asset_contributors",
    "ZDATA": "sceneprint",
    "ZDUPLICATEMATCHINGDATA": "duplicate_matching",
    "ZDUPLICATEMATCHINGALTERNATEDATA": "duplicate_matching_alternate",
    "ZVISUALSEARCHDATA": "visual_search",
}


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\x00", "").strip()
    return text or None


def clean_whitespace(value: str | None) -> str | None:
    if not value:
        return None
    compact = " ".join(value.split())
    return compact or None


def to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_cocoa_date(value: Any) -> dt.datetime | None:
    number = to_float(value)
    if number in (None, 0):
        return None
    unix_seconds = number + COCOA_EPOCH_OFFSET_SECONDS
    try:
        return dt.datetime.fromtimestamp(unix_seconds, tz=dt.timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def normalize_gps(lat: Any, lng: Any) -> tuple[float, float] | None:
    latitude = to_float(lat)
    longitude = to_float(lng)
    if latitude is None or longitude is None:
        return None
    if latitude == APPLE_NO_GPS_SENTINEL and longitude == APPLE_NO_GPS_SENTINEL:
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    return latitude, longitude


def infer_media_kind(filename: str | None, uti: str | None, zkind: int | None) -> str:
    lower_filename = (filename or "").lower()
    lower_uti = (uti or "").lower()
    image_exts = (".jpg", ".jpeg", ".heic", ".heif", ".png", ".gif", ".webp", ".tif", ".tiff", ".bmp")
    video_exts = (".mov", ".mp4", ".m4v", ".avi", ".mkv", ".mts", ".3gp", ".webm")
    if lower_filename.endswith(video_exts) or lower_uti.startswith("public.movie") or lower_uti.startswith("public.video"):
        return "VIDEO"
    if lower_filename.endswith(image_exts) or lower_uti.startswith("public.image"):
        return "IMAGE"
    if zkind in {1, 8}:
        return "VIDEO"
    return "IMAGE"


def sha1_text(*parts: Any) -> str:
    joined = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def json_default(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=json_default, sort_keys=True)


def sha1_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha1(value).hexdigest()


def uuid_from_blob(value: bytes | None) -> str | None:
    if not value or len(value) != 16:
        return None
    try:
        return str(uuid.UUID(bytes=value)).upper()
    except (ValueError, AttributeError):
        return None


def blob_format(value: bytes | None) -> str | None:
    if value is None:
        return None
    if value[:8] == b"bplist00":
        return "bplist00"
    return "raw"


def normalize_plist_value(value: Any) -> Any:
    if isinstance(value, plistlib.UID):
        return {"$uid": value.data}
    if isinstance(value, bytes):
        return {"$bytesBase64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): normalize_plist_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_plist_value(item) for item in value]
    return value


def decode_bplist_to_jsonable(value: bytes | None) -> Any | None:
    if not value or value[:8] != b"bplist00":
        return None
    try:
        return normalize_plist_value(plistlib.loads(value))
    except Exception:
        return None


def float32le_vector_metadata(value: bytes | None) -> dict[str, Any] | None:
    if not value or len(value) % 4 != 0:
        return None
    dimensions = len(value) // 4
    try:
        numbers = struct.unpack(f"<{dimensions}f", value)
    except struct.error:
        return None
    if not all(math.isfinite(number) for number in numbers):
        return None
    norm = math.sqrt(sum(number * number for number in numbers))
    return {
        "format": "float32le",
        "dimensions": dimensions,
        "l2_norm": norm,
    }


def payload_type_for_column(column: str) -> str:
    return PAYLOAD_TYPE_OVERRIDES.get(column, column.removeprefix("Z").lower())


def row_scalar_metadata(row: sqlite3.Row, skip_keys: Sequence[str] = ()) -> dict[str, Any]:
    result: dict[str, Any] = {}
    skip = set(skip_keys)
    for key in row.keys():
        if key in skip:
            continue
        value = row[key]
        if isinstance(value, (bytes, bytearray)):
            continue
        result[key] = value
    return result


def build_asset_blob_payload_row(
    apple_uuid: str,
    payload_type: str,
    source_table: str,
    source_pk: int | None,
    blob_column: str,
    blob_value: bytes,
    metadata: dict[str, Any] | None = None,
    *,
    include_raw_blob: bool = False,
) -> dict[str, Any]:
    decoded = decode_bplist_to_jsonable(blob_value)
    return {
        "row_key": sha1_text(apple_uuid, payload_type, source_table, source_pk, blob_column),
        "apple_uuid": apple_uuid,
        "payload_type": payload_type,
        "source_table": source_table,
        "source_pk": source_pk,
        "blob_column": blob_column,
        "byte_length": len(blob_value),
        "blob_sha1": sha1_bytes(blob_value),
        "blob_format": blob_format(blob_value),
        "decoded_json": json_dumps(decoded) if decoded is not None else None,
        "raw_blob": blob_value if include_raw_blob else None,
        "metadata_json": json_dumps(metadata or {}),
    }


def chunked(items: Sequence[dict[str, Any]], size: int) -> Iterator[Sequence[dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def csv_quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def discover_photos_library(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if path.exists():
            return path
        raise FileNotFoundError(f"Apple Photos library not found: {path}")

    pictures_dir = Path.home() / "Pictures"
    for basename in DEFAULT_LIBRARY_BASENAMES:
        preferred = pictures_dir / basename
        if preferred.exists():
            return preferred

    candidates = sorted(
        (p for p in pictures_dir.glob("*.photoslibrary") if p.is_dir()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        return candidates[0]

    raise FileNotFoundError(
        f"No .photoslibrary bundle found under {pictures_dir}. Pass --library-path explicitly."
    )


def sqlite_uri(path: Path) -> str:
    return f"file:{urllib.parse.quote(str(path), safe='/')}?mode=ro"


def open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(sqlite_uri(path), uri=True)
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            f"Cannot open SQLite database at {path}. "
            "Close Apple Photos and grant Full Disk Access to your terminal/Python process."
        ) from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = 1")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


class SqliteInspector:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._tables: set[str] | None = None
        self._columns: dict[str, list[str]] = {}

    def tables(self) -> set[str]:
        if self._tables is None:
            rows = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            ).fetchall()
            self._tables = {str(row[0]) for row in rows}
        return self._tables

    def has_table(self, table: str) -> bool:
        return table in self.tables()

    def columns(self, table: str) -> list[str]:
        if table not in self._columns:
            rows = self.conn.execute(f"PRAGMA table_info({csv_quote_identifier(table)})").fetchall()
            self._columns[table] = [str(row[1]) for row in rows]
        return self._columns[table]

    def choose_column(
        self,
        table: str,
        candidates: Sequence[str] = (),
        contains_all: Sequence[str] = (),
        excludes: Sequence[str] = (),
    ) -> str | None:
        columns = self.columns(table)
        column_set = set(columns)
        for candidate in candidates:
            if candidate in column_set:
                return candidate
        lowered_excludes = [item.lower() for item in excludes]
        for column in columns:
            lowered = column.lower()
            if lowered_excludes and any(ex in lowered for ex in lowered_excludes):
                continue
            if all(token.lower() in lowered for token in contains_all):
                return column
        return None

    def table_names_matching(self, substring_tokens: Sequence[str]) -> list[str]:
        matches: list[str] = []
        for table in self.tables():
            lowered = table.lower()
            if all(token.lower() in lowered for token in substring_tokens):
                matches.append(table)
        return sorted(matches)


@dataclass
class AssetRecord:
    asset_pk: int
    apple_uuid: str
    local_identifier: str
    canonical_media_asset_id: str | None
    filename: str | None
    original_filename: str | None
    title: str | None
    caption: str | None
    ocr_text: str | None
    taken_at: dt.datetime | None
    timezone_offset_min: int | None
    gps_lat: float | None
    gps_lng: float | None
    media_kind: str
    favorite_flag: bool
    managed_filename: str | None
    directory_shard: str | None
    uniform_type_identifier: str | None
    original_file_size_bytes: int | None
    original_width: int | None
    original_height: int | None
    display_width: int | None
    display_height: int | None
    metadata_json: dict[str, Any] = field(default_factory=dict)
    aesthetic_scores: dict[str, float | None] = field(default_factory=dict)
    person_summaries: list[dict[str, Any]] = field(default_factory=list)
    memory_refs: list[dict[str, Any]] = field(default_factory=list)
    album_names: list[str] = field(default_factory=list)
    keyword_names: list[str] = field(default_factory=list)

    def refresh_metadata(self) -> None:
        summary = {
            "scanner": "apple_photos_stage1_sqlite",
            "assetPk": self.asset_pk,
            "appleUuid": self.apple_uuid,
            "localIdentifier": self.local_identifier,
            "managedFilename": self.managed_filename,
            "directoryShard": self.directory_shard,
            "uniformTypeIdentifier": self.uniform_type_identifier,
            "favoriteFlag": self.favorite_flag,
            "title": self.title,
            "persons": self.person_summaries,
            "memoryRefs": self.memory_refs,
            "albumNames": sorted(set(filter(None, self.album_names))),
            "keywordNames": sorted(set(filter(None, self.keyword_names))),
            "aestheticScores": {k: v for k, v in self.aesthetic_scores.items() if v is not None},
        }
        self.metadata_json = {k: v for k, v in summary.items() if v not in (None, [], {}, "")}

    def to_stage_row(self) -> dict[str, Any]:
        self.refresh_metadata()
        return {
            "apple_uuid": self.apple_uuid,
            "local_identifier": self.local_identifier,
            "canonical_media_asset_id": self.canonical_media_asset_id,
            "filename": self.filename,
            "original_filename": self.original_filename,
            "title": self.title,
            "caption": self.caption,
            "ocr_text": self.ocr_text,
            "taken_at": self.taken_at,
            "timezone_offset_min": self.timezone_offset_min,
            "gps_lat": self.gps_lat,
            "gps_lng": self.gps_lng,
            "media_kind": self.media_kind,
            "favorite_flag": self.favorite_flag,
            "managed_filename": self.managed_filename,
            "directory_shard": self.directory_shard,
            "uniform_type_identifier": self.uniform_type_identifier,
            "original_file_size_bytes": self.original_file_size_bytes,
            "original_width": self.original_width,
            "original_height": self.original_height,
            "display_width": self.display_width,
            "display_height": self.display_height,
            "metadata_json": json_dumps(self.metadata_json),
        }


def build_asset_query(inspector: SqliteInspector) -> str:
    asset_cols = set(inspector.columns("ZASSET"))
    additional_cols = set(inspector.columns("ZADDITIONALASSETATTRIBUTES")) if inspector.has_table("ZADDITIONALASSETATTRIBUTES") else set()
    description_cols = set(inspector.columns("ZASSETDESCRIPTION")) if inspector.has_table("ZASSETDESCRIPTION") else set()
    computed_cols = set(inspector.columns("ZCOMPUTEDASSETATTRIBUTES")) if inspector.has_table("ZCOMPUTEDASSETATTRIBUTES") else set()

    selected = [
        'a.Z_PK AS "asset_pk"',
        'a.ZUUID AS "apple_uuid"',
    ]
    for col in [
        "ZFILENAME",
        "ZDIRECTORY",
        "ZDATECREATED",
        "ZLATITUDE",
        "ZLONGITUDE",
        "ZKIND",
        "ZWIDTH",
        "ZHEIGHT",
        "ZFAVORITE",
        "ZUNIFORMTYPEIDENTIFIER",
    ]:
        if col in asset_cols:
            selected.append(f'a.{col} AS "{col}"')

    if additional_cols:
        for col in [
            "ZORIGINALFILENAME",
            "ZTITLE",
            "ZACCESSIBILITYDESCRIPTION",
            "ZINFERREDTIMEZONEOFFSET",
            "ZTIMEZONENAME",
            "ZORIGINALFILESIZE",
            "ZORIGINALWIDTH",
            "ZORIGINALHEIGHT",
        ]:
            if col in additional_cols:
                selected.append(f'ad.{col} AS "{col}"')

    if description_cols and "ZLONGDESCRIPTION" in description_cols:
        selected.append('descr.ZLONGDESCRIPTION AS "ZLONGDESCRIPTION"')

    for col in AESTHETIC_COLUMNS:
        if col in computed_cols:
            selected.append(f'comp.{col} AS "{col}"')

    additional_fk = "ZADDITIONALATTRIBUTES" if "ZADDITIONALATTRIBUTES" in asset_cols else None
    computed_fk = None
    for candidate in ("ZCOMPUTEDASSETATTRIBUTES", "ZCOMPUTEDATTRIBUTES"):
        if candidate in asset_cols:
            computed_fk = candidate
            break
    if computed_fk is None:
        for col in asset_cols:
            upper = col.upper()
            if "COMPUTED" in upper and "ATTR" in upper:
                computed_fk = col
                break

    joins: list[str] = []
    if additional_fk and additional_cols:
        joins.append(f"LEFT JOIN ZADDITIONALASSETATTRIBUTES ad ON ad.Z_PK = a.{additional_fk}")
    if description_cols and additional_cols and "ZASSETATTRIBUTES" in description_cols:
        joins.append("LEFT JOIN ZASSETDESCRIPTION descr ON descr.ZASSETATTRIBUTES = ad.Z_PK")
    if computed_fk and computed_cols:
        joins.append(f"LEFT JOIN ZCOMPUTEDASSETATTRIBUTES comp ON comp.Z_PK = a.{computed_fk}")

    where_clauses = ["a.Z_PK > ?"]
    if "ZTRASHEDSTATE" in asset_cols:
        where_clauses.append("COALESCE(a.ZTRASHEDSTATE, 0) = 0")
    if "ZHIDDEN" in asset_cols:
        where_clauses.append("COALESCE(a.ZHIDDEN, 0) = 0")

    return f"""
        SELECT
          {", ".join(selected)}
        FROM ZASSET a
        {" ".join(joins)}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY a.Z_PK ASC
        LIMIT ?
    """


def extract_assets(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    origin_map: dict[str, str],
    batch_size: int,
) -> tuple[list[AssetRecord], dict[int, AssetRecord], dict[str, AssetRecord]]:
    if not inspector.has_table("ZASSET"):
        raise RuntimeError("Photos.sqlite is missing ZASSET.")

    query = build_asset_query(inspector)
    assets: list[AssetRecord] = []
    assets_by_pk: dict[int, AssetRecord] = {}
    assets_by_uuid: dict[str, AssetRecord] = {}
    last_pk = 0

    while True:
        rows = conn.execute(query, (last_pk, batch_size)).fetchall()
        if not rows:
            break
        for row in rows:
            asset_pk = int(row["asset_pk"])
            apple_uuid = clean_string(row["apple_uuid"])
            if not apple_uuid:
                continue
            local_identifier = f"{apple_uuid}/L0/001"
            filename = clean_string(row["ZORIGINALFILENAME"] if "ZORIGINALFILENAME" in row.keys() else None) or clean_string(
                row["ZFILENAME"] if "ZFILENAME" in row.keys() else None
            )
            title = clean_string(row["ZTITLE"] if "ZTITLE" in row.keys() else None)
            caption = clean_whitespace(
                clean_string(row["ZLONGDESCRIPTION"] if "ZLONGDESCRIPTION" in row.keys() else None)
                or clean_string(row["ZACCESSIBILITYDESCRIPTION"] if "ZACCESSIBILITYDESCRIPTION" in row.keys() else None)
                or title
            )
            taken_at = parse_cocoa_date(row["ZDATECREATED"] if "ZDATECREATED" in row.keys() else None)
            gps = normalize_gps(
                row["ZLATITUDE"] if "ZLATITUDE" in row.keys() else None,
                row["ZLONGITUDE"] if "ZLONGITUDE" in row.keys() else None,
            )
            timezone_offset_seconds = to_float(
                row["ZINFERREDTIMEZONEOFFSET"] if "ZINFERREDTIMEZONEOFFSET" in row.keys() else None
            )
            timezone_offset_min = (
                None if timezone_offset_seconds is None else int(round(timezone_offset_seconds / 60.0))
            )
            aesthetic_scores = {
                col: to_float(row[col]) for col in AESTHETIC_COLUMNS if col in row.keys()
            }
            asset = AssetRecord(
                asset_pk=asset_pk,
                apple_uuid=apple_uuid,
                local_identifier=local_identifier,
                canonical_media_asset_id=origin_map.get(apple_uuid) or origin_map.get(local_identifier),
                filename=clean_string(row["ZFILENAME"] if "ZFILENAME" in row.keys() else None),
                original_filename=clean_string(row["ZORIGINALFILENAME"] if "ZORIGINALFILENAME" in row.keys() else None),
                title=title,
                caption=caption,
                ocr_text=None,
                taken_at=taken_at,
                timezone_offset_min=timezone_offset_min,
                gps_lat=gps[0] if gps else None,
                gps_lng=gps[1] if gps else None,
                media_kind=infer_media_kind(
                    filename=filename,
                    uti=clean_string(row["ZUNIFORMTYPEIDENTIFIER"] if "ZUNIFORMTYPEIDENTIFIER" in row.keys() else None),
                    zkind=to_int(row["ZKIND"] if "ZKIND" in row.keys() else None),
                ),
                favorite_flag=to_int(row["ZFAVORITE"] if "ZFAVORITE" in row.keys() else None) == 1,
                managed_filename=clean_string(row["ZFILENAME"] if "ZFILENAME" in row.keys() else None),
                directory_shard=clean_string(row["ZDIRECTORY"] if "ZDIRECTORY" in row.keys() else None),
                uniform_type_identifier=clean_string(
                    row["ZUNIFORMTYPEIDENTIFIER"] if "ZUNIFORMTYPEIDENTIFIER" in row.keys() else None
                ),
                original_file_size_bytes=to_int(
                    row["ZORIGINALFILESIZE"] if "ZORIGINALFILESIZE" in row.keys() else None
                ),
                original_width=to_int(row["ZORIGINALWIDTH"] if "ZORIGINALWIDTH" in row.keys() else None),
                original_height=to_int(row["ZORIGINALHEIGHT"] if "ZORIGINALHEIGHT" in row.keys() else None),
                display_width=to_int(row["ZWIDTH"] if "ZWIDTH" in row.keys() else None),
                display_height=to_int(row["ZHEIGHT"] if "ZHEIGHT" in row.keys() else None),
                aesthetic_scores=aesthetic_scores,
            )
            asset.refresh_metadata()
            assets.append(asset)
            assets_by_pk[asset.asset_pk] = asset
            assets_by_uuid[asset.apple_uuid] = asset
        last_pk = int(rows[-1]["asset_pk"])
        eprint(f"[photos] extracted {len(assets):,} assets so far...")
        if len(rows) < batch_size:
            break

    return assets, assets_by_pk, assets_by_uuid


def choose_face_meta_columns(columns: Sequence[str], asset_col: str, person_col: str) -> list[str]:
    result: list[str] = []
    for column in columns:
        if column in {"Z_PK", asset_col, person_col}:
            continue
        lowered = column.lower()
        if any(token in lowered for token in ("bound", "rect", "center", "size", "roll", "yaw", "pitch")):
            result.append(column)
    return result


def extract_people(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not (inspector.has_table("ZDETECTEDFACE") and inspector.has_table("ZPERSON")):
        return [], []

    face_asset_col = inspector.choose_column(
        "ZDETECTEDFACE",
        candidates=("ZASSETFORFACE", "ZASSET"),
        contains_all=("asset",),
    )
    face_person_col = inspector.choose_column(
        "ZDETECTEDFACE",
        candidates=("ZPERSONFORFACE", "ZPERSON"),
        contains_all=("person",),
    )
    person_uuid_col = inspector.choose_column(
        "ZPERSON",
        candidates=("ZPERSONUUID", "ZUUID"),
        contains_all=("uuid",),
    )
    person_name_col = inspector.choose_column(
        "ZPERSON",
        candidates=("ZFULLNAME", "ZDISPLAYNAME", "ZNAME"),
        contains_all=("name",),
        excludes=("cloud", "phonetic"),
    )
    if not face_asset_col or not face_person_col:
        return [], []

    face_meta_cols = choose_face_meta_columns(inspector.columns("ZDETECTEDFACE"), face_asset_col, face_person_col)
    select_parts = [
        "f.Z_PK AS face_pk",
        f"f.{face_asset_col} AS asset_pk",
        f"f.{face_person_col} AS person_pk",
    ]
    if person_uuid_col:
        select_parts.append(f"p.{person_uuid_col} AS person_uuid")
    if person_name_col:
        select_parts.append(f"p.{person_name_col} AS person_name")
    for col in face_meta_cols:
        select_parts.append(f"f.{col} AS {col}")

    rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM ZDETECTEDFACE f
        LEFT JOIN ZPERSON p ON p.Z_PK = f.{face_person_col}
        """
    ).fetchall()

    assets_by_uuid = {asset.apple_uuid: asset for asset in assets_by_pk.values()}
    person_aggregate: dict[str, dict[str, Any]] = {}
    asset_person_faces: dict[tuple[str, str], list[dict[str, Any]]] = collections.defaultdict(list)

    for row in rows:
        asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
        if not asset:
            continue
        person_pk = to_int(row["person_pk"])
        person_uuid = clean_string(row["person_uuid"] if "person_uuid" in row.keys() else None)
        person_key = person_uuid or (f"pk:{person_pk}" if person_pk is not None else None)
        if not person_key:
            continue

        person_name = clean_string(row["person_name"] if "person_name" in row.keys() else None)
        face_meta = {"facePk": to_int(row["face_pk"])}
        for col in face_meta_cols:
            face_meta[col] = row[col]
        asset_person_faces[(asset.apple_uuid, person_key)].append(face_meta)

        existing = person_aggregate.get(person_key)
        if existing is None:
            existing = {
                "person_key": person_key,
                "person_uuid": person_uuid,
                "person_pk": person_pk,
                "full_name": person_name,
                "display_name": person_name,
                "face_count": 0,
                "is_named": bool(person_name),
                "metadata_json": json_dumps(
                    {
                        "source": "apple_photos",
                        "personPk": person_pk,
                        "personUuid": person_uuid,
                    }
                ),
            }
            person_aggregate[person_key] = existing
        existing["face_count"] += 1
        if person_name and not existing["full_name"]:
            existing["full_name"] = person_name
            existing["display_name"] = person_name
            existing["is_named"] = True

    asset_people_rows: list[dict[str, Any]] = []
    for (apple_uuid, person_key), faces in asset_person_faces.items():
        asset = assets_by_uuid.get(apple_uuid)
        if asset is None:
            continue
        person = person_aggregate[person_key]
        asset.person_summaries.append(
            {
                "personKey": person_key,
                "name": person["display_name"],
                "faceCount": len(faces),
            }
        )
        asset_people_rows.append(
            {
                "apple_uuid": apple_uuid,
                "person_key": person_key,
                "person_uuid": person["person_uuid"],
                "person_pk": person["person_pk"],
                "display_name": person["display_name"],
                "face_count": len(faces),
                "face_rows_json": json_dumps(faces),
            }
        )

    people_rows = sorted(person_aggregate.values(), key=lambda row: row["person_key"])
    return people_rows, asset_people_rows


def extract_asset_resources(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> list[dict[str, Any]]:
    if not inspector.has_table("ZINTERNALRESOURCE"):
        return []

    asset_col = inspector.choose_column("ZINTERNALRESOURCE", candidates=("ZASSET",), contains_all=("asset",))
    if not asset_col:
        return []

    columns = inspector.columns("ZINTERNALRESOURCE")
    select_parts = ['Z_PK AS "resource_pk"', f"{asset_col} AS asset_pk"]
    wanted = [
        "ZRESOURCETYPE",
        "ZVERSION",
        "ZDATALENGTH",
        "ZLOCALAVAILABILITY",
        "ZLOCALAVAILABILITYTARGET",
        "ZREMOTEAVAILABILITY",
        "ZREMOTEAVAILABILITYTARGET",
        "ZCLOUDLOCALSTATE",
        "ZCLOUDSOURCETYPE",
        "ZRECIPEID",
        "ZSIDECARINDEX",
        "ZFILEID",
        "ZDATASTORECLASSID",
        "ZDATASTORESUBTYPE",
        "ZCOMPACTUTI",
        "ZCODECFOURCHARCODENAME",
        "ZFINGERPRINT",
        "ZSTABLEHASH",
        "ZDATASTOREKEYDATA",
        "ZUNORIENTEDWIDTH",
        "ZUNORIENTEDHEIGHT",
        "ZUTICONFORMANCEHINT",
        "ZCLOUDLASTONDEMANDDOWNLOADDATE",
        "ZCLOUDMASTERDATECREATED",
    ]
    select_parts.extend(f"{column} AS {column}" for column in wanted if column in columns)
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZINTERNALRESOURCE").fetchall()

    resource_rows: list[dict[str, Any]] = []
    for row in rows:
        asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
        if asset is None:
            continue
        datastore_key = row["ZDATASTOREKEYDATA"] if "ZDATASTOREKEYDATA" in row.keys() else None
        metadata = row_scalar_metadata(row, ("resource_pk", "asset_pk", "ZDATASTOREKEYDATA"))
        resource_pk = to_int(row["resource_pk"])
        resource_rows.append(
            {
                "row_key": sha1_text(asset.apple_uuid, resource_pk),
                "apple_uuid": asset.apple_uuid,
                "resource_pk": resource_pk,
                "resource_type": to_int(row["ZRESOURCETYPE"] if "ZRESOURCETYPE" in row.keys() else None),
                "version": to_int(row["ZVERSION"] if "ZVERSION" in row.keys() else None),
                "data_length": to_int(row["ZDATALENGTH"] if "ZDATALENGTH" in row.keys() else None),
                "local_availability": to_int(row["ZLOCALAVAILABILITY"] if "ZLOCALAVAILABILITY" in row.keys() else None),
                "local_availability_target": to_int(
                    row["ZLOCALAVAILABILITYTARGET"] if "ZLOCALAVAILABILITYTARGET" in row.keys() else None
                ),
                "remote_availability": to_int(row["ZREMOTEAVAILABILITY"] if "ZREMOTEAVAILABILITY" in row.keys() else None),
                "remote_availability_target": to_int(
                    row["ZREMOTEAVAILABILITYTARGET"] if "ZREMOTEAVAILABILITYTARGET" in row.keys() else None
                ),
                "cloud_local_state": to_int(row["ZCLOUDLOCALSTATE"] if "ZCLOUDLOCALSTATE" in row.keys() else None),
                "cloud_source_type": to_int(row["ZCLOUDSOURCETYPE"] if "ZCLOUDSOURCETYPE" in row.keys() else None),
                "recipe_id": to_int(row["ZRECIPEID"] if "ZRECIPEID" in row.keys() else None),
                "sidecar_index": to_int(row["ZSIDECARINDEX"] if "ZSIDECARINDEX" in row.keys() else None),
                "file_id": to_int(row["ZFILEID"] if "ZFILEID" in row.keys() else None),
                "datastore_class_id": to_int(
                    row["ZDATASTORECLASSID"] if "ZDATASTORECLASSID" in row.keys() else None
                ),
                "datastore_subtype": to_int(
                    row["ZDATASTORESUBTYPE"] if "ZDATASTORESUBTYPE" in row.keys() else None
                ),
                "compact_uti": clean_string(row["ZCOMPACTUTI"] if "ZCOMPACTUTI" in row.keys() else None),
                "codec_fourcc": clean_string(
                    row["ZCODECFOURCHARCODENAME"] if "ZCODECFOURCHARCODENAME" in row.keys() else None
                ),
                "fingerprint": clean_string(row["ZFINGERPRINT"] if "ZFINGERPRINT" in row.keys() else None),
                "stable_hash": clean_string(row["ZSTABLEHASH"] if "ZSTABLEHASH" in row.keys() else None),
                "datastore_key_hex": datastore_key.hex() if isinstance(datastore_key, (bytes, bytearray)) else None,
                "metadata_json": json_dumps(metadata),
            }
        )
    return resource_rows


def extract_asset_blob_payloads(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> list[dict[str, Any]]:
    payload_rows: list[dict[str, Any]] = []
    additional_asset_map: dict[int, str] = {}

    if inspector.has_table("ZADDITIONALASSETATTRIBUTES"):
        asset_col = inspector.choose_column("ZADDITIONALASSETATTRIBUTES", candidates=("ZASSET",), contains_all=("asset",))
        if asset_col:
            columns = inspector.columns("ZADDITIONALASSETATTRIBUTES")
            blob_columns = [
                column
                for column in [
                    "ZLIBRARYSCOPEASSETCONTRIBUTORSTOUPDATE",
                    "ZDISTANCEIDENTITY",
                    "ZFACEREGIONS",
                    "ZOBJECTSALIENCYRECTSDATA",
                    "ZORIGINALHASH",
                    "ZPLACEANNOTATIONDATA",
                    "ZREVERSELOCATIONDATA",
                    "ZSHIFTEDLOCATIONDATA",
                ]
                if column in columns
            ]
            select_parts = ['Z_PK AS "source_pk"', f"{asset_col} AS asset_pk"] + [
                f"{column} AS {column}" for column in blob_columns
            ]
            rows = conn.execute(
                f"SELECT {', '.join(select_parts)} FROM ZADDITIONALASSETATTRIBUTES"
            ).fetchall()
            for row in rows:
                source_pk = to_int(row["source_pk"])
                asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
                if asset is None or source_pk is None:
                    continue
                additional_asset_map[source_pk] = asset.apple_uuid
                metadata = row_scalar_metadata(row, ("source_pk", "asset_pk", *blob_columns))
                for column in blob_columns:
                    blob_value = row[column]
                    if not isinstance(blob_value, (bytes, bytearray)):
                        continue
                    payload_rows.append(
                        build_asset_blob_payload_row(
                            asset.apple_uuid,
                            payload_type_for_column(column),
                            "ZADDITIONALASSETATTRIBUTES",
                            source_pk,
                            column,
                            bytes(blob_value),
                            metadata,
                        )
                    )

    if inspector.has_table("ZSCENEPRINT"):
        addl_col = inspector.choose_column(
            "ZSCENEPRINT",
            candidates=("ZADDITIONALASSETATTRIBUTES",),
            contains_all=("additional", "asset", "attributes"),
        )
        columns = inspector.columns("ZSCENEPRINT")
        blob_columns = [
            column
            for column in ["ZDATA", "ZDUPLICATEMATCHINGDATA", "ZDUPLICATEMATCHINGALTERNATEDATA"]
            if column in columns
        ]
        if addl_col and blob_columns:
            select_parts = ['Z_PK AS "source_pk"', f"{addl_col} AS additional_pk"] + [
                f"{column} AS {column}" for column in blob_columns
            ]
            rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZSCENEPRINT").fetchall()
            for row in rows:
                source_pk = to_int(row["source_pk"])
                additional_pk = to_int(row["additional_pk"])
                if source_pk is None or additional_pk is None:
                    continue
                apple_uuid = additional_asset_map.get(additional_pk)
                if not apple_uuid:
                    continue
                metadata = row_scalar_metadata(row, ("source_pk", "additional_pk", *blob_columns))
                metadata["additionalAssetAttributesPk"] = additional_pk
                for column in blob_columns:
                    blob_value = row[column]
                    if not isinstance(blob_value, (bytes, bytearray)):
                        continue
                    payload_rows.append(
                        build_asset_blob_payload_row(
                            apple_uuid,
                            payload_type_for_column(column if column != "ZDATA" else "ZDATA"),
                            "ZSCENEPRINT",
                            source_pk,
                            column,
                            bytes(blob_value),
                            metadata,
                        )
                    )

    if inspector.has_table("ZMEDIAANALYSISASSETATTRIBUTES"):
        asset_col = inspector.choose_column("ZMEDIAANALYSISASSETATTRIBUTES", candidates=("ZASSET",), contains_all=("asset",))
        ocr_fk_col = inspector.choose_column(
            "ZMEDIAANALYSISASSETATTRIBUTES",
            candidates=("ZCHARACTERRECOGNITIONATTRIBUTES",),
            contains_all=("character", "recognition", "attributes"),
        )
        visual_fk_col = inspector.choose_column(
            "ZMEDIAANALYSISASSETATTRIBUTES",
            candidates=("ZVISUALSEARCHATTRIBUTES",),
            contains_all=("visual", "search", "attributes"),
        )
        version_columns = [
            column
            for column in [
                "ZMEDIAANALYSISVERSION",
                "ZIMAGEEMBEDDINGVERSION",
                "ZVIDEOEMBEDDINGVERSION",
                "ZCHARACTERRECOGNITIONVERSION",
                "ZTEXTUNDERSTANDINGVERSION",
                "ZVISUALSEARCHVERSION",
                "ZVISUALSEARCHSTICKERCONFIDENCEVERSION",
            ]
            if column in inspector.columns("ZMEDIAANALYSISASSETATTRIBUTES")
        ]
        if asset_col:
            select_parts = ['Z_PK AS "source_pk"', f"{asset_col} AS asset_pk"]
            if ocr_fk_col:
                select_parts.append(f"{ocr_fk_col} AS ocr_attributes_pk")
            if visual_fk_col:
                select_parts.append(f"{visual_fk_col} AS visual_search_attributes_pk")
            select_parts.extend(f"{column} AS {column}" for column in version_columns)
            rows = conn.execute(
                f"SELECT {', '.join(select_parts)} FROM ZMEDIAANALYSISASSETATTRIBUTES"
            ).fetchall()
            ocr_asset_map: dict[int, tuple[str, dict[str, Any]]] = {}
            visual_asset_map: dict[int, tuple[str, dict[str, Any]]] = {}
            for row in rows:
                source_pk = to_int(row["source_pk"])
                asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
                if asset is None or source_pk is None:
                    continue
                metadata = row_scalar_metadata(
                    row,
                    ("source_pk", "asset_pk", "ocr_attributes_pk", "visual_search_attributes_pk"),
                )
                ocr_attributes_pk = to_int(row["ocr_attributes_pk"] if "ocr_attributes_pk" in row.keys() else None)
                visual_attributes_pk = to_int(
                    row["visual_search_attributes_pk"] if "visual_search_attributes_pk" in row.keys() else None
                )
                if ocr_attributes_pk is not None:
                    ocr_asset_map[ocr_attributes_pk] = (asset.apple_uuid, metadata)
                if visual_attributes_pk is not None:
                    visual_asset_map[visual_attributes_pk] = (asset.apple_uuid, metadata)

            if ocr_fk_col and inspector.has_table("ZCHARACTERRECOGNITIONATTRIBUTES"):
                columns = inspector.columns("ZCHARACTERRECOGNITIONATTRIBUTES")
                blob_columns = [
                    column
                    for column in [
                        "ZCHARACTERRECOGNITIONDATA",
                        "ZMACHINEREADABLECODEDATA",
                        "ZTEXTUNDERSTANDINGDATA",
                    ]
                    if column in columns
                ]
                select_parts = ['Z_PK AS "source_pk"'] + [f"{column} AS {column}" for column in blob_columns]
                scalar_columns = [
                    column
                    for column in ["ZALGORITHMVERSION", "ZADJUSTMENTVERSION", "ZMEDIAANALYSISASSETATTRIBUTES"]
                    if column in columns
                ]
                select_parts.extend(f"{column} AS {column}" for column in scalar_columns)
                rows = conn.execute(
                    f"SELECT {', '.join(select_parts)} FROM ZCHARACTERRECOGNITIONATTRIBUTES"
                ).fetchall()
                for row in rows:
                    source_pk = to_int(row["source_pk"])
                    if source_pk is None or source_pk not in ocr_asset_map:
                        continue
                    apple_uuid, media_metadata = ocr_asset_map[source_pk]
                    metadata = {
                        **media_metadata,
                        **row_scalar_metadata(row, ("source_pk", *blob_columns)),
                    }
                    for column in blob_columns:
                        blob_value = row[column]
                        if not isinstance(blob_value, (bytes, bytearray)):
                            continue
                        payload_rows.append(
                            build_asset_blob_payload_row(
                                apple_uuid,
                                payload_type_for_column(column),
                                "ZCHARACTERRECOGNITIONATTRIBUTES",
                                source_pk,
                                column,
                                bytes(blob_value),
                                metadata,
                            )
                        )

            if visual_fk_col and inspector.has_table("ZVISUALSEARCHATTRIBUTES"):
                columns = inspector.columns("ZVISUALSEARCHATTRIBUTES")
                if "ZVISUALSEARCHDATA" in columns:
                    select_parts = [
                        'Z_PK AS "source_pk"',
                        "ZVISUALSEARCHDATA AS ZVISUALSEARCHDATA",
                    ]
                    scalar_columns = [
                        column
                        for column in [
                            "ZALGORITHMVERSION",
                            "ZSTICKERCONFIDENCEALGORITHMVERSION",
                            "ZSTICKERCONFIDENCESCORE",
                            "ZADJUSTMENTVERSION",
                        ]
                        if column in columns
                    ]
                    select_parts.extend(f"{column} AS {column}" for column in scalar_columns)
                    rows = conn.execute(
                        f"SELECT {', '.join(select_parts)} FROM ZVISUALSEARCHATTRIBUTES"
                    ).fetchall()
                    for row in rows:
                        source_pk = to_int(row["source_pk"])
                        if source_pk is None or source_pk not in visual_asset_map:
                            continue
                        blob_value = row["ZVISUALSEARCHDATA"]
                        if not isinstance(blob_value, (bytes, bytearray)):
                            continue
                        apple_uuid, media_metadata = visual_asset_map[source_pk]
                        metadata = {
                            **media_metadata,
                            **row_scalar_metadata(row, ("source_pk", "ZVISUALSEARCHDATA")),
                        }
                        payload_rows.append(
                            build_asset_blob_payload_row(
                                apple_uuid,
                                payload_type_for_column("ZVISUALSEARCHDATA"),
                                "ZVISUALSEARCHATTRIBUTES",
                                source_pk,
                                "ZVISUALSEARCHDATA",
                                bytes(blob_value),
                                metadata,
                                include_raw_blob=True,
                            )
                        )

    if inspector.has_table("ZASSET") and inspector.has_table("ZCLOUDMASTER") and inspector.has_table("ZCLOUDMASTERMEDIAMETADATA"):
        asset_master_col = inspector.choose_column("ZASSET", candidates=("ZMASTER",), contains_all=("master",))
        master_media_col = inspector.choose_column(
            "ZCLOUDMASTER",
            candidates=("ZMEDIAMETADATA",),
            contains_all=("media", "metadata"),
        )
        if asset_master_col and master_media_col and "ZDATA" in inspector.columns("ZCLOUDMASTERMEDIAMETADATA"):
            rows = conn.execute(
                f"""
                SELECT
                  a.Z_PK AS asset_pk,
                  cm.Z_PK AS cloud_master_pk,
                  md.Z_PK AS source_pk,
                  md.ZDATA AS ZDATA
                FROM ZASSET a
                JOIN ZCLOUDMASTER cm ON cm.Z_PK = a.{asset_master_col}
                JOIN ZCLOUDMASTERMEDIAMETADATA md ON md.Z_PK = cm.{master_media_col}
                WHERE md.ZDATA IS NOT NULL
                """
            ).fetchall()
            for row in rows:
                source_pk = to_int(row["source_pk"])
                asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
                blob_value = row["ZDATA"]
                if asset is None or source_pk is None or not isinstance(blob_value, (bytes, bytearray)):
                    continue
                metadata = {
                    "cloudMasterPk": to_int(row["cloud_master_pk"] if "cloud_master_pk" in row.keys() else None),
                }
                payload_rows.append(
                    build_asset_blob_payload_row(
                        asset.apple_uuid,
                        "cloud_master_media_metadata",
                        "ZCLOUDMASTERMEDIAMETADATA",
                        source_pk,
                        "ZDATA",
                        bytes(blob_value),
                        metadata,
                    )
                )

    return payload_rows


def extract_person_blob_payloads(conn: sqlite3.Connection, inspector: SqliteInspector) -> list[dict[str, Any]]:
    if not inspector.has_table("ZPERSON"):
        return []

    columns = inspector.columns("ZPERSON")
    if "ZCONTACTMATCHINGDICTIONARY" not in columns:
        return []

    uuid_col = inspector.choose_column("ZPERSON", candidates=("ZPERSONUUID", "ZUUID"), contains_all=("uuid",))
    name_col = inspector.choose_column(
        "ZPERSON",
        candidates=("ZFULLNAME", "ZDISPLAYNAME", "ZNAME"),
        contains_all=("name",),
        excludes=("cloud", "phonetic"),
    )
    select_parts = ['Z_PK AS "person_pk"', "ZCONTACTMATCHINGDICTIONARY AS ZCONTACTMATCHINGDICTIONARY"]
    if uuid_col:
        select_parts.append(f"{uuid_col} AS person_uuid")
    if name_col:
        select_parts.append(f"{name_col} AS person_name")
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZPERSON").fetchall()

    payload_rows: list[dict[str, Any]] = []
    for row in rows:
        blob_value = row["ZCONTACTMATCHINGDICTIONARY"]
        if not isinstance(blob_value, (bytes, bytearray)):
            continue
        person_pk = to_int(row["person_pk"])
        person_uuid = clean_string(row["person_uuid"] if "person_uuid" in row.keys() else None)
        person_key = person_uuid or (f"pk:{person_pk}" if person_pk is not None else None)
        if not person_key:
            continue
        decoded = decode_bplist_to_jsonable(bytes(blob_value))
        payload_rows.append(
            {
                "row_key": sha1_text(person_key, "contact_matching_dictionary"),
                "person_key": person_key,
                "person_uuid": person_uuid,
                "person_pk": person_pk,
                "display_name": clean_string(row["person_name"] if "person_name" in row.keys() else None),
                "payload_type": "contact_matching_dictionary",
                "source_table": "ZPERSON",
                "source_pk": person_pk,
                "blob_column": "ZCONTACTMATCHINGDICTIONARY",
                "byte_length": len(blob_value),
                "blob_sha1": sha1_bytes(bytes(blob_value)),
                "blob_format": blob_format(bytes(blob_value)),
                "decoded_json": json_dumps(decoded) if decoded is not None else None,
                "metadata_json": json_dumps({}),
            }
        )
    return payload_rows


def extract_faceprints(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> list[dict[str, Any]]:
    if not (inspector.has_table("ZDETECTEDFACE") and inspector.has_table("ZDETECTEDFACEPRINT")):
        return []

    face_asset_col = inspector.choose_column(
        "ZDETECTEDFACE",
        candidates=("ZASSETFORFACE", "ZASSET"),
        contains_all=("asset",),
    )
    face_person_col = inspector.choose_column(
        "ZDETECTEDFACE",
        candidates=("ZPERSONFORFACE", "ZPERSON"),
        contains_all=("person",),
    )
    faceprint_fk_col = inspector.choose_column(
        "ZDETECTEDFACE",
        candidates=("ZFACEPRINT",),
        contains_all=("faceprint",),
    )
    if not face_asset_col or not faceprint_fk_col:
        return []

    person_uuid_col = inspector.choose_column("ZPERSON", candidates=("ZPERSONUUID", "ZUUID"), contains_all=("uuid",))
    person_name_col = inspector.choose_column(
        "ZPERSON",
        candidates=("ZFULLNAME", "ZDISPLAYNAME", "ZNAME"),
        contains_all=("name",),
        excludes=("cloud", "phonetic"),
    )
    face_columns = inspector.columns("ZDETECTEDFACE")
    selected_face_columns: list[str] = []
    for candidate in [
        "ZUUID",
        "ZFACEALGORITHMVERSION",
        "ZQUALITY",
        "ZBLURSCORE",
        "ZHASSMILE",
        "ZISLEFTEYECLOSED",
        "ZISRIGHTEYECLOSED",
        "ZFACEEXPRESSIONTYPE",
        "ZGLASSESTYPE",
        "ZGENDERTYPE",
        "ZAGETYPE",
        "ZETHNICITYTYPE",
        "ZROLL",
        "ZPOSEYAW",
        "ZCENTERX",
        "ZCENTERY",
        "ZSIZE",
        "ZSOURCEWIDTH",
        "ZSOURCEHEIGHT",
        "ZSTARTTIME",
        "ZDURATION",
        "ZVUOBSERVATIONID",
    ]:
        if candidate in face_columns and candidate not in selected_face_columns:
            selected_face_columns.append(candidate)
    for candidate in choose_face_meta_columns(face_columns, face_asset_col, face_person_col):
        if candidate not in selected_face_columns:
            selected_face_columns.append(candidate)

    select_parts = [
        'f.Z_PK AS "face_pk"',
        f"f.{face_asset_col} AS asset_pk",
        f"f.{faceprint_fk_col} AS faceprint_pk",
        'fp.Z_PK AS "source_pk"',
        "fp.ZFACEPRINTVERSION AS faceprint_version",
        "fp.ZDATA AS faceprint_blob",
    ]
    if face_person_col:
        select_parts.append(f"f.{face_person_col} AS person_pk")
    if person_uuid_col:
        select_parts.append(f"p.{person_uuid_col} AS person_uuid")
    if person_name_col:
        select_parts.append(f"p.{person_name_col} AS person_name")
    for column in selected_face_columns:
        select_parts.append(f"f.{column} AS {column}")

    join_person = f"LEFT JOIN ZPERSON p ON p.Z_PK = f.{face_person_col}" if face_person_col else ""
    rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM ZDETECTEDFACE f
        JOIN ZDETECTEDFACEPRINT fp ON fp.Z_PK = f.{faceprint_fk_col}
        {join_person}
        WHERE fp.ZDATA IS NOT NULL
        """
    ).fetchall()

    faceprint_rows: list[dict[str, Any]] = []
    for row in rows:
        asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
        blob_value = row["faceprint_blob"]
        if asset is None or not isinstance(blob_value, (bytes, bytearray)):
            continue
        person_pk = to_int(row["person_pk"] if "person_pk" in row.keys() else None)
        person_uuid = clean_string(row["person_uuid"] if "person_uuid" in row.keys() else None)
        person_key = person_uuid or (f"pk:{person_pk}" if person_pk is not None else None)
        source_pk = to_int(row["source_pk"])
        face_pk = to_int(row["face_pk"])
        metadata = row_scalar_metadata(
            row,
            ("asset_pk", "face_pk", "faceprint_pk", "source_pk", "faceprint_blob"),
        )
        faceprint_rows.append(
            {
                "row_key": sha1_text(asset.apple_uuid, face_pk, source_pk),
                "apple_uuid": asset.apple_uuid,
                "face_pk": face_pk,
                "faceprint_pk": to_int(row["faceprint_pk"] if "faceprint_pk" in row.keys() else None),
                "person_key": person_key,
                "person_uuid": person_uuid,
                "person_pk": person_pk,
                "display_name": clean_string(row["person_name"] if "person_name" in row.keys() else None),
                "detected_face_uuid": clean_string(row["ZUUID"] if "ZUUID" in row.keys() else None),
                "faceprint_version": to_int(row["faceprint_version"] if "faceprint_version" in row.keys() else None),
                "byte_length": len(blob_value),
                "blob_sha1": sha1_bytes(bytes(blob_value)),
                "blob_format": blob_format(bytes(blob_value)),
                "faceprint_blob": bytes(blob_value),
                "metadata_json": json_dumps(metadata),
            }
        )
    return faceprint_rows


def extract_vu_observations(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    asset_uuid_set: set[str],
) -> list[dict[str, Any]]:
    if not inspector.has_table("ZVUINDEXOBSERVATION"):
        return []

    columns = inspector.columns("ZVUINDEXOBSERVATION")
    select_parts = ['Z_PK AS "observation_pk"'] + [
        f"{column} AS {column}"
        for column in [
            "ZCLIENT",
            "ZIDENTIFIER",
            "ZISPRIMARY",
            "ZLEGACYLABEL",
            "ZLEGACYPARTITION",
            "ZSOURCE",
            "ZTYPE",
            "ZMAPPING",
            "ZCONFIDENCE",
            "ZQUALITY",
            "ZASSET",
            "ZMOMENT",
            "ZEMBEDDING",
            "ZCONTEXTUALEMBEDDING",
            "ZASSETSUFFIX",
        ]
        if column in columns
    ]
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZVUINDEXOBSERVATION").fetchall()

    observation_rows: list[dict[str, Any]] = []
    for row in rows:
        apple_uuid = uuid_from_blob(row["ZASSET"] if "ZASSET" in row.keys() else None)
        if not apple_uuid or apple_uuid not in asset_uuid_set:
            continue
        moment_uuid = uuid_from_blob(row["ZMOMENT"] if "ZMOMENT" in row.keys() else None)
        embedding_blob = row["ZEMBEDDING"] if "ZEMBEDDING" in row.keys() else None
        contextual_blob = row["ZCONTEXTUALEMBEDDING"] if "ZCONTEXTUALEMBEDDING" in row.keys() else None
        embedding_meta = float32le_vector_metadata(bytes(embedding_blob)) if isinstance(embedding_blob, (bytes, bytearray)) else None
        contextual_meta = (
            float32le_vector_metadata(bytes(contextual_blob))
            if isinstance(contextual_blob, (bytes, bytearray))
            else None
        )
        metadata = row_scalar_metadata(row, ("ZASSET", "ZMOMENT", "ZEMBEDDING", "ZCONTEXTUALEMBEDDING"))
        observation_pk = to_int(row["observation_pk"])
        observation_rows.append(
            {
                "row_key": sha1_text(apple_uuid, observation_pk, moment_uuid),
                "apple_uuid": apple_uuid,
                "moment_uuid": moment_uuid,
                "observation_pk": observation_pk,
                "identifier": to_int(row["ZIDENTIFIER"] if "ZIDENTIFIER" in row.keys() else None),
                "observation_type": to_int(row["ZTYPE"] if "ZTYPE" in row.keys() else None),
                "source": to_int(row["ZSOURCE"] if "ZSOURCE" in row.keys() else None),
                "client": to_int(row["ZCLIENT"] if "ZCLIENT" in row.keys() else None),
                "mapping": to_int(row["ZMAPPING"] if "ZMAPPING" in row.keys() else None),
                "is_primary": to_int(row["ZISPRIMARY"] if "ZISPRIMARY" in row.keys() else None),
                "confidence": to_float(row["ZCONFIDENCE"] if "ZCONFIDENCE" in row.keys() else None),
                "quality": to_float(row["ZQUALITY"] if "ZQUALITY" in row.keys() else None),
                "asset_suffix": clean_string(row["ZASSETSUFFIX"] if "ZASSETSUFFIX" in row.keys() else None),
                "embedding_format": embedding_meta["format"] if embedding_meta else None,
                "embedding_dimensions": embedding_meta["dimensions"] if embedding_meta else None,
                "embedding_l2_norm": embedding_meta["l2_norm"] if embedding_meta else None,
                "embedding_blob_sha1": sha1_bytes(bytes(embedding_blob))
                if isinstance(embedding_blob, (bytes, bytearray))
                else None,
                "embedding_blob": bytes(embedding_blob) if isinstance(embedding_blob, (bytes, bytearray)) else None,
                "contextual_embedding_format": contextual_meta["format"] if contextual_meta else None,
                "contextual_embedding_dimensions": contextual_meta["dimensions"] if contextual_meta else None,
                "contextual_embedding_l2_norm": contextual_meta["l2_norm"] if contextual_meta else None,
                "contextual_embedding_blob_sha1": sha1_bytes(bytes(contextual_blob))
                if isinstance(contextual_blob, (bytes, bytearray))
                else None,
                "contextual_embedding_blob": bytes(contextual_blob)
                if isinstance(contextual_blob, (bytes, bytearray))
                else None,
                "metadata_json": json_dumps(metadata),
            }
        )
    return observation_rows


def find_memory_junction_tables(inspector: SqliteInspector) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    for table in inspector.table_names_matching(("memories", "assets")):
        lowered = table.lower()
        relation_type = "curated"
        if "extended" in lowered:
            relation_type = "extended_curated"
        elif "representative" in lowered:
            relation_type = "representative"
        results.append((table, relation_type))
    return results


def extract_memories(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not inspector.has_table("ZMEMORY"):
        return [], []

    columns = inspector.columns("ZMEMORY")
    title_col = inspector.choose_column("ZMEMORY", candidates=("ZTITLE",), contains_all=("title",))
    subtitle_col = inspector.choose_column("ZMEMORY", candidates=("ZSUBTITLE",), contains_all=("subtitle",))
    category_col = inspector.choose_column("ZMEMORY", candidates=("ZCATEGORY",), contains_all=("category",))
    start_col = inspector.choose_column("ZMEMORY", candidates=("ZSTARTDATE",), contains_all=("start", "date"))
    end_col = inspector.choose_column("ZMEMORY", candidates=("ZENDDATE",), contains_all=("end", "date"))
    score_col = inspector.choose_column("ZMEMORY", candidates=("ZSCORE",), contains_all=("score",))
    uuid_col = inspector.choose_column("ZMEMORY", candidates=("ZUUID",), contains_all=("uuid",))

    select_parts = ['Z_PK AS "memory_pk"']
    for alias, col in [
        ("title", title_col),
        ("subtitle", subtitle_col),
        ("category", category_col),
        ("start_value", start_col),
        ("end_value", end_col),
        ("score", score_col),
        ("memory_uuid", uuid_col),
    ]:
        if col:
            select_parts.append(f"{col} AS {alias}")
    memory_rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZMEMORY").fetchall()

    memories: list[dict[str, Any]] = []
    for row in memory_rows:
        raw = {column: row[column] for column in row.keys()}
        memories.append(
            {
                "memory_pk": to_int(row["memory_pk"]),
                "memory_uuid": clean_string(row["memory_uuid"] if "memory_uuid" in row.keys() else None),
                "title": clean_string(row["title"] if "title" in row.keys() else None),
                "subtitle": clean_string(row["subtitle"] if "subtitle" in row.keys() else None),
                "category": to_int(row["category"] if "category" in row.keys() else None),
                "start_at": parse_cocoa_date(row["start_value"] if "start_value" in row.keys() else None),
                "end_at": parse_cocoa_date(row["end_value"] if "end_value" in row.keys() else None),
                "score": to_float(row["score"] if "score" in row.keys() else None),
                "metadata_json": json_dumps(raw),
            }
        )

    memory_map = {row["memory_pk"]: row for row in memories if row["memory_pk"] is not None}
    memory_asset_rows: list[dict[str, Any]] = []
    for table, relation_type in find_memory_junction_tables(inspector):
        memory_col = inspector.choose_column(table, contains_all=("memor",))
        asset_col = inspector.choose_column(table, contains_all=("asset",))
        if not memory_col or not asset_col:
            continue
        rows = conn.execute(f"SELECT {memory_col} AS memory_pk, {asset_col} AS asset_pk FROM {table}").fetchall()
        for row in rows:
            memory_pk = to_int(row["memory_pk"])
            asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
            if memory_pk is None or asset is None:
                continue
            memory_asset_rows.append(
                {
                    "memory_pk": memory_pk,
                    "apple_uuid": asset.apple_uuid,
                    "relation_type": relation_type,
                }
            )
            memory = memory_map.get(memory_pk)
            if memory:
                asset.memory_refs.append(
                    {
                        "memoryPk": memory_pk,
                        "title": memory["title"],
                        "relationType": relation_type,
                    }
                )

    return memories, dedupe_dict_rows(memory_asset_rows, ("memory_pk", "apple_uuid", "relation_type"))


def dedupe_dict_rows(rows: Iterable[dict[str, Any]], key_fields: Sequence[str]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def extract_moments(conn: sqlite3.Connection, inspector: SqliteInspector) -> list[dict[str, Any]]:
    if not inspector.has_table("ZMOMENT"):
        return []

    title_col = inspector.choose_column("ZMOMENT", candidates=("ZTITLE",), contains_all=("title",))
    subtitle_col = inspector.choose_column("ZMOMENT", candidates=("ZSUBTITLE",), contains_all=("subtitle",))
    start_col = inspector.choose_column("ZMOMENT", candidates=("ZSTARTDATE",), contains_all=("start", "date"))
    end_col = inspector.choose_column("ZMOMENT", candidates=("ZENDDATE",), contains_all=("end", "date"))
    lat_col = inspector.choose_column("ZMOMENT", candidates=("ZAPPROXIMATELATITUDE",), contains_all=("latitude",))
    lng_col = inspector.choose_column("ZMOMENT", candidates=("ZAPPROXIMATELONGITUDE",), contains_all=("longitude",))
    type_col = inspector.choose_column("ZMOMENT", candidates=("ZMOMENTTYPE",), contains_all=("type",))
    uuid_col = inspector.choose_column("ZMOMENT", candidates=("ZUUID",), contains_all=("uuid",))

    select_parts = ['Z_PK AS "moment_pk"']
    for alias, col in [
        ("moment_uuid", uuid_col),
        ("title", title_col),
        ("subtitle", subtitle_col),
        ("start_value", start_col),
        ("end_value", end_col),
        ("lat_value", lat_col),
        ("lng_value", lng_col),
        ("moment_type", type_col),
    ]:
        if col:
            select_parts.append(f"{col} AS {alias}")
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZMOMENT").fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "moment_pk": to_int(row["moment_pk"]),
                "moment_uuid": clean_string(row["moment_uuid"] if "moment_uuid" in row.keys() else None),
                "title": clean_string(row["title"] if "title" in row.keys() else None),
                "subtitle": clean_string(row["subtitle"] if "subtitle" in row.keys() else None),
                "start_at": parse_cocoa_date(row["start_value"] if "start_value" in row.keys() else None),
                "end_at": parse_cocoa_date(row["end_value"] if "end_value" in row.keys() else None),
                "approx_lat": to_float(row["lat_value"] if "lat_value" in row.keys() else None),
                "approx_lng": to_float(row["lng_value"] if "lng_value" in row.keys() else None),
                "moment_type": clean_string(row["moment_type"] if "moment_type" in row.keys() else None)
                or str(row["moment_type"])
                if "moment_type" in row.keys() and row["moment_type"] is not None
                else None,
                "metadata_json": json_dumps({key: row[key] for key in row.keys()}),
            }
        )
    return result


def find_album_junction_table(inspector: SqliteInspector) -> str | None:
    for table in inspector.tables():
        cols = inspector.columns(table)
        if any("album" in col.lower() for col in cols) and any("asset" in col.lower() for col in cols):
            if table != "ZGENERICALBUM":
                return table
    return None


def extract_albums(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not inspector.has_table("ZGENERICALBUM"):
        return [], []

    title_col = inspector.choose_column("ZGENERICALBUM", candidates=("ZTITLE",), contains_all=("title",))
    uuid_col = inspector.choose_column("ZGENERICALBUM", candidates=("ZUUID",), contains_all=("uuid",))
    kind_col = inspector.choose_column("ZGENERICALBUM", candidates=("ZKIND",), contains_all=("kind",))
    subtype_col = inspector.choose_column("ZGENERICALBUM", candidates=("ZSUBTYPE",), contains_all=("subtype",))
    cloud_col = inspector.choose_column("ZGENERICALBUM", candidates=("ZCLOUDGUID",), contains_all=("cloud",))

    select_parts = ['Z_PK AS "album_pk"']
    for alias, col in [
        ("album_uuid", uuid_col),
        ("title", title_col),
        ("kind", kind_col),
        ("subtype", subtype_col),
        ("cloud_guid", cloud_col),
    ]:
        if col:
            select_parts.append(f"{col} AS {alias}")
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZGENERICALBUM").fetchall()

    albums: list[dict[str, Any]] = []
    for row in rows:
        albums.append(
            {
                "album_pk": to_int(row["album_pk"]),
                "album_uuid": clean_string(row["album_uuid"] if "album_uuid" in row.keys() else None),
                "title": clean_string(row["title"] if "title" in row.keys() else None),
                "kind": clean_string(row["kind"] if "kind" in row.keys() else None)
                or (str(row["kind"]) if "kind" in row.keys() and row["kind"] is not None else None),
                "subtype": clean_string(row["subtype"] if "subtype" in row.keys() else None)
                or (str(row["subtype"]) if "subtype" in row.keys() and row["subtype"] is not None else None),
                "cloud_guid": clean_string(row["cloud_guid"] if "cloud_guid" in row.keys() else None),
                "metadata_json": json_dumps({key: row[key] for key in row.keys()}),
            }
        )

    album_map = {row["album_pk"]: row for row in albums if row["album_pk"] is not None}
    album_asset_rows: list[dict[str, Any]] = []
    junction_table = find_album_junction_table(inspector)
    if junction_table:
        album_col = inspector.choose_column(junction_table, contains_all=("album",))
        asset_col = inspector.choose_column(junction_table, contains_all=("asset",))
        if album_col and asset_col:
            rows = conn.execute(f"SELECT {album_col} AS album_pk, {asset_col} AS asset_pk FROM {junction_table}").fetchall()
            for row in rows:
                asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
                album_pk = to_int(row["album_pk"])
                if not asset or album_pk is None:
                    continue
                album_asset_rows.append(
                    {
                        "album_pk": album_pk,
                        "apple_uuid": asset.apple_uuid,
                    }
                )
                album = album_map.get(album_pk)
                if album and album.get("title"):
                    asset.album_names.append(album["title"])

    return albums, dedupe_dict_rows(album_asset_rows, ("album_pk", "apple_uuid"))


def find_keyword_junction_table(inspector: SqliteInspector) -> str | None:
    for table in inspector.tables():
        cols = inspector.columns(table)
        if table == "ZKEYWORD":
            continue
        if any("keyword" in col.lower() for col in cols) and any("asset" in col.lower() for col in cols):
            return table
    return None


def extract_keywords(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not inspector.has_table("ZKEYWORD"):
        return [], []

    name_col = inspector.choose_column(
        "ZKEYWORD",
        candidates=("ZTITLE", "ZKEYWORD", "ZNAME"),
        contains_all=("name",),
    )
    select_parts = ['Z_PK AS "keyword_pk"']
    if name_col:
        select_parts.append(f"{name_col} AS keyword_name")
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZKEYWORD").fetchall()

    keywords: list[dict[str, Any]] = []
    for row in rows:
        keywords.append(
            {
                "keyword_pk": to_int(row["keyword_pk"]),
                "keyword_name": clean_string(row["keyword_name"] if "keyword_name" in row.keys() else None),
                "metadata_json": json_dumps({key: row[key] for key in row.keys()}),
            }
        )
    keyword_map = {row["keyword_pk"]: row for row in keywords if row["keyword_pk"] is not None}

    keyword_asset_rows: list[dict[str, Any]] = []
    junction_table = find_keyword_junction_table(inspector)
    if junction_table:
        keyword_col = inspector.choose_column(junction_table, contains_all=("keyword",))
        asset_col = inspector.choose_column(junction_table, contains_all=("asset",))
        if keyword_col and asset_col:
            rows = conn.execute(
                f"SELECT {keyword_col} AS keyword_pk, {asset_col} AS asset_pk FROM {junction_table}"
            ).fetchall()
            for row in rows:
                asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
                keyword_pk = to_int(row["keyword_pk"])
                if asset is None or keyword_pk is None:
                    continue
                keyword_asset_rows.append(
                    {
                        "keyword_pk": keyword_pk,
                        "apple_uuid": asset.apple_uuid,
                    }
                )
                keyword = keyword_map.get(keyword_pk)
                if keyword and keyword.get("keyword_name"):
                    asset.keyword_names.append(keyword["keyword_name"])
    return keywords, dedupe_dict_rows(keyword_asset_rows, ("keyword_pk", "apple_uuid"))


def candidate_uuid_decoders() -> list[Callable[[int, int], str]]:
    def decode_big(uuid0: int, uuid1: int) -> str:
        raw = (uuid0 & ((1 << 64) - 1)).to_bytes(8, "big", signed=False) + (
            uuid1 & ((1 << 64) - 1)
        ).to_bytes(8, "big", signed=False)
        return str(uuid.UUID(bytes=raw)).upper()

    def decode_little(uuid0: int, uuid1: int) -> str:
        raw = (uuid0 & ((1 << 64) - 1)).to_bytes(8, "little", signed=False) + (
            uuid1 & ((1 << 64) - 1)
        ).to_bytes(8, "little", signed=False)
        return str(uuid.UUID(bytes=raw)).upper()

    def decode_bytes_le(uuid0: int, uuid1: int) -> str:
        raw = (uuid0 & ((1 << 64) - 1)).to_bytes(8, "big", signed=False) + (
            uuid1 & ((1 << 64) - 1)
        ).to_bytes(8, "big", signed=False)
        return str(uuid.UUID(bytes_le=raw)).upper()

    return [decode_big, decode_little, decode_bytes_le]


def detect_psi_uuid_decoder(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    photo_uuid_set: set[str],
) -> tuple[str, str, str, Callable[[int, int], str]]:
    assets_table = "assets"
    uuid0_col = inspector.choose_column(assets_table, candidates=("uuid_0",), contains_all=("uuid", "0"))
    uuid1_col = inspector.choose_column(assets_table, candidates=("uuid_1",), contains_all=("uuid", "1"))
    asset_id_col = inspector.choose_column(assets_table, candidates=("id",), contains_all=("id",))
    asset_id_expr = asset_id_col if asset_id_col else "rowid"
    if not uuid0_col or not uuid1_col or not asset_id_col:
        if not uuid0_col or not uuid1_col:
            raise RuntimeError("psi.sqlite assets table is missing uuid_0/uuid_1 columns.")

    sample_rows = conn.execute(
        f"SELECT {asset_id_expr} AS asset_id, {uuid0_col}, {uuid1_col} FROM {assets_table} LIMIT 500"
    ).fetchall()
    best_decoder = candidate_uuid_decoders()[0]
    best_matches = -1
    for decoder in candidate_uuid_decoders():
        matches = 0
        for row in sample_rows:
            try:
                decoded = decoder(int(row[uuid0_col]), int(row[uuid1_col]))
            except Exception:
                continue
            if decoded in photo_uuid_set:
                matches += 1
        if matches > best_matches:
            best_matches = matches
            best_decoder = decoder
    if best_matches <= 0:
        eprint(
            "[psi] warning: could not confidently decode uuid_0/uuid_1 against Photos UUIDs. "
            "Falling back to big-endian decoder."
        )
    return asset_id_expr, uuid0_col, uuid1_col, best_decoder


def extract_search_index(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_uuid: dict[str, AssetRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not (inspector.has_table("assets") and inspector.has_table("groups") and inspector.has_table("ga")):
        return [], []

    photo_uuid_set = set(assets_by_uuid)
    asset_id_col, uuid0_col, uuid1_col, decoder = detect_psi_uuid_decoder(conn, inspector, photo_uuid_set)
    group_id_col = inspector.choose_column("groups", candidates=("id",), contains_all=("id",))
    group_id_expr = group_id_col if group_id_col else "rowid"
    group_category_col = inspector.choose_column("groups", candidates=("category",), contains_all=("category",))
    group_lookup_col = inspector.choose_column(
        "groups",
        candidates=("lookup_identifier",),
        contains_all=("lookup", "identifier"),
    )
    group_text_col = inspector.choose_column(
        "groups",
        candidates=("content_string", "normalized_string", "title", "display_name", "name", "label", "token", "text", "value"),
        contains_all=("name",),
    ) or inspector.choose_column(
        "groups",
        candidates=("content_string", "normalized_string"),
        contains_all=("token",),
    )
    ga_asset_col = inspector.choose_column("ga", candidates=("asset_id", "assetid"), contains_all=("asset",))
    ga_group_col = inspector.choose_column("ga", candidates=("group_id", "groupid"), contains_all=("group",))

    if not all([group_category_col, ga_asset_col, ga_group_col]):
        return [], []

    psi_asset_to_uuid: dict[int, str] = {}
    for row in conn.execute(
        f"SELECT {asset_id_col} AS asset_id, {uuid0_col} AS uuid0, {uuid1_col} AS uuid1 FROM assets"
    ):
        try:
            decoded = decoder(int(row["uuid0"]), int(row["uuid1"]))
        except Exception:
            continue
        if decoded in photo_uuid_set:
            psi_asset_to_uuid[int(row["asset_id"])] = decoded

    group_select = [
        f"{group_id_expr} AS group_id",
        f"{group_category_col} AS category",
    ]
    if group_lookup_col:
        group_select.append(f"{group_lookup_col} AS lookup_identifier")
    if group_text_col:
        group_select.append(f"{group_text_col} AS label_text")
    group_rows = conn.execute(
        f"""
        SELECT {", ".join(group_select)}
        FROM groups
        WHERE {group_category_col} IN (?, ?)
        """,
        (OCR_CATEGORY, SCENE_SEARCH_CATEGORY),
    ).fetchall()

    search_groups = [
        {
            "group_id": to_int(row["group_id"]),
            "category": to_int(row["category"]),
            "lookup_identifier": clean_string(row["lookup_identifier"] if "lookup_identifier" in row.keys() else None),
            "label_text": clean_string(row["label_text"] if "label_text" in row.keys() else None),
        }
        for row in group_rows
    ]
    groups_by_id = {row["group_id"]: row for row in search_groups if row["group_id"] is not None}

    ocr_tokens_by_uuid: dict[str, list[str]] = collections.defaultdict(list)
    scene_asset_labels: list[dict[str, Any]] = []

    query = f"""
        SELECT
          ga.{ga_asset_col} AS asset_id,
          ga.{ga_group_col} AS group_id,
          g.{group_category_col} AS category
          {f", g.{group_text_col} AS label_text" if group_text_col else ""}
          {f", g.{group_lookup_col} AS lookup_identifier" if group_lookup_col else ""}
        FROM ga
        JOIN groups g ON g.{group_id_expr} = ga.{ga_group_col}
        WHERE g.{group_category_col} IN (?, ?)
        ORDER BY ga.{ga_asset_col}, ga.{ga_group_col}
    """
    rows = conn.execute(query, (OCR_CATEGORY, SCENE_SEARCH_CATEGORY))
    seen_scene_links: set[tuple[str, int]] = set()

    for row in rows:
        apple_uuid = psi_asset_to_uuid.get(to_int(row["asset_id"]) or -1)
        if not apple_uuid:
            continue
        category = to_int(row["category"])
        group_id = to_int(row["group_id"])
        label_text = clean_string(row["label_text"] if "label_text" in row.keys() else None)
        if category == OCR_CATEGORY:
            if label_text:
                ocr_tokens_by_uuid[apple_uuid].append(label_text)
        elif category == SCENE_SEARCH_CATEGORY and group_id is not None:
            key = (apple_uuid, group_id)
            if key in seen_scene_links:
                continue
            seen_scene_links.add(key)
            scene_asset_labels.append(
                {
                    "apple_uuid": apple_uuid,
                    "group_id": group_id,
                    "label_text": label_text,
                    "lookup_identifier": clean_string(
                        row["lookup_identifier"] if "lookup_identifier" in row.keys() else None
                    ),
                }
            )

    for apple_uuid, tokens in ocr_tokens_by_uuid.items():
        seen: set[str] = set()
        deduped: list[str] = []
        for token in tokens:
            normalized = token.strip()
            if not normalized:
                continue
            lowered = normalized.casefold()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(normalized)
        assets_by_uuid[apple_uuid].ocr_text = clean_whitespace(" ".join(deduped))

    search_groups_rows: list[dict[str, Any]] = [
        {
            "group_id": row["group_id"],
            "category": row["category"],
            "lookup_identifier": row["lookup_identifier"],
            "label_text": row["label_text"],
        }
        for row in groups_by_id.values()
    ]
    return search_groups_rows, scene_asset_labels


def extract_scene_classifications(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    assets_by_pk: dict[int, AssetRecord],
) -> Iterator[list[dict[str, Any]]]:
    if not inspector.has_table("ZSCENECLASSIFICATION"):
        return iter(())

    asset_col = inspector.choose_column(
        "ZSCENECLASSIFICATION",
        candidates=("ZASSET", "ZASSETFORSCENECLASSIFICATION"),
        contains_all=("asset",),
    )
    scene_id_col = inspector.choose_column(
        "ZSCENECLASSIFICATION",
        candidates=("ZSCENEIDENTIFIER",),
        contains_all=("scene", "identifier"),
    )
    confidence_col = inspector.choose_column(
        "ZSCENECLASSIFICATION",
        candidates=("ZCONFIDENCE",),
        contains_all=("confidence",),
    )
    bbox_col = inspector.choose_column(
        "ZSCENECLASSIFICATION",
        candidates=("ZPACKEDBOUNDINGBOXRECT",),
        contains_all=("bounding",),
    )
    start_col = inspector.choose_column(
        "ZSCENECLASSIFICATION",
        candidates=("ZSTARTTIME",),
        contains_all=("start", "time"),
    )
    duration_col = inspector.choose_column(
        "ZSCENECLASSIFICATION",
        candidates=("ZDURATION",),
        contains_all=("duration",),
    )
    if not asset_col or not scene_id_col:
        return iter(())

    select_parts = [
        f"{asset_col} AS asset_pk",
        f"{scene_id_col} AS scene_identifier",
    ]
    if confidence_col:
        select_parts.append(f"{confidence_col} AS confidence")
    if bbox_col:
        select_parts.append(f"{bbox_col} AS packed_bounding_box")
    if start_col:
        select_parts.append(f"{start_col} AS start_time_seconds")
    if duration_col:
        select_parts.append(f"{duration_col} AS duration_seconds")

    cursor = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZSCENECLASSIFICATION")

    def iterator() -> Iterator[list[dict[str, Any]]]:
        batch: list[dict[str, Any]] = []
        while True:
            rows = cursor.fetchmany(10_000)
            if not rows:
                if batch:
                    yield batch
                break
            for row in rows:
                asset = assets_by_pk.get(to_int(row["asset_pk"]) or -1)
                if not asset:
                    continue
                scene_identifier = to_int(row["scene_identifier"])
                if scene_identifier is None:
                    continue
                packed_bbox = row["packed_bounding_box"] if "packed_bounding_box" in row.keys() else None
                batch.append(
                    {
                        "row_key": sha1_text(
                            asset.apple_uuid,
                            scene_identifier,
                            packed_bbox,
                            row["start_time_seconds"] if "start_time_seconds" in row.keys() else None,
                            row["duration_seconds"] if "duration_seconds" in row.keys() else None,
                        ),
                        "apple_uuid": asset.apple_uuid,
                        "scene_identifier": scene_identifier,
                        "confidence": to_float(row["confidence"] if "confidence" in row.keys() else None),
                        "packed_bounding_box": clean_string(packed_bbox) if not isinstance(packed_bbox, bytes) else packed_bbox.hex(),
                        "start_time_seconds": to_float(row["start_time_seconds"] if "start_time_seconds" in row.keys() else None),
                        "duration_seconds": to_float(row["duration_seconds"] if "duration_seconds" in row.keys() else None),
                    }
                )
                if len(batch) >= 5_000:
                    yield batch
                    batch = []

    return iterator()


def haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_m = 6_371_000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius_m * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def cell_key(lat: float, lng: float, cell_size_deg: float) -> tuple[int, int]:
    return (math.floor(lat / cell_size_deg), math.floor(lng / cell_size_deg))


def neighbor_keys(lat: float, lng: float, cell_size_deg: float) -> Iterator[tuple[int, int]]:
    base_lat, base_lng = cell_key(lat, lng, cell_size_deg)
    for dlat in (-1, 0, 1):
        for dlng in (-1, 0, 1):
            yield base_lat + dlat, base_lng + dlng


def build_spatial_index(
    items: Sequence[dict[str, Any]], lat_field: str, lng_field: str, threshold_m: float
) -> tuple[float, dict[tuple[int, int], list[dict[str, Any]]]]:
    cell_size_deg = max(threshold_m / 111_320.0, 0.0025)
    grid: dict[tuple[int, int], list[dict[str, Any]]] = collections.defaultdict(list)
    for item in items:
        lat = to_float(item.get(lat_field))
        lng = to_float(item.get(lng_field))
        if lat is None or lng is None:
            continue
        grid[cell_key(lat, lng, cell_size_deg)].append(item)
    return cell_size_deg, grid


def extract_placemarks(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
) -> list[dict[str, Any]]:
    if not inspector.has_table("ZPLACEMARK"):
        return []

    columns = inspector.columns("ZPLACEMARK")
    wanted = [
        "ZLATITUDE",
        "ZLONGITUDE",
        "ZADMINISTRATIVEAREA",
        "ZLOCALITY",
        "ZSUBLOCALITY",
        "ZTHOROUGHFARE",
        "ZISOCOUNTRYCODE",
        "ZAREASOFINTEREST",
    ]
    select_parts = ['Z_PK AS "placemark_pk"'] + [
        f"{col} AS {col}" for col in wanted if col in columns
    ]
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZPLACEMARK").fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "placemark_pk": to_int(row["placemark_pk"]),
                "lat": to_float(row["ZLATITUDE"] if "ZLATITUDE" in row.keys() else None),
                "lng": to_float(row["ZLONGITUDE"] if "ZLONGITUDE" in row.keys() else None),
                "administrative_area": clean_string(row["ZADMINISTRATIVEAREA"] if "ZADMINISTRATIVEAREA" in row.keys() else None),
                "locality": clean_string(row["ZLOCALITY"] if "ZLOCALITY" in row.keys() else None),
                "sub_locality": clean_string(row["ZSUBLOCALITY"] if "ZSUBLOCALITY" in row.keys() else None),
                "thoroughfare": clean_string(row["ZTHOROUGHFARE"] if "ZTHOROUGHFARE" in row.keys() else None),
                "iso_country_code": clean_string(row["ZISOCOUNTRYCODE"] if "ZISOCOUNTRYCODE" in row.keys() else None),
                "areas_of_interest": clean_string(row["ZAREASOFINTEREST"] if "ZAREASOFINTEREST" in row.keys() else None),
            }
        )
    return result


def match_assets_to_placemarks(
    assets: Sequence[AssetRecord],
    placemarks: Sequence[dict[str, Any]],
    threshold_m: float,
) -> list[dict[str, Any]]:
    cell_size_deg, grid = build_spatial_index(placemarks, "lat", "lng", threshold_m)
    matches: list[dict[str, Any]] = []
    for asset in assets:
        if asset.gps_lat is None or asset.gps_lng is None:
            continue
        best: tuple[float, dict[str, Any]] | None = None
        for key in neighbor_keys(asset.gps_lat, asset.gps_lng, cell_size_deg):
            for placemark in grid.get(key, []):
                lat = placemark["lat"]
                lng = placemark["lng"]
                if lat is None or lng is None:
                    continue
                distance = haversine_meters(asset.gps_lat, asset.gps_lng, lat, lng)
                if distance > threshold_m:
                    continue
                if best is None or distance < best[0]:
                    best = (distance, placemark)
        if best is None:
            continue
        matches.append(
            {
                "apple_uuid": asset.apple_uuid,
                "placemark_pk": best[1]["placemark_pk"],
                "distance_m": round(best[0], 3),
            }
        )
    return matches


def extract_public_events(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
) -> list[dict[str, Any]]:
    if not inspector.has_table("ZPUBLICEVENT"):
        return []

    columns = inspector.columns("ZPUBLICEVENT")
    wanted = [
        "ZNAME",
        "ZLOCALSTARTDATE",
        "ZBUSINESSITEMLATITUDE",
        "ZBUSINESSITEMLONGITUDE",
        "ZBUSINESSITEMMUID",
    ]
    select_parts = ['Z_PK AS "public_event_pk"'] + [
        f"{col} AS {col}" for col in wanted if col in columns
    ]
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZPUBLICEVENT").fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "public_event_pk": to_int(row["public_event_pk"]),
                "name": clean_string(row["ZNAME"] if "ZNAME" in row.keys() else None),
                "local_start_at": parse_cocoa_date(row["ZLOCALSTARTDATE"] if "ZLOCALSTARTDATE" in row.keys() else None),
                "lat": to_float(row["ZBUSINESSITEMLATITUDE"] if "ZBUSINESSITEMLATITUDE" in row.keys() else None),
                "lng": to_float(row["ZBUSINESSITEMLONGITUDE"] if "ZBUSINESSITEMLONGITUDE" in row.keys() else None),
                "business_item_muid": clean_string(row["ZBUSINESSITEMMUID"] if "ZBUSINESSITEMMUID" in row.keys() else None),
                "metadata_json": json_dumps({key: row[key] for key in row.keys()}),
            }
        )
    return result


def match_assets_to_public_events(
    assets: Sequence[AssetRecord],
    public_events: Sequence[dict[str, Any]],
    distance_threshold_m: float,
    time_window_hours: float,
) -> list[dict[str, Any]]:
    cell_size_deg, grid = build_spatial_index(public_events, "lat", "lng", distance_threshold_m)
    time_window_seconds = time_window_hours * 3600.0
    matches: list[dict[str, Any]] = []
    for asset in assets:
        if asset.gps_lat is None or asset.gps_lng is None or asset.taken_at is None:
            continue
        for key in neighbor_keys(asset.gps_lat, asset.gps_lng, cell_size_deg):
            for event in grid.get(key, []):
                lat = event["lat"]
                lng = event["lng"]
                event_start = event["local_start_at"]
                if lat is None or lng is None or event_start is None:
                    continue
                distance = haversine_meters(asset.gps_lat, asset.gps_lng, lat, lng)
                if distance > distance_threshold_m:
                    continue
                time_delta_seconds = abs((asset.taken_at - event_start).total_seconds())
                if time_delta_seconds > time_window_seconds:
                    continue
                matches.append(
                    {
                        "row_key": sha1_text(asset.apple_uuid, event["public_event_pk"]),
                        "apple_uuid": asset.apple_uuid,
                        "public_event_pk": event["public_event_pk"],
                        "distance_m": round(distance, 3),
                        "time_delta_seconds": round(time_delta_seconds, 3),
                    }
                )
    return dedupe_dict_rows(matches, ("row_key",))


def extract_business_items(
    conn: sqlite3.Connection,
    inspector: SqliteInspector,
    cache_kind: str,
) -> list[dict[str, Any]]:
    if not inspector.has_table("ZBUSINESSITEM"):
        return []

    columns = inspector.columns("ZBUSINESSITEM")
    wanted = [
        "ZNAME",
        "ZBUSINESSCATEGORIES",
        "ZLATITUDE",
        "ZLONGITUDE",
        "ZISOCOUNTRYCODE",
    ]
    select_parts = ['Z_PK AS "item_pk"'] + [f"{col} AS {col}" for col in wanted if col in columns]
    rows = conn.execute(f"SELECT {', '.join(select_parts)} FROM ZBUSINESSITEM").fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "cache_kind": cache_kind,
                "item_pk": to_int(row["item_pk"]),
                "name": clean_string(row["ZNAME"] if "ZNAME" in row.keys() else None),
                "business_categories": clean_string(
                    row["ZBUSINESSCATEGORIES"] if "ZBUSINESSCATEGORIES" in row.keys() else None
                ),
                "lat": to_float(row["ZLATITUDE"] if "ZLATITUDE" in row.keys() else None),
                "lng": to_float(row["ZLONGITUDE"] if "ZLONGITUDE" in row.keys() else None),
                "iso_country_code": clean_string(
                    row["ZISOCOUNTRYCODE"] if "ZISOCOUNTRYCODE" in row.keys() else None
                ),
            }
        )
    return result


def match_assets_to_business_items(
    assets: Sequence[AssetRecord],
    items: Sequence[dict[str, Any]],
    distance_threshold_m: float,
    max_matches_per_asset: int,
) -> list[dict[str, Any]]:
    cell_size_deg, grid = build_spatial_index(items, "lat", "lng", distance_threshold_m)
    matches: list[dict[str, Any]] = []
    for asset in assets:
        if asset.gps_lat is None or asset.gps_lng is None:
            continue
        candidates: list[tuple[float, dict[str, Any]]] = []
        for key in neighbor_keys(asset.gps_lat, asset.gps_lng, cell_size_deg):
            for item in grid.get(key, []):
                lat = item["lat"]
                lng = item["lng"]
                if lat is None or lng is None:
                    continue
                distance = haversine_meters(asset.gps_lat, asset.gps_lng, lat, lng)
                if distance > distance_threshold_m:
                    continue
                candidates.append((distance, item))
        candidates.sort(key=lambda pair: pair[0])
        for distance, item in candidates[:max_matches_per_asset]:
            matches.append(
                {
                    "row_key": sha1_text(asset.apple_uuid, item["cache_kind"], item["item_pk"]),
                    "apple_uuid": asset.apple_uuid,
                    "cache_kind": item["cache_kind"],
                    "item_pk": item["item_pk"],
                    "distance_m": round(distance, 3),
                }
            )
    return dedupe_dict_rows(matches, ("row_key",))


def lazy_import_psycopg():
    try:
        import psycopg  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is not installed. Create a venv and run `pip install -r requirements.txt`."
        ) from exc
    return psycopg


class PostgresSink:
    def __init__(self, dsn: str, force_canonical_ocr: bool):
        self.psycopg = lazy_import_psycopg()
        self.conn = self.psycopg.connect(dsn)
        self.force_canonical_ocr = force_canonical_ocr

    def close(self) -> None:
        self.conn.close()

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
        self.conn.commit()

    def ensure_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS apple_photos_assets (
          apple_uuid TEXT PRIMARY KEY,
          local_identifier TEXT NOT NULL UNIQUE,
          canonical_media_asset_id UUID,
          filename TEXT,
          original_filename TEXT,
          title TEXT,
          caption TEXT,
          ocr_text TEXT,
          taken_at TIMESTAMPTZ,
          timezone_offset_min INTEGER,
          gps_lat DOUBLE PRECISION,
          gps_lng DOUBLE PRECISION,
          media_kind TEXT NOT NULL,
          favorite_flag BOOLEAN NOT NULL DEFAULT FALSE,
          managed_filename TEXT,
          directory_shard TEXT,
          uniform_type_identifier TEXT,
          original_file_size_bytes BIGINT,
          original_width INTEGER,
          original_height INTEGER,
          display_width INTEGER,
          display_height INTEGER,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_aesthetics (
          apple_uuid TEXT PRIMARY KEY REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          failure_score DOUBLE PRECISION,
          harmonious_color_score DOUBLE PRECISION,
          immersiveness_score DOUBLE PRECISION,
          interesting_subject_score DOUBLE PRECISION,
          intrusive_object_presence_score DOUBLE PRECISION,
          lively_color_score DOUBLE PRECISION,
          low_light_score DOUBLE PRECISION,
          noise_score DOUBLE PRECISION,
          pleasant_camera_tilt_score DOUBLE PRECISION,
          pleasant_composition_score DOUBLE PRECISION,
          pleasant_lighting_score DOUBLE PRECISION,
          pleasant_pattern_score DOUBLE PRECISION,
          pleasant_perspective_score DOUBLE PRECISION,
          pleasant_post_processing_score DOUBLE PRECISION,
          pleasant_reflection_score DOUBLE PRECISION,
          pleasant_symmetry_score DOUBLE PRECISION,
          sharply_focused_subject_score DOUBLE PRECISION,
          tastefully_blurred_score DOUBLE PRECISION,
          well_chosen_subject_score DOUBLE PRECISION,
          well_framed_subject_score DOUBLE PRECISION,
          well_timed_shot_score DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS apple_photos_search_groups (
          group_id BIGINT PRIMARY KEY,
          category INTEGER NOT NULL,
          lookup_identifier TEXT,
          label_text TEXT
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_search_labels (
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          group_id BIGINT NOT NULL REFERENCES apple_photos_search_groups(group_id) ON DELETE CASCADE,
          label_text TEXT,
          lookup_identifier TEXT,
          PRIMARY KEY (apple_uuid, group_id)
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_scene_labels (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          scene_identifier BIGINT NOT NULL,
          confidence DOUBLE PRECISION,
          packed_bounding_box TEXT,
          start_time_seconds DOUBLE PRECISION,
          duration_seconds DOUBLE PRECISION
        );

        CREATE TABLE IF NOT EXISTS apple_photos_memories (
          memory_pk BIGINT PRIMARY KEY,
          memory_uuid TEXT,
          title TEXT,
          subtitle TEXT,
          category INTEGER,
          start_at TIMESTAMPTZ,
          end_at TIMESTAMPTZ,
          score DOUBLE PRECISION,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS apple_photos_memory_assets (
          memory_pk BIGINT NOT NULL REFERENCES apple_photos_memories(memory_pk) ON DELETE CASCADE,
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          relation_type TEXT NOT NULL,
          PRIMARY KEY (memory_pk, apple_uuid, relation_type)
        );

        CREATE TABLE IF NOT EXISTS apple_photos_moments (
          moment_pk BIGINT PRIMARY KEY,
          moment_uuid TEXT,
          title TEXT,
          subtitle TEXT,
          start_at TIMESTAMPTZ,
          end_at TIMESTAMPTZ,
          approx_lat DOUBLE PRECISION,
          approx_lng DOUBLE PRECISION,
          moment_type TEXT,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS apple_photos_albums (
          album_pk BIGINT PRIMARY KEY,
          album_uuid TEXT,
          title TEXT,
          kind TEXT,
          subtype TEXT,
          cloud_guid TEXT,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS apple_photos_album_assets (
          album_pk BIGINT NOT NULL REFERENCES apple_photos_albums(album_pk) ON DELETE CASCADE,
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          PRIMARY KEY (album_pk, apple_uuid)
        );

        CREATE TABLE IF NOT EXISTS apple_photos_keywords (
          keyword_pk BIGINT PRIMARY KEY,
          keyword_name TEXT,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_keywords (
          keyword_pk BIGINT NOT NULL REFERENCES apple_photos_keywords(keyword_pk) ON DELETE CASCADE,
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          PRIMARY KEY (keyword_pk, apple_uuid)
        );

        CREATE TABLE IF NOT EXISTS apple_photos_people (
          person_key TEXT PRIMARY KEY,
          person_uuid TEXT,
          person_pk BIGINT,
          full_name TEXT,
          display_name TEXT,
          face_count INTEGER NOT NULL DEFAULT 0,
          is_named BOOLEAN NOT NULL DEFAULT FALSE,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_people (
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          person_key TEXT NOT NULL REFERENCES apple_photos_people(person_key) ON DELETE CASCADE,
          person_uuid TEXT,
          person_pk BIGINT,
          display_name TEXT,
          face_count INTEGER NOT NULL DEFAULT 0,
          face_rows_json JSONB NOT NULL DEFAULT '[]'::jsonb,
          PRIMARY KEY (apple_uuid, person_key)
        );

        CREATE TABLE IF NOT EXISTS apple_photos_placemarks (
          placemark_pk BIGINT PRIMARY KEY,
          lat DOUBLE PRECISION,
          lng DOUBLE PRECISION,
          administrative_area TEXT,
          locality TEXT,
          sub_locality TEXT,
          thoroughfare TEXT,
          iso_country_code TEXT,
          areas_of_interest TEXT
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_placemarks (
          apple_uuid TEXT PRIMARY KEY REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          placemark_pk BIGINT NOT NULL REFERENCES apple_photos_placemarks(placemark_pk) ON DELETE CASCADE,
          distance_m DOUBLE PRECISION NOT NULL
        );

        CREATE TABLE IF NOT EXISTS apple_photos_public_events (
          public_event_pk BIGINT PRIMARY KEY,
          name TEXT,
          local_start_at TIMESTAMPTZ,
          lat DOUBLE PRECISION,
          lng DOUBLE PRECISION,
          business_item_muid TEXT,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_public_events (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          public_event_pk BIGINT NOT NULL REFERENCES apple_photos_public_events(public_event_pk) ON DELETE CASCADE,
          distance_m DOUBLE PRECISION NOT NULL,
          time_delta_seconds DOUBLE PRECISION NOT NULL
        );

        CREATE TABLE IF NOT EXISTS apple_photos_business_items (
          cache_kind TEXT NOT NULL,
          item_pk BIGINT NOT NULL,
          name TEXT,
          business_categories TEXT,
          lat DOUBLE PRECISION,
          lng DOUBLE PRECISION,
          iso_country_code TEXT,
          PRIMARY KEY (cache_kind, item_pk)
        );

        CREATE TABLE IF NOT EXISTS apple_photos_asset_business_items (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL REFERENCES apple_photos_assets(apple_uuid) ON DELETE CASCADE,
          cache_kind TEXT NOT NULL,
          item_pk BIGINT NOT NULL,
          distance_m DOUBLE PRECISION NOT NULL,
          FOREIGN KEY (cache_kind, item_pk) REFERENCES apple_photos_business_items(cache_kind, item_pk) ON DELETE CASCADE
        );
        """
        self.execute(ddl)

    def truncate_stage_tables(self) -> None:
        sql = """
        TRUNCATE TABLE
          apple_photos_asset_business_items,
          apple_photos_business_items,
          apple_photos_asset_public_events,
          apple_photos_public_events,
          apple_photos_asset_placemarks,
          apple_photos_placemarks,
          apple_photos_asset_people,
          apple_photos_people,
          apple_photos_asset_keywords,
          apple_photos_keywords,
          apple_photos_album_assets,
          apple_photos_albums,
          apple_photos_memory_assets,
          apple_photos_memories,
          apple_photos_moments,
          apple_photos_asset_scene_labels,
          apple_photos_asset_search_labels,
          apple_photos_search_groups,
          apple_photos_asset_aesthetics,
          apple_photos_assets
        """
        self.execute(sql)

    def fetch_origin_map(self) -> dict[str, str]:
        sql = """
        SELECT source_item_id, media_asset_id::text
        FROM media_asset_origins
        WHERE source = 'APPLE_PHOTOS'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return {str(source_item_id): str(media_asset_id) for source_item_id, media_asset_id in rows}

    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[dict[str, Any]], page_size: int = 1000) -> None:
        if not rows:
            return
        quoted_columns = ", ".join(csv_quote_identifier(col) for col in columns)
        placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
        with self.conn.cursor() as cur:
            for chunk in chunked(list(rows), page_size):
                values_sql = ", ".join([placeholders] * len(chunk))
                params: list[Any] = []
                for row in chunk:
                    for column in columns:
                        params.append(row.get(column))
                sql = f"INSERT INTO {table} ({quoted_columns}) VALUES {values_sql}"
                cur.execute(sql, params)
        self.conn.commit()

    def sync_canonical_media(self) -> None:
        update_sql = """
        UPDATE media_assets AS ma
        SET
          ocr_text = CASE
            WHEN %s THEN COALESCE(apa.ocr_text, ma.ocr_text)
            WHEN ma.ocr_text IS NULL OR btrim(ma.ocr_text) = '' THEN COALESCE(apa.ocr_text, ma.ocr_text)
            ELSE ma.ocr_text
          END,
          caption = CASE
            WHEN ma.caption IS NULL OR btrim(ma.caption) = '' THEN COALESCE(apa.caption, ma.caption)
            ELSE ma.caption
          END,
          metadata_json = COALESCE(ma.metadata_json, '{}'::jsonb)
            || jsonb_build_object('apple_photos', COALESCE(apa.metadata_json, '{}'::jsonb)),
          updated_at = now()
        FROM apple_photos_assets AS apa
        WHERE ma.id = apa.canonical_media_asset_id
        """

        origin_sql = """
        UPDATE media_asset_origins AS mao
        SET
          source_metadata_json = COALESCE(mao.source_metadata_json, '{}'::jsonb)
            || jsonb_build_object(
              'apple_photos_stage1',
              jsonb_build_object(
                'appleUuid', apa.apple_uuid,
                'localIdentifier', apa.local_identifier,
                'title', apa.title,
                'caption', apa.caption,
                'ocrText', apa.ocr_text,
                'takenAt', apa.taken_at,
                'gpsLat', apa.gps_lat,
                'gpsLng', apa.gps_lng,
                'metadata', COALESCE(apa.metadata_json, '{}'::jsonb)
              )
            ),
          import_last_seen_at = now(),
          updated_at = now()
        FROM apple_photos_assets AS apa
        WHERE mao.source = 'APPLE_PHOTOS'
          AND mao.media_asset_id = apa.canonical_media_asset_id
          AND (mao.source_item_id = apa.apple_uuid OR mao.source_item_id = apa.local_identifier)
        """

        with self.conn.cursor() as cur:
            cur.execute(update_sql, (self.force_canonical_ocr,))
            cur.execute(origin_sql)
        self.conn.commit()


def aesthetic_row_for_asset(asset: AssetRecord) -> dict[str, Any]:
    mapping = {
        "apple_uuid": asset.apple_uuid,
        "failure_score": asset.aesthetic_scores.get("ZFAILURESCORE"),
        "harmonious_color_score": asset.aesthetic_scores.get("ZHARMONIOUSCOLORSCORE"),
        "immersiveness_score": asset.aesthetic_scores.get("ZIMMERSIVENESSSCORE"),
        "interesting_subject_score": asset.aesthetic_scores.get("ZINTERESTINGSUBJECTSCORE"),
        "intrusive_object_presence_score": asset.aesthetic_scores.get("ZINTRUSIVEOBJECTPRESENCESCORE"),
        "lively_color_score": asset.aesthetic_scores.get("ZLIVELYCOLORSCORE"),
        "low_light_score": asset.aesthetic_scores.get("ZLOWLIGHT"),
        "noise_score": asset.aesthetic_scores.get("ZNOISESCORE"),
        "pleasant_camera_tilt_score": asset.aesthetic_scores.get("ZPLEASANTCAMERATILTSCORE"),
        "pleasant_composition_score": asset.aesthetic_scores.get("ZPLEASANTCOMPOSITIONSCORE"),
        "pleasant_lighting_score": asset.aesthetic_scores.get("ZPLEASANTLIGHTINGSCORE"),
        "pleasant_pattern_score": asset.aesthetic_scores.get("ZPLEASANTPATTERNSCORE"),
        "pleasant_perspective_score": asset.aesthetic_scores.get("ZPLEASANTPERSPECTIVESCORE"),
        "pleasant_post_processing_score": asset.aesthetic_scores.get("ZPLEASANTPOSTPROCESSINGSCORE"),
        "pleasant_reflection_score": asset.aesthetic_scores.get("ZPLEASANTREFLECTIONSSCORE"),
        "pleasant_symmetry_score": asset.aesthetic_scores.get("ZPLEASANTSYMMETRYSCORE"),
        "sharply_focused_subject_score": asset.aesthetic_scores.get("ZSHARPLYFOCUSEDSUBJECTSCORE"),
        "tastefully_blurred_score": asset.aesthetic_scores.get("ZTASTEFULLYBLURREDSCORE"),
        "well_chosen_subject_score": asset.aesthetic_scores.get("ZWELLCHOSENSUBJECTSCORE"),
        "well_framed_subject_score": asset.aesthetic_scores.get("ZWELLFRAMEDSUBJECTSCORE"),
        "well_timed_shot_score": asset.aesthetic_scores.get("ZWELLTIMEDSHOTSCORE"),
    }
    return mapping


def maybe_open_db(library_path: Path, relative_path: str) -> sqlite3.Connection | None:
    absolute = library_path / relative_path
    if not absolute.exists():
        eprint(f"[skip] database not found: {absolute}")
        return None
    return open_sqlite_readonly(absolute)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract Apple Photos stage-1 metadata and sync it into Postgres."
    )
    parser.add_argument("--library-path", help="Path to the .photoslibrary bundle.")
    parser.add_argument(
        "--postgres-dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to DATABASE_URL.",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--placemark-radius-m", type=float, default=300.0)
    parser.add_argument("--business-radius-m", type=float, default=200.0)
    parser.add_argument("--public-event-radius-m", type=float, default=500.0)
    parser.add_argument("--public-event-time-window-hours", type=float, default=24.0)
    parser.add_argument("--max-business-matches", type=int, default=5)
    parser.add_argument("--force-canonical-ocr", action="store_true")
    parser.add_argument("--skip-business-items", action="store_true")
    parser.add_argument("--skip-public-events", action="store_true")
    parser.add_argument("--skip-scene-classifications", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    if args.dry_run and args.postgres_dsn:
        eprint("[info] --dry-run set: Postgres writes will be skipped.")

    library_path = discover_photos_library(args.library_path)
    eprint(f"[start] library={library_path}")

    photos_conn = open_sqlite_readonly(library_path / "database/Photos.sqlite")
    photos_inspector = SqliteInspector(photos_conn)

    sink: PostgresSink | None = None
    origin_map: dict[str, str] = {}
    if not args.dry_run:
        if not args.postgres_dsn:
            parser.error("--postgres-dsn or DATABASE_URL is required unless --dry-run is used.")
        sink = PostgresSink(args.postgres_dsn, force_canonical_ocr=args.force_canonical_ocr)
        sink.ensure_schema()
        origin_map = sink.fetch_origin_map()
        eprint(f"[postgres] loaded {len(origin_map):,} existing apple_photos origins")

    try:
        assets, assets_by_pk, assets_by_uuid = extract_assets(
            photos_conn, photos_inspector, origin_map, args.batch_size
        )
        eprint(f"[photos] finished assets: {len(assets):,}")

        people_rows, asset_people_rows = extract_people(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[photos] people={len(people_rows):,} asset_people={len(asset_people_rows):,}")

        memories_rows, memory_asset_rows = extract_memories(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[photos] memories={len(memories_rows):,} memory_assets={len(memory_asset_rows):,}")

        moments_rows = extract_moments(photos_conn, photos_inspector)
        eprint(f"[photos] moments={len(moments_rows):,}")

        albums_rows, album_asset_rows = extract_albums(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[photos] albums={len(albums_rows):,} album_assets={len(album_asset_rows):,}")

        keywords_rows, keyword_asset_rows = extract_keywords(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[photos] keywords={len(keywords_rows):,} asset_keywords={len(keyword_asset_rows):,}")

        search_groups_rows: list[dict[str, Any]] = []
        asset_search_label_rows: list[dict[str, Any]] = []
        psi_conn = maybe_open_db(library_path, "database/search/psi.sqlite")
        if psi_conn:
            try:
                psi_inspector = SqliteInspector(psi_conn)
                search_groups_rows, asset_search_label_rows = extract_search_index(
                    psi_conn, psi_inspector, assets_by_uuid
                )
                eprint(
                    "[psi] search_groups="
                    f"{len(search_groups_rows):,} asset_scene_search_labels={len(asset_search_label_rows):,}"
                )
            finally:
                psi_conn.close()

        placemark_rows: list[dict[str, Any]] = []
        asset_placemark_rows: list[dict[str, Any]] = []
        placemark_conn = maybe_open_db(
            library_path,
            "private/com.apple.photoanalysisd/caches/graph/CLSLocationCache.sqlite",
        )
        if placemark_conn:
            try:
                placemark_inspector = SqliteInspector(placemark_conn)
                placemark_rows = extract_placemarks(placemark_conn, placemark_inspector)
                asset_placemark_rows = match_assets_to_placemarks(
                    assets, placemark_rows, args.placemark_radius_m
                )
                eprint(
                    f"[graph] placemarks={len(placemark_rows):,} asset_placemarks={len(asset_placemark_rows):,}"
                )
            finally:
                placemark_conn.close()

        public_event_rows: list[dict[str, Any]] = []
        asset_public_event_rows: list[dict[str, Any]] = []
        if not args.skip_public_events:
            public_event_conn = maybe_open_db(
                library_path,
                "private/com.apple.photoanalysisd/caches/graph/CLSPublicEventCache.sqlite",
            )
            if public_event_conn:
                try:
                    public_event_inspector = SqliteInspector(public_event_conn)
                    public_event_rows = extract_public_events(public_event_conn, public_event_inspector)
                    asset_public_event_rows = match_assets_to_public_events(
                        assets,
                        public_event_rows,
                        args.public_event_radius_m,
                        args.public_event_time_window_hours,
                    )
                    eprint(
                        f"[graph] public_events={len(public_event_rows):,} asset_public_events={len(asset_public_event_rows):,}"
                    )
                finally:
                    public_event_conn.close()

        business_item_rows: list[dict[str, Any]] = []
        asset_business_item_rows: list[dict[str, Any]] = []
        if not args.skip_business_items:
            for cache_kind, relative_path in BUSINESS_CACHE_PATHS.items():
                cache_conn = maybe_open_db(library_path, relative_path)
                if not cache_conn:
                    continue
                try:
                    cache_inspector = SqliteInspector(cache_conn)
                    rows = extract_business_items(cache_conn, cache_inspector, cache_kind)
                    business_item_rows.extend(rows)
                    matches = match_assets_to_business_items(
                        assets, rows, args.business_radius_m, args.max_business_matches
                    )
                    asset_business_item_rows.extend(matches)
                    eprint(
                        f"[graph] business cache={cache_kind} items={len(rows):,} matches={len(matches):,}"
                    )
                finally:
                    cache_conn.close()
            asset_business_item_rows = dedupe_dict_rows(asset_business_item_rows, ("row_key",))

        scene_row_count = 0
        if sink:
            sink.truncate_stage_tables()
            stage_asset_rows = [asset.to_stage_row() for asset in assets]
            sink.insert_rows(
                "apple_photos_assets",
                [
                    "apple_uuid",
                    "local_identifier",
                    "canonical_media_asset_id",
                    "filename",
                    "original_filename",
                    "title",
                    "caption",
                    "ocr_text",
                    "taken_at",
                    "timezone_offset_min",
                    "gps_lat",
                    "gps_lng",
                    "media_kind",
                    "favorite_flag",
                    "managed_filename",
                    "directory_shard",
                    "uniform_type_identifier",
                    "original_file_size_bytes",
                    "original_width",
                    "original_height",
                    "display_width",
                    "display_height",
                    "metadata_json",
                ],
                stage_asset_rows,
            )
            sink.insert_rows(
                "apple_photos_asset_aesthetics",
                [
                    "apple_uuid",
                    "failure_score",
                    "harmonious_color_score",
                    "immersiveness_score",
                    "interesting_subject_score",
                    "intrusive_object_presence_score",
                    "lively_color_score",
                    "low_light_score",
                    "noise_score",
                    "pleasant_camera_tilt_score",
                    "pleasant_composition_score",
                    "pleasant_lighting_score",
                    "pleasant_pattern_score",
                    "pleasant_perspective_score",
                    "pleasant_post_processing_score",
                    "pleasant_reflection_score",
                    "pleasant_symmetry_score",
                    "sharply_focused_subject_score",
                    "tastefully_blurred_score",
                    "well_chosen_subject_score",
                    "well_framed_subject_score",
                    "well_timed_shot_score",
                ],
                [aesthetic_row_for_asset(asset) for asset in assets],
            )
            sink.insert_rows(
                "apple_photos_search_groups",
                ["group_id", "category", "lookup_identifier", "label_text"],
                [row for row in search_groups_rows if row.get("group_id") is not None],
            )
            sink.insert_rows(
                "apple_photos_asset_search_labels",
                ["apple_uuid", "group_id", "label_text", "lookup_identifier"],
                asset_search_label_rows,
            )
            sink.insert_rows(
                "apple_photos_memories",
                [
                    "memory_pk",
                    "memory_uuid",
                    "title",
                    "subtitle",
                    "category",
                    "start_at",
                    "end_at",
                    "score",
                    "metadata_json",
                ],
                memories_rows,
            )
            sink.insert_rows(
                "apple_photos_memory_assets",
                ["memory_pk", "apple_uuid", "relation_type"],
                memory_asset_rows,
            )
            sink.insert_rows(
                "apple_photos_moments",
                [
                    "moment_pk",
                    "moment_uuid",
                    "title",
                    "subtitle",
                    "start_at",
                    "end_at",
                    "approx_lat",
                    "approx_lng",
                    "moment_type",
                    "metadata_json",
                ],
                moments_rows,
            )
            sink.insert_rows(
                "apple_photos_albums",
                ["album_pk", "album_uuid", "title", "kind", "subtype", "cloud_guid", "metadata_json"],
                albums_rows,
            )
            sink.insert_rows(
                "apple_photos_album_assets",
                ["album_pk", "apple_uuid"],
                album_asset_rows,
            )
            sink.insert_rows(
                "apple_photos_keywords",
                ["keyword_pk", "keyword_name", "metadata_json"],
                keywords_rows,
            )
            sink.insert_rows(
                "apple_photos_asset_keywords",
                ["keyword_pk", "apple_uuid"],
                keyword_asset_rows,
            )
            sink.insert_rows(
                "apple_photos_people",
                [
                    "person_key",
                    "person_uuid",
                    "person_pk",
                    "full_name",
                    "display_name",
                    "face_count",
                    "is_named",
                    "metadata_json",
                ],
                people_rows,
            )
            sink.insert_rows(
                "apple_photos_asset_people",
                [
                    "apple_uuid",
                    "person_key",
                    "person_uuid",
                    "person_pk",
                    "display_name",
                    "face_count",
                    "face_rows_json",
                ],
                asset_people_rows,
            )
            sink.insert_rows(
                "apple_photos_placemarks",
                [
                    "placemark_pk",
                    "lat",
                    "lng",
                    "administrative_area",
                    "locality",
                    "sub_locality",
                    "thoroughfare",
                    "iso_country_code",
                    "areas_of_interest",
                ],
                placemark_rows,
            )
            sink.insert_rows(
                "apple_photos_asset_placemarks",
                ["apple_uuid", "placemark_pk", "distance_m"],
                asset_placemark_rows,
            )
            sink.insert_rows(
                "apple_photos_public_events",
                [
                    "public_event_pk",
                    "name",
                    "local_start_at",
                    "lat",
                    "lng",
                    "business_item_muid",
                    "metadata_json",
                ],
                public_event_rows,
            )
            sink.insert_rows(
                "apple_photos_asset_public_events",
                ["row_key", "apple_uuid", "public_event_pk", "distance_m", "time_delta_seconds"],
                asset_public_event_rows,
            )
            sink.insert_rows(
                "apple_photos_business_items",
                ["cache_kind", "item_pk", "name", "business_categories", "lat", "lng", "iso_country_code"],
                business_item_rows,
            )
            sink.insert_rows(
                "apple_photos_asset_business_items",
                ["row_key", "apple_uuid", "cache_kind", "item_pk", "distance_m"],
                asset_business_item_rows,
            )

            if not args.skip_scene_classifications:
                seen_scene_row_keys: set[str] = set()
                for scene_chunk in extract_scene_classifications(photos_conn, photos_inspector, assets_by_pk):
                    deduped_chunk = []
                    for row in scene_chunk:
                        rk = row.get("row_key")
                        if rk is None or rk in seen_scene_row_keys:
                            continue
                        seen_scene_row_keys.add(rk)
                        deduped_chunk.append(row)
                    if not deduped_chunk:
                        continue
                    scene_row_count += len(deduped_chunk)
                    sink.insert_rows(
                        "apple_photos_asset_scene_labels",
                        [
                            "row_key",
                            "apple_uuid",
                            "scene_identifier",
                            "confidence",
                            "packed_bounding_box",
                            "start_time_seconds",
                            "duration_seconds",
                        ],
                        deduped_chunk,
                        page_size=2_000,
                    )
                    eprint(f"[photos] scene labels inserted: {scene_row_count:,}")

            sink.sync_canonical_media()
            eprint("[postgres] canonical media_assets/media_asset_origins sync complete")

        matched_assets = sum(1 for asset in assets if asset.canonical_media_asset_id)
        print(
            json_dumps(
                {
                    "libraryPath": str(library_path),
                    "assetCount": len(assets),
                    "matchedCanonicalAssets": matched_assets,
                    "peopleCount": len(people_rows),
                    "memoryCount": len(memories_rows),
                    "momentCount": len(moments_rows),
                    "albumCount": len(albums_rows),
                    "keywordCount": len(keywords_rows),
                    "searchGroupCount": len(search_groups_rows),
                    "sceneSearchLabelCount": len(asset_search_label_rows),
                    "sceneClassificationCount": scene_row_count,
                    "placemarkCount": len(placemark_rows),
                    "publicEventCount": len(public_event_rows),
                    "businessItemCount": len(business_item_rows),
                    "dryRun": args.dry_run,
                }
            )
        )
        return 0
    finally:
        photos_conn.close()
        if sink:
            sink.close()


if __name__ == "__main__":
    raise SystemExit(main())

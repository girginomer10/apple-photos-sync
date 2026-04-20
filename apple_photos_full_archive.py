#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import apple_photos_stage1_sync as stage1

METADATA_RELATIVE_PATHS = [
    "database",
    "internal",
    "private",
    "external",
    "scopes",
    "resources/caches",
    "resources/renders",
    "resources/journals",
    "resources/cpl",
    "resources/smartsharing",
]

DERIVATIVE_RELATIVE_PATHS = [
    "resources/derivatives",
    "resources/streams",
    "resources/partialvideo",
]


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=stage1.json_default, sort_keys=True)


def resolve_destination_bundle(library_path: Path, archive_root: Path, archive_name: str | None) -> Path:
    bundle_name = archive_name.strip() if archive_name else library_path.name
    if not bundle_name.endswith(".photoslibrary"):
        bundle_name = f"{bundle_name}.photoslibrary"
    return archive_root / bundle_name


def rsync_path(source_path: Path, destination_path: Path) -> None:
    rsync = shutil.which("rsync")
    if not rsync:
        raise RuntimeError("rsync is required for resumable archive copy mode.")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source_arg = f"{source_path}/" if source_path.is_dir() else str(source_path)
    destination_arg = f"{destination_path}/" if source_path.is_dir() else str(destination_path)
    cmd = [
        rsync,
        "-aEH",
        "--partial",
        "--append-verify",
        "--human-readable",
        "--info=progress2,stats1,name0",
        "--protect-args",
        source_arg,
        destination_arg,
    ]
    subprocess.run(cmd, check=True)


def copy_library_bundle(source_bundle: Path, destination_bundle: Path) -> None:
    destination_bundle.parent.mkdir(parents=True, exist_ok=True)
    rsync = shutil.which("rsync")
    if rsync:
        eprint(f"[archive] rsync bundle -> {destination_bundle}")
        rsync_path(source_bundle, destination_bundle)
        return

    eprint(f"[archive] rsync not found; falling back to shutil.copytree -> {destination_bundle}")
    if destination_bundle.exists():
        shutil.rmtree(destination_bundle)
    shutil.copytree(source_bundle, destination_bundle, symlinks=True)


def copy_metadata_bundle(source_bundle: Path, destination_bundle: Path, include_derivatives: bool) -> None:
    destination_bundle.mkdir(parents=True, exist_ok=True)
    for child in sorted(source_bundle.iterdir()):
        if child.is_file():
            destination_file = destination_bundle / child.name
            eprint(f"[archive] copy root file {child.name} -> {destination_file}")
            rsync_path(child, destination_file)
    relative_paths = list(METADATA_RELATIVE_PATHS)
    if include_derivatives:
        relative_paths.extend(DERIVATIVE_RELATIVE_PATHS)
    for relative in relative_paths:
        source_path = source_bundle / relative
        if not source_path.exists():
            eprint(f"[archive] skip missing metadata path: {relative}")
            continue
        destination_path = destination_bundle / relative
        eprint(f"[archive] copy metadata path {relative} -> {destination_path}")
        rsync_path(source_path, destination_path)


def ensure_index_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS archive_meta (
          key TEXT PRIMARY KEY,
          value_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS assets (
          apple_uuid TEXT PRIMARY KEY,
          local_identifier TEXT NOT NULL UNIQUE,
          media_kind TEXT NOT NULL,
          filename TEXT,
          original_filename TEXT,
          title TEXT,
          caption TEXT,
          ocr_text TEXT,
          taken_at TEXT,
          timezone_offset_min INTEGER,
          gps_lat REAL,
          gps_lng REAL,
          favorite_flag INTEGER NOT NULL DEFAULT 0,
          managed_filename TEXT,
          directory_shard TEXT,
          uniform_type_identifier TEXT,
          original_file_size_bytes INTEGER,
          original_width INTEGER,
          original_height INTEGER,
          display_width INTEGER,
          display_height INTEGER,
          original_rel_path TEXT,
          original_abs_path TEXT,
          original_exists INTEGER NOT NULL DEFAULT 0,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_aesthetics (
          apple_uuid TEXT PRIMARY KEY,
          payload_json TEXT NOT NULL,
          FOREIGN KEY (apple_uuid) REFERENCES assets(apple_uuid) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS people (
          person_key TEXT PRIMARY KEY,
          person_uuid TEXT,
          person_pk INTEGER,
          full_name TEXT,
          display_name TEXT,
          face_count INTEGER NOT NULL DEFAULT 0,
          is_named INTEGER NOT NULL DEFAULT 0,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_people (
          apple_uuid TEXT NOT NULL,
          person_key TEXT NOT NULL,
          person_uuid TEXT,
          person_pk INTEGER,
          display_name TEXT,
          face_count INTEGER NOT NULL DEFAULT 0,
          face_rows_json TEXT NOT NULL,
          PRIMARY KEY (apple_uuid, person_key)
        );

        CREATE TABLE IF NOT EXISTS memories (
          memory_pk INTEGER PRIMARY KEY,
          memory_uuid TEXT,
          title TEXT,
          subtitle TEXT,
          category INTEGER,
          start_at TEXT,
          end_at TEXT,
          score REAL,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_assets (
          memory_pk INTEGER NOT NULL,
          apple_uuid TEXT NOT NULL,
          relation_type TEXT NOT NULL,
          PRIMARY KEY (memory_pk, apple_uuid, relation_type)
        );

        CREATE TABLE IF NOT EXISTS moments (
          moment_pk INTEGER PRIMARY KEY,
          moment_uuid TEXT,
          title TEXT,
          subtitle TEXT,
          start_at TEXT,
          end_at TEXT,
          approx_lat REAL,
          approx_lng REAL,
          moment_type TEXT,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS albums (
          album_pk INTEGER PRIMARY KEY,
          album_uuid TEXT,
          title TEXT,
          kind TEXT,
          subtype TEXT,
          cloud_guid TEXT,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS album_assets (
          album_pk INTEGER NOT NULL,
          apple_uuid TEXT NOT NULL,
          PRIMARY KEY (album_pk, apple_uuid)
        );

        CREATE TABLE IF NOT EXISTS keywords (
          keyword_pk INTEGER PRIMARY KEY,
          keyword_name TEXT,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_keywords (
          keyword_pk INTEGER NOT NULL,
          apple_uuid TEXT NOT NULL,
          PRIMARY KEY (keyword_pk, apple_uuid)
        );

        CREATE TABLE IF NOT EXISTS search_groups (
          group_id INTEGER PRIMARY KEY,
          category INTEGER NOT NULL,
          lookup_identifier TEXT,
          label_text TEXT
        );

        CREATE TABLE IF NOT EXISTS asset_search_labels (
          apple_uuid TEXT NOT NULL,
          group_id INTEGER NOT NULL,
          label_text TEXT,
          lookup_identifier TEXT,
          PRIMARY KEY (apple_uuid, group_id)
        );

        CREATE TABLE IF NOT EXISTS asset_scene_labels (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          scene_identifier INTEGER NOT NULL,
          confidence REAL,
          packed_bounding_box TEXT,
          start_time_seconds REAL,
          duration_seconds REAL
        );

        CREATE TABLE IF NOT EXISTS asset_resources (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          resource_pk INTEGER,
          resource_type INTEGER,
          version INTEGER,
          data_length INTEGER,
          local_availability INTEGER,
          local_availability_target INTEGER,
          remote_availability INTEGER,
          remote_availability_target INTEGER,
          cloud_local_state INTEGER,
          cloud_source_type INTEGER,
          recipe_id INTEGER,
          sidecar_index INTEGER,
          file_id INTEGER,
          datastore_class_id INTEGER,
          datastore_subtype INTEGER,
          compact_uti TEXT,
          codec_fourcc TEXT,
          fingerprint TEXT,
          stable_hash TEXT,
          datastore_key_hex TEXT,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_blob_payloads (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          payload_type TEXT NOT NULL,
          source_table TEXT NOT NULL,
          source_pk INTEGER,
          blob_column TEXT NOT NULL,
          byte_length INTEGER NOT NULL,
          blob_sha1 TEXT,
          blob_format TEXT,
          decoded_json TEXT,
          raw_blob BLOB,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS person_blob_payloads (
          row_key TEXT PRIMARY KEY,
          person_key TEXT NOT NULL,
          person_uuid TEXT,
          person_pk INTEGER,
          display_name TEXT,
          payload_type TEXT NOT NULL,
          source_table TEXT NOT NULL,
          source_pk INTEGER,
          blob_column TEXT NOT NULL,
          byte_length INTEGER NOT NULL,
          blob_sha1 TEXT,
          blob_format TEXT,
          decoded_json TEXT,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_faceprints (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          face_pk INTEGER,
          faceprint_pk INTEGER,
          person_key TEXT,
          person_uuid TEXT,
          person_pk INTEGER,
          display_name TEXT,
          detected_face_uuid TEXT,
          faceprint_version INTEGER,
          byte_length INTEGER NOT NULL,
          blob_sha1 TEXT,
          blob_format TEXT,
          faceprint_blob BLOB NOT NULL,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_vu_observations (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          moment_uuid TEXT,
          observation_pk INTEGER,
          identifier INTEGER,
          observation_type INTEGER,
          source INTEGER,
          client INTEGER,
          mapping INTEGER,
          is_primary INTEGER,
          confidence REAL,
          quality REAL,
          asset_suffix TEXT,
          embedding_format TEXT,
          embedding_dimensions INTEGER,
          embedding_l2_norm REAL,
          embedding_blob_sha1 TEXT,
          embedding_blob BLOB,
          contextual_embedding_format TEXT,
          contextual_embedding_dimensions INTEGER,
          contextual_embedding_l2_norm REAL,
          contextual_embedding_blob_sha1 TEXT,
          contextual_embedding_blob BLOB,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS placemarks (
          placemark_pk INTEGER PRIMARY KEY,
          lat REAL,
          lng REAL,
          administrative_area TEXT,
          locality TEXT,
          sub_locality TEXT,
          thoroughfare TEXT,
          iso_country_code TEXT,
          areas_of_interest TEXT
        );

        CREATE TABLE IF NOT EXISTS asset_placemarks (
          apple_uuid TEXT PRIMARY KEY,
          placemark_pk INTEGER NOT NULL,
          distance_m REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS public_events (
          public_event_pk INTEGER PRIMARY KEY,
          name TEXT,
          local_start_at TEXT,
          lat REAL,
          lng REAL,
          business_item_muid TEXT,
          metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS asset_public_events (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          public_event_pk INTEGER NOT NULL,
          distance_m REAL NOT NULL,
          time_delta_seconds REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS business_items (
          cache_kind TEXT NOT NULL,
          item_pk INTEGER NOT NULL,
          name TEXT,
          business_categories TEXT,
          lat REAL,
          lng REAL,
          iso_country_code TEXT,
          PRIMARY KEY (cache_kind, item_pk)
        );

        CREATE TABLE IF NOT EXISTS asset_business_items (
          row_key TEXT PRIMARY KEY,
          apple_uuid TEXT NOT NULL,
          cache_kind TEXT NOT NULL,
          item_pk INTEGER NOT NULL,
          distance_m REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bundle_files (
          rel_path TEXT PRIMARY KEY,
          file_size_bytes INTEGER NOT NULL,
          mtime_epoch REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bundle_databases (
          rel_path TEXT PRIMARY KEY,
          file_size_bytes INTEGER NOT NULL,
          table_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bundle_table_inventory (
          db_rel_path TEXT NOT NULL,
          table_name TEXT NOT NULL,
          row_count INTEGER,
          PRIMARY KEY (db_rel_path, table_name)
        );
        """
    )
    conn.commit()


def clear_index_tables(conn: sqlite3.Connection, with_table_inventory: bool) -> None:
    tables = [
        "archive_meta",
        "assets",
        "asset_aesthetics",
        "people",
        "asset_people",
        "memories",
        "memory_assets",
        "moments",
        "albums",
        "album_assets",
        "keywords",
        "asset_keywords",
        "search_groups",
        "asset_search_labels",
        "asset_scene_labels",
        "asset_resources",
        "asset_blob_payloads",
        "person_blob_payloads",
        "asset_faceprints",
        "asset_vu_observations",
        "placemarks",
        "asset_placemarks",
        "public_events",
        "asset_public_events",
        "business_items",
        "asset_business_items",
        "bundle_files",
        "bundle_databases",
    ]
    if with_table_inventory:
        tables.append("bundle_table_inventory")
    for table in tables:
        conn.execute(f"DELETE FROM {table}")
    conn.commit()


def insert_many(conn: sqlite3.Connection, table: str, columns: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    payload = [tuple(row.get(column) for column in columns) for row in rows]
    conn.executemany(sql, payload)
    conn.commit()


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def asset_original_paths(bundle_path: Path, asset: stage1.AssetRecord) -> tuple[str | None, str | None, int]:
    if not asset.directory_shard or not asset.managed_filename:
        return None, None, 0
    rel_path = f"originals/{asset.directory_shard}/{asset.managed_filename}"
    abs_path = bundle_path / rel_path
    return rel_path, str(abs_path), int(abs_path.exists())


def build_bundle_file_rows(bundle_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(bundle_path.rglob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        rows.append(
            {
                "rel_path": str(path.relative_to(bundle_path)),
                "file_size_bytes": stat.st_size,
                "mtime_epoch": stat.st_mtime,
            }
        )
    return rows


def build_table_inventory(bundle_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    db_rows: list[dict[str, Any]] = []
    table_rows: list[dict[str, Any]] = []
    for path in sorted(bundle_path.rglob("*")):
        if not path.is_file():
            continue
        lower = path.name.lower()
        if not (lower.endswith(".sqlite") or lower.endswith(".db")):
            continue
        rel_path = str(path.relative_to(bundle_path))
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(stage1.sqlite_uri(path), uri=True)
            table_names = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
            ]
            db_rows.append(
                {
                    "rel_path": rel_path,
                    "file_size_bytes": path.stat().st_size,
                    "table_count": len(table_names),
                }
            )
            for table_name in table_names:
                try:
                    row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                except sqlite3.DatabaseError:
                    row_count = None
                table_rows.append(
                    {
                        "db_rel_path": rel_path,
                        "table_name": table_name,
                        "row_count": row_count,
                    }
                )
        except sqlite3.DatabaseError:
            db_rows.append(
                {
                    "rel_path": rel_path,
                    "file_size_bytes": path.stat().st_size,
                    "table_count": 0,
                }
            )
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
    return db_rows, table_rows


def archive_meta_rows(meta: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"key": key, "value_json": json_dumps(value)} for key, value in meta.items()]


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a full Apple Photos archive: bundle mirror + self-contained SQLite index."
    )
    parser.add_argument("--library-path", help="Path to the source .photoslibrary bundle.")
    parser.add_argument("--archive-root", required=True, help="Destination folder on the external drive.")
    parser.add_argument("--archive-name", help="Destination bundle name. Defaults to source bundle name.")
    parser.add_argument("--index-name", default="archive-index.sqlite", help="SQLite index filename.")
    parser.add_argument("--skip-bundle-copy", action="store_true", help="Do not rsync the bundle; index the existing source bundle.")
    parser.add_argument("--skip-index", action="store_true", help="Do not build the archive SQLite index.")
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Copy only non-media bundle data (DBs, analysis caches, metadata paths). Originals/derivatives are skipped.",
    )
    parser.add_argument(
        "--include-derivatives",
        action="store_true",
        help="When used with --metadata-only, also copy generated previews/thumbnails under resources/derivatives.",
    )
    parser.add_argument("--with-table-inventory", action="store_true", help="Count every table in every SQLite/DB file inside the bundle.")
    parser.add_argument("--skip-business-items", action="store_true")
    parser.add_argument("--skip-public-events", action="store_true")
    parser.add_argument("--skip-scene-classifications", action="store_true")
    parser.add_argument("--batch-size", type=int, default=stage1.DEFAULT_BATCH_SIZE)
    parser.add_argument("--placemark-radius-m", type=float, default=300.0)
    parser.add_argument("--business-radius-m", type=float, default=200.0)
    parser.add_argument("--public-event-radius-m", type=float, default=500.0)
    parser.add_argument("--public-event-time-window-hours", type=float, default=24.0)
    parser.add_argument("--max-business-matches", type=int, default=5)
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    source_library = stage1.discover_photos_library(args.library_path)
    archive_root = Path(args.archive_root).expanduser()
    archive_root.mkdir(parents=True, exist_ok=True)

    archive_bundle = resolve_destination_bundle(source_library, archive_root, args.archive_name)
    if not args.skip_bundle_copy:
        if args.metadata_only:
            copy_metadata_bundle(source_library, archive_bundle, include_derivatives=args.include_derivatives)
        else:
            copy_library_bundle(source_library, archive_bundle)
    else:
        eprint("[archive] skipping bundle copy; indexing source bundle in place")

    indexed_bundle = archive_bundle if not args.skip_bundle_copy else source_library
    manifest_path = archive_root / "archive-manifest.json"

    if args.skip_index:
        manifest = {
            "sourceLibrary": str(source_library),
            "indexedBundle": str(indexed_bundle),
            "bundleCopied": not args.skip_bundle_copy,
            "warning": "Cloud-only originals that are not downloaded locally cannot be archived until Photos is configured to download originals to this Mac.",
        }
        manifest_path.write_text(json_dumps(manifest) + "\n", encoding="utf-8")
        print(json_dumps(manifest))
        return 0

    photos_conn = stage1.open_sqlite_readonly(indexed_bundle / "database/Photos.sqlite")
    photos_inspector = stage1.SqliteInspector(photos_conn)
    index_path = archive_root / args.index_name
    index_conn = sqlite3.connect(index_path)
    index_conn.row_factory = sqlite3.Row

    try:
        ensure_index_schema(index_conn)
        clear_index_tables(index_conn, with_table_inventory=args.with_table_inventory)

        assets, assets_by_pk, assets_by_uuid = stage1.extract_assets(
            photos_conn, photos_inspector, {}, args.batch_size
        )
        eprint(f"[index] assets extracted: {len(assets):,}")
        people_rows, asset_people_rows = stage1.extract_people(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] people extracted: {len(people_rows):,} people / {len(asset_people_rows):,} asset-person links")
        memories_rows, memory_asset_rows = stage1.extract_memories(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] memories extracted: {len(memories_rows):,} memories / {len(memory_asset_rows):,} links")
        moments_rows = stage1.extract_moments(photos_conn, photos_inspector)
        eprint(f"[index] moments extracted: {len(moments_rows):,}")
        albums_rows, album_asset_rows = stage1.extract_albums(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] albums extracted: {len(albums_rows):,} albums / {len(album_asset_rows):,} links")
        keywords_rows, keyword_asset_rows = stage1.extract_keywords(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] keywords extracted: {len(keywords_rows):,} keywords / {len(keyword_asset_rows):,} links")
        asset_resource_rows = stage1.extract_asset_resources(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] asset resources extracted: {len(asset_resource_rows):,}")
        asset_blob_payload_rows = stage1.extract_asset_blob_payloads(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] asset blob payloads extracted: {len(asset_blob_payload_rows):,}")
        person_blob_payload_rows = stage1.extract_person_blob_payloads(photos_conn, photos_inspector)
        eprint(f"[index] person blob payloads extracted: {len(person_blob_payload_rows):,}")
        asset_faceprint_rows = stage1.extract_faceprints(photos_conn, photos_inspector, assets_by_pk)
        eprint(f"[index] faceprints extracted: {len(asset_faceprint_rows):,}")

        vu_observation_rows: list[dict[str, Any]] = []
        vu_conn = stage1.maybe_open_db(
            indexed_bundle,
            "private/com.apple.mediaanalysisd/caches/vision/VUIndex.sqlite",
        )
        if vu_conn:
            try:
                vu_inspector = stage1.SqliteInspector(vu_conn)
                vu_observation_rows = stage1.extract_vu_observations(vu_conn, vu_inspector, set(assets_by_uuid))
                eprint(f"[index] VU observations extracted: {len(vu_observation_rows):,}")
            finally:
                vu_conn.close()

        search_groups_rows: list[dict[str, Any]] = []
        asset_search_label_rows: list[dict[str, Any]] = []
        psi_conn = stage1.maybe_open_db(indexed_bundle, "database/search/psi.sqlite")
        if psi_conn:
            try:
                psi_inspector = stage1.SqliteInspector(psi_conn)
                search_groups_rows, asset_search_label_rows = stage1.extract_search_index(
                    psi_conn, psi_inspector, assets_by_uuid
                )
                search_groups_rows = stage1.dedupe_dict_rows(search_groups_rows, ("group_id",))
                asset_search_label_rows = stage1.dedupe_dict_rows(
                    asset_search_label_rows, ("apple_uuid", "group_id")
                )
                eprint(
                    f"[index] search groups extracted: {len(search_groups_rows):,} groups / "
                    f"{len(asset_search_label_rows):,} asset-label links"
                )
            finally:
                psi_conn.close()

        placemark_rows: list[dict[str, Any]] = []
        asset_placemark_rows: list[dict[str, Any]] = []
        placemark_conn = stage1.maybe_open_db(
            indexed_bundle,
            "private/com.apple.photoanalysisd/caches/graph/CLSLocationCache.sqlite",
        )
        if placemark_conn:
            try:
                placemark_inspector = stage1.SqliteInspector(placemark_conn)
                placemark_rows = stage1.extract_placemarks(placemark_conn, placemark_inspector)
                asset_placemark_rows = stage1.match_assets_to_placemarks(
                    assets, placemark_rows, args.placemark_radius_m
                )
                eprint(
                    f"[index] placemarks extracted: {len(placemark_rows):,} placemarks / "
                    f"{len(asset_placemark_rows):,} asset matches"
                )
            finally:
                placemark_conn.close()

        public_event_rows: list[dict[str, Any]] = []
        asset_public_event_rows: list[dict[str, Any]] = []
        if not args.skip_public_events:
            public_event_conn = stage1.maybe_open_db(
                indexed_bundle,
                "private/com.apple.photoanalysisd/caches/graph/CLSPublicEventCache.sqlite",
            )
            if public_event_conn:
                try:
                    public_event_inspector = stage1.SqliteInspector(public_event_conn)
                    public_event_rows = stage1.extract_public_events(public_event_conn, public_event_inspector)
                    asset_public_event_rows = stage1.match_assets_to_public_events(
                        assets,
                        public_event_rows,
                        args.public_event_radius_m,
                        args.public_event_time_window_hours,
                    )
                    eprint(
                        f"[index] public events extracted: {len(public_event_rows):,} events / "
                        f"{len(asset_public_event_rows):,} asset matches"
                    )
                finally:
                    public_event_conn.close()

        business_item_rows: list[dict[str, Any]] = []
        asset_business_item_rows: list[dict[str, Any]] = []
        if not args.skip_business_items:
            for cache_kind, relative_path in stage1.BUSINESS_CACHE_PATHS.items():
                cache_conn = stage1.maybe_open_db(indexed_bundle, relative_path)
                if not cache_conn:
                    continue
                try:
                    cache_inspector = stage1.SqliteInspector(cache_conn)
                    rows = stage1.extract_business_items(cache_conn, cache_inspector, cache_kind)
                    business_item_rows.extend(rows)
                    asset_business_item_rows.extend(
                        stage1.match_assets_to_business_items(
                            assets, rows, args.business_radius_m, args.max_business_matches
                        )
                    )
                    eprint(f"[index] business cache {cache_kind}: {len(rows):,} items")
                finally:
                    cache_conn.close()
            asset_business_item_rows = stage1.dedupe_dict_rows(asset_business_item_rows, ("row_key",))
            eprint(f"[index] asset-business matches: {len(asset_business_item_rows):,}")

        scene_label_count = 0

        eprint("[index] scanning bundle file inventory...")
        bundle_file_rows = build_bundle_file_rows(indexed_bundle)
        eprint(f"[index] bundle files inventoried: {len(bundle_file_rows):,}")
        db_rows, table_rows = build_table_inventory(indexed_bundle) if args.with_table_inventory else ([], [])
        if args.with_table_inventory:
            eprint(f"[index] sqlite table inventory: {len(db_rows):,} db files / {len(table_rows):,} tables")

        asset_rows_for_index: list[dict[str, Any]] = []
        for asset in assets:
            asset.refresh_metadata()
            original_rel_path, original_abs_path, original_exists = asset_original_paths(indexed_bundle, asset)
            asset_rows_for_index.append(
                {
                    "apple_uuid": asset.apple_uuid,
                    "local_identifier": asset.local_identifier,
                    "media_kind": asset.media_kind,
                    "filename": asset.filename,
                    "original_filename": asset.original_filename,
                    "title": asset.title,
                    "caption": asset.caption,
                    "ocr_text": asset.ocr_text,
                    "taken_at": iso_or_none(asset.taken_at),
                    "timezone_offset_min": asset.timezone_offset_min,
                    "gps_lat": asset.gps_lat,
                    "gps_lng": asset.gps_lng,
                    "favorite_flag": int(asset.favorite_flag),
                    "managed_filename": asset.managed_filename,
                    "directory_shard": asset.directory_shard,
                    "uniform_type_identifier": asset.uniform_type_identifier,
                    "original_file_size_bytes": asset.original_file_size_bytes,
                    "original_width": asset.original_width,
                    "original_height": asset.original_height,
                    "display_width": asset.display_width,
                    "display_height": asset.display_height,
                    "original_rel_path": original_rel_path,
                    "original_abs_path": original_abs_path,
                    "original_exists": original_exists,
                    "metadata_json": json_dumps(asset.metadata_json),
                }
            )

        insert_many(
            index_conn,
            "assets",
            [
                "apple_uuid",
                "local_identifier",
                "media_kind",
                "filename",
                "original_filename",
                "title",
                "caption",
                "ocr_text",
                "taken_at",
                "timezone_offset_min",
                "gps_lat",
                "gps_lng",
                "favorite_flag",
                "managed_filename",
                "directory_shard",
                "uniform_type_identifier",
                "original_file_size_bytes",
                "original_width",
                "original_height",
                "display_width",
                "display_height",
                "original_rel_path",
                "original_abs_path",
                "original_exists",
                "metadata_json",
            ],
            asset_rows_for_index,
        )
        insert_many(
            index_conn,
            "asset_aesthetics",
            ["apple_uuid", "payload_json"],
            [
                {"apple_uuid": asset.apple_uuid, "payload_json": json_dumps(stage1.aesthetic_row_for_asset(asset))}
                for asset in assets
            ],
        )
        insert_many(
            index_conn,
            "people",
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
            [
                {
                    **row,
                    "is_named": int(bool(row.get("is_named"))),
                }
                for row in people_rows
            ],
        )
        insert_many(
            index_conn,
            "asset_people",
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
        insert_many(
            index_conn,
            "memories",
            ["memory_pk", "memory_uuid", "title", "subtitle", "category", "start_at", "end_at", "score", "metadata_json"],
            [
                {
                    **row,
                    "start_at": iso_or_none(row.get("start_at")),
                    "end_at": iso_or_none(row.get("end_at")),
                }
                for row in memories_rows
            ],
        )
        insert_many(index_conn, "memory_assets", ["memory_pk", "apple_uuid", "relation_type"], memory_asset_rows)
        insert_many(
            index_conn,
            "moments",
            ["moment_pk", "moment_uuid", "title", "subtitle", "start_at", "end_at", "approx_lat", "approx_lng", "moment_type", "metadata_json"],
            [
                {
                    **row,
                    "start_at": iso_or_none(row.get("start_at")),
                    "end_at": iso_or_none(row.get("end_at")),
                }
                for row in moments_rows
            ],
        )
        insert_many(index_conn, "albums", ["album_pk", "album_uuid", "title", "kind", "subtype", "cloud_guid", "metadata_json"], albums_rows)
        insert_many(index_conn, "album_assets", ["album_pk", "apple_uuid"], album_asset_rows)
        insert_many(index_conn, "keywords", ["keyword_pk", "keyword_name", "metadata_json"], keywords_rows)
        insert_many(index_conn, "asset_keywords", ["keyword_pk", "apple_uuid"], keyword_asset_rows)
        insert_many(index_conn, "search_groups", ["group_id", "category", "lookup_identifier", "label_text"], search_groups_rows)
        insert_many(index_conn, "asset_search_labels", ["apple_uuid", "group_id", "label_text", "lookup_identifier"], asset_search_label_rows)
        if not args.skip_scene_classifications:
            for chunk in stage1.extract_scene_classifications(photos_conn, photos_inspector, assets_by_pk):
                insert_many(
                    index_conn,
                    "asset_scene_labels",
                    ["row_key", "apple_uuid", "scene_identifier", "confidence", "packed_bounding_box", "start_time_seconds", "duration_seconds"],
                    chunk,
                )
                scene_label_count += len(chunk)
                eprint(f"[index] scene labels indexed: {scene_label_count:,}")
        insert_many(
            index_conn,
            "asset_resources",
            [
                "row_key",
                "apple_uuid",
                "resource_pk",
                "resource_type",
                "version",
                "data_length",
                "local_availability",
                "local_availability_target",
                "remote_availability",
                "remote_availability_target",
                "cloud_local_state",
                "cloud_source_type",
                "recipe_id",
                "sidecar_index",
                "file_id",
                "datastore_class_id",
                "datastore_subtype",
                "compact_uti",
                "codec_fourcc",
                "fingerprint",
                "stable_hash",
                "datastore_key_hex",
                "metadata_json",
            ],
            asset_resource_rows,
        )
        insert_many(
            index_conn,
            "asset_blob_payloads",
            [
                "row_key",
                "apple_uuid",
                "payload_type",
                "source_table",
                "source_pk",
                "blob_column",
                "byte_length",
                "blob_sha1",
                "blob_format",
                "decoded_json",
                "raw_blob",
                "metadata_json",
            ],
            asset_blob_payload_rows,
        )
        insert_many(
            index_conn,
            "person_blob_payloads",
            [
                "row_key",
                "person_key",
                "person_uuid",
                "person_pk",
                "display_name",
                "payload_type",
                "source_table",
                "source_pk",
                "blob_column",
                "byte_length",
                "blob_sha1",
                "blob_format",
                "decoded_json",
                "metadata_json",
            ],
            person_blob_payload_rows,
        )
        insert_many(
            index_conn,
            "asset_faceprints",
            [
                "row_key",
                "apple_uuid",
                "face_pk",
                "faceprint_pk",
                "person_key",
                "person_uuid",
                "person_pk",
                "display_name",
                "detected_face_uuid",
                "faceprint_version",
                "byte_length",
                "blob_sha1",
                "blob_format",
                "faceprint_blob",
                "metadata_json",
            ],
            asset_faceprint_rows,
        )
        insert_many(
            index_conn,
            "asset_vu_observations",
            [
                "row_key",
                "apple_uuid",
                "moment_uuid",
                "observation_pk",
                "identifier",
                "observation_type",
                "source",
                "client",
                "mapping",
                "is_primary",
                "confidence",
                "quality",
                "asset_suffix",
                "embedding_format",
                "embedding_dimensions",
                "embedding_l2_norm",
                "embedding_blob_sha1",
                "embedding_blob",
                "contextual_embedding_format",
                "contextual_embedding_dimensions",
                "contextual_embedding_l2_norm",
                "contextual_embedding_blob_sha1",
                "contextual_embedding_blob",
                "metadata_json",
            ],
            vu_observation_rows,
        )
        insert_many(
            index_conn,
            "placemarks",
            ["placemark_pk", "lat", "lng", "administrative_area", "locality", "sub_locality", "thoroughfare", "iso_country_code", "areas_of_interest"],
            placemark_rows,
        )
        insert_many(index_conn, "asset_placemarks", ["apple_uuid", "placemark_pk", "distance_m"], asset_placemark_rows)
        insert_many(
            index_conn,
            "public_events",
            ["public_event_pk", "name", "local_start_at", "lat", "lng", "business_item_muid", "metadata_json"],
            [
                {
                    **row,
                    "local_start_at": iso_or_none(row.get("local_start_at")),
                }
                for row in public_event_rows
            ],
        )
        insert_many(
            index_conn,
            "asset_public_events",
            ["row_key", "apple_uuid", "public_event_pk", "distance_m", "time_delta_seconds"],
            asset_public_event_rows,
        )
        insert_many(
            index_conn,
            "business_items",
            ["cache_kind", "item_pk", "name", "business_categories", "lat", "lng", "iso_country_code"],
            business_item_rows,
        )
        insert_many(
            index_conn,
            "asset_business_items",
            ["row_key", "apple_uuid", "cache_kind", "item_pk", "distance_m"],
            asset_business_item_rows,
        )
        insert_many(index_conn, "bundle_files", ["rel_path", "file_size_bytes", "mtime_epoch"], bundle_file_rows)
        insert_many(index_conn, "bundle_databases", ["rel_path", "file_size_bytes", "table_count"], db_rows)
        if args.with_table_inventory:
            insert_many(index_conn, "bundle_table_inventory", ["db_rel_path", "table_name", "row_count"], table_rows)

        manifest = {
            "sourceLibrary": str(source_library),
            "archiveRoot": str(archive_root),
            "archiveBundle": str(archive_bundle),
            "indexedBundle": str(indexed_bundle),
            "indexSqlite": str(index_path),
            "bundleCopied": not args.skip_bundle_copy,
            "copyMode": "metadata_only" if args.metadata_only else "full_bundle",
            "assetCount": len(assets),
            "peopleCount": len(people_rows),
            "memoryCount": len(memories_rows),
            "momentCount": len(moments_rows),
            "albumCount": len(albums_rows),
            "keywordCount": len(keywords_rows),
            "assetResourceCount": len(asset_resource_rows),
            "assetBlobPayloadCount": len(asset_blob_payload_rows),
            "personBlobPayloadCount": len(person_blob_payload_rows),
            "faceprintCount": len(asset_faceprint_rows),
            "vuObservationCount": len(vu_observation_rows),
            "sceneClassificationCount": scene_label_count,
            "placemarkCount": len(placemark_rows),
            "publicEventCount": len(public_event_rows),
            "businessItemCount": len(business_item_rows),
            "bundleFileCount": len(bundle_file_rows),
            "warning": "This archive preserves all locally available data in the bundle, including raw Apple SQLite/BLOB analysis data. If Photos is using Optimize Mac Storage, cloud-only originals must be downloaded to this Mac before running the archive.",
        }
        insert_many(index_conn, "archive_meta", ["key", "value_json"], archive_meta_rows(manifest))
        manifest_path.write_text(json_dumps(manifest) + "\n", encoding="utf-8")
        print(json_dumps(manifest))
        return 0
    finally:
        photos_conn.close()
        index_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

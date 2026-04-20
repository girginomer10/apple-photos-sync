# apple-photos-sync

Extract rich metadata from a local Apple Photos (`*.photoslibrary`) bundle and either:

1. **Sync to PostgreSQL** — via `apple_photos_stage1_sync.py`, producing a normalized set of `apple_photos_*` tables (assets, people, memories, moments, albums, keywords, placemarks, public events, business POIs, scene labels, aesthetic scores, OCR).
2. **Archive the full bundle + self-contained SQLite index** — via `apple_photos_full_archive.py`, mirroring the `.photoslibrary` folder to an external drive and emitting a portable `archive-index.sqlite` with resource graph, asset/person blob payloads, raw faceprints, VU embeddings, and a complete file inventory.

Both scripts open Apple's internal SQLite files in **read-only** mode and never modify the library.

## What gets extracted (stage-1 sync)

| Source file | What the script reads |
|---|---|
| `database/Photos.sqlite` | asset metadata, caption, aesthetic scores, memories, moments, albums, keywords, people/faces, raw scene classifications |
| `database/search/psi.sqlite` | tokenized OCR and scene-search labels |
| `database/search/CLSLocationCache.sqlite` | placemark cache + asset proximity match |
| `database/search/CLSPublicEventCache.sqlite` | public event cache + asset time/GPS proximity match |
| `database/search/CLSBusinessCategoryCache.{POI,Nature,AOI,ROI}.sqlite` | business item cache + asset proximity match |

The script populates `apple_photos_*` tables (full refresh every run) and optionally enriches pre-existing `media_assets` / `media_asset_origins` rows when a broader photo-asset pipeline is wired in via the `APPLE_PHOTOS` origin key.

## Requirements

- macOS with an Apple Photos library at `~/Pictures/Photos Library.photoslibrary` (or pass `--library-path`).
- PostgreSQL 14+ for the stage-1 sync (SQLite is the source of truth; Postgres is the target).
- Python 3.10+.
- Terminal granted **Full Disk Access** (System Settings → Privacy & Security → Full Disk Access) so Python can read the library bundle.
- Apple Photos **closed** during the run (SQLite WAL locks would otherwise block the reader).

## Install

```bash
git clone https://github.com/<you>/apple-photos-sync.git
cd apple-photos-sync
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Stage-1 sync: extract → PostgreSQL

Set your DSN and run:

```bash
export DATABASE_URL="postgresql://user@localhost:5432/photos"
python apple_photos_stage1_sync.py
```

The script auto-discovers `~/Pictures/Photos Library.photoslibrary` (and the Turkish default `Fotoğraflar Arşivi.photoslibrary`). Override explicitly:

```bash
python apple_photos_stage1_sync.py \
  --library-path "~/Pictures/My Library.photoslibrary" \
  --postgres-dsn "$DATABASE_URL"
```

### Minimal target schema

The script creates every `apple_photos_*` table itself. Two tables are expected to exist for the optional canonical-media enrichment step (leave them empty if you don't use it):

```sql
CREATE TABLE IF NOT EXISTS media_assets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ocr_text TEXT,
  caption TEXT,
  metadata_json JSONB DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS media_asset_origins (
  media_asset_id UUID REFERENCES media_assets(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  source_item_id TEXT NOT NULL,
  source_metadata_json JSONB DEFAULT '{}'::jsonb,
  import_last_seen_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source, source_item_id)
);
```

### Useful flags

- `--dry-run` — extract and print counts without touching Postgres.
- `--force-canonical-ocr` — overwrite `media_assets.ocr_text` even when non-empty.
- `--skip-business-items` — skip all four business caches.
- `--skip-public-events` — skip public-event proximity matches.
- `--skip-scene-classifications` — skip the ~2M-row raw scene-label dump.
- `--placemark-radius-m`, `--business-radius-m`, `--public-event-radius-m` — proximity tuning.
- `--public-event-time-window-hours` — temporal matching window for events.

Example with looser proximity:

```bash
python apple_photos_stage1_sync.py \
  --postgres-dsn "$DATABASE_URL" \
  --business-radius-m 200 \
  --placemark-radius-m 300 \
  --public-event-time-window-hours 24
```

## Full archive: bundle mirror + index

```bash
python apple_photos_full_archive.py \
  --archive-root /Volumes/EXTERNAL_DRIVE/apple-photos-archive \
  --metadata-only \
  --with-table-inventory
```

This:

- rsyncs the `.photoslibrary` bundle to the archive root
- writes a self-contained `archive-index.sqlite` next to it
- extracts assets, people, memories, moments, albums, keywords, placemarks, public events, business items, and a scene-classification index
- materializes the Apple resource graph in `asset_resources`
- decodes Apple plist/blob payloads into `asset_blob_payloads` / `person_blob_payloads`
- stores raw `faceprint` blobs in `asset_faceprints`
- stores raw VU/Vision embedding observations in `asset_vu_observations`
- inventories every file inside the bundle
- (optional) counts every table row in every `.sqlite`/`.db` inside the bundle

`--metadata-only` copies `database`, `internal`, `private`, `external`, `scopes`, and the non-derivative resource caches. Add `--include-derivatives` to also mirror `resources/derivatives`, `resources/streams`, `resources/partialvideo`.

### Archive-index tables (highlights)

- `asset_resources` — resource type, availability, recipe, fingerprint, datastore key
- `asset_blob_payloads` — OCR, text understanding, reverse location, sceneprint, visual lookup, etc.
- `person_blob_payloads` — e.g. contact-matching dictionaries
- `asset_faceprints` — Apple face-descriptor blobs
- `asset_vu_observations` — Vision/VU embeddings
- `asset_scene_classifications` — raw scene IDs with confidence + bbox

## Caveats

- Originals that are **iCloud-only** (Optimize Mac Storage) have `managed_filename` set but no physical file on disk — the archive script will mirror only what is local. Flip on *Download Originals to this Mac* in Photos before archiving if you want the full bundle.
- The raw bundle copy is the future-proof layer; the SQLite index is the app-facing layer.
- Schema stability: Apple's private SQLite schemas evolve between macOS versions. The script defensively checks column existence before referencing anything optional, but new versions may require tweaks.

## License

MIT — see [LICENSE](./LICENSE).

## Credits

This project is a direct read-only extractor over Apple's internal Photos databases. It carries no Apple source code and depends on public SQLite column naming observed on recent macOS releases.

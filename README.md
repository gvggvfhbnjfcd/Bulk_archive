# 2chan Archive

A GitHub Actions worker that continuously archives threads from 2chan boards and uploads them to the [Internet Archive](https://archive.org).

Each thread is saved as a self-contained offline copy (`index.html` plus its images and other assets), streamed into large zip bundles, and shipped to numbered IA items (`bulk-futaba-archive1`, `bulk-futaba-archive2`, …). Everything runs on GitHub's free hosted runners with no server of your own.

## How it works

```
 cron (every 5 min)
        │
        ▼
 ┌──────────────────────── one job, up to 5 h ────────────────────────┐
 │                                                                     │
 │  pass: visit every board → read catalog → pick new / grown threads  │
 │        → download page + assets → append to ONE growing zip         │
 │                                                                     │
 │  idle until 5 min after the pass started (or go again at once)      │
 │                                                                     │
 │  zip reaches BUNDLE_MAX_GB ─► seal ─► uploader.py (background)      │
 │        archiving continues in a fresh zip while the upload runs     │
 └─────────────────────────────────────────────────────────────────────┘
        │  after RUN_HOURS: seal what's left, upload, exit
        ▼
 next cron tick takes over (state restored from the futaba-state branch)
```

**Key points**

- **One long-running job.** The workflow starts a job that loops for `RUN_HOURS` (default 5; GitHub's hard limit is 6). A cron tick every 5 minutes keeps the next job queued, so there is effectively no gap. `concurrency` guarantees only one worker runs at a time.
- **Streaming bundle.** Threads are appended to a single zip on the runner's `/mnt` scratch disk as soon as they are built. A small `index.jsonl` tracks entries while the bundle is open; the real zip central directory (zip64 when needed) is written only when the bundle is sealed. A thread is committed to the bundle only after it is fully written and fsynced, so a crash can't leave a corrupt bundle (`recover()` discards any half-written thread on restart).
- **Background uploads.** Sealed bundles go to `ready/<id>/` and `uploader.py` sends them to IA as a separate process, one at a time, while archiving carries on. Per-file progress is remembered, so a failed manifest upload never re-sends gigabytes, and a bundle is deleted only after IA accepted every file.
- **State survives between jobs.** Runners start with an empty disk, so the "already archived" list (a SQLite DB, gzipped) lives in a single-commit, force-pushed branch called `futaba-state`. Only threads whose bundle IA has *accepted* are saved there; anything in an unsent bundle is simply archived again if the job dies. If the state branch can't be read for any reason other than "first run", the worker stops rather than re-archive everything (fail closed).
- **Re-archiving.** A thread is archived once it has at least `MIN_REPLIES` replies, and archived again after it gains `REARCHIVE_MIN_DELTA` new replies. If the older copy is still in the open bundle, it is dropped, and the zip is compacted at seal time if enough space was wasted (`BUNDLE_COMPACT_MIN_MB`).
- **Board rotation.** Each pass visits a rotating "lead" group first (`CYCLE_LEADS`), then `PRIORITY_BOARDS`, then every other board in shuffled order. Catalogs are fetched with a randomly chosen sort order to surface different threads each time.

## Repository layout

```
.github/workflows/futaba-archive.yml   the scheduled worker
futaba/
  worker.py        main loop (also: --flush)
  archiver.py      catalog parsing, thread + asset download, zip entries, config
  boards.py        list of supported boards
  bundle.py        streaming zip bundle: add, recover, seal, compact
  uploader.py      uploads sealed bundles to IA, manages the item counter
  ia.py            Internet Archive S3 upload layer (official `internetarchive` lib)
  state.py         SQLite state (archived threads, cycle counter)
  state_sync.py    restore / push state to the futaba-state branch
  import_state.py  one-time import of an old state.db
  http_util.py     stdlib HTTP helper (timeouts, redirects, gzip)
  requirements.txt
tests/
  bundle_test.py   unit tests for the bundle (zip64, crash recovery, replacement)
  e2e.py           end-to-end test with a fake board, fake IA and a local git remote
```

> `archiver.py` and `ia.py` still carry some docstrings from an earlier spool-based design (per-thread zips, `SPOOL_MAX_GB`, `RUN_BUDGET_SECONDS`, etc.). The bundle-based flow in `worker.py` / `bundle.py` / `uploader.py` is what actually runs; use the settings below as the reference.

## Setup

1. **Create a repo** and push this code (keep the `futaba/` folder and the workflow path as-is).
2. **Get IA S3 keys** at <https://archive.org/account/s3.php>.
3. **Add repository secrets** (*Settings → Secrets and variables → Actions*):
   - `IA_ACCESS_KEY`
   - `IA_SECRET_KEY`
4. **Allow the workflow to push the state branch**: *Settings → Actions → General → Workflow permissions* → "Read and write permissions". The workflow already requests `contents: write`; `GITHUB_TOKEN` is provided automatically.
5. **Enable Actions**, then either wait for the next cron tick or start it manually from *Actions → Futaba archive worker → Run workflow*.

The first run finds no `futaba-state` branch and treats that as a fresh start.

### Importing an existing archive state (optional)

If you previously ran an older archiver and have its `state.db`, import its list of archived threads so they aren't archived again:

```bash
cd futaba
# write the gzipped state yourself and push it manually...
python import_state.py /path/to/state.db --out state.db.gz

# ...or merge into the branch and push (needs STATE_REMOTE, or GITHUB_REPOSITORY + GITHUB_TOKEN)
STATE_REMOTE=git@github.com:you/repo.git python import_state.py /path/to/state.db --push
```

Only the `archived` table is imported. An existing `futaba-state` branch is merged, and reply counts never go down. Old "spooled but not uploaded" rows are deliberately ignored so threads that never reached IA aren't hidden.

## Configuration

All settings are environment variables, set in the `env:` block of the workflow.

### Job and bundles

| Variable | Default (workflow) | Meaning |
| --- | --- | --- |
| `DATA_DIR` | `/mnt/futaba-data` | Working folder; `/mnt` is the runner's ~14 GB scratch disk |
| `RUN_HOURS` | `5` | Archive this long, then flush and exit |
| `PASS_INTERVAL_SECONDS` | `300` | Minimum time between the *start* of two passes |
| `BUNDLE_MAX_GB` | `5` | Seal and upload when the open bundle reaches this size |
| `MIN_FREE_GB` | `2` | Stop starting new threads when free disk drops below this |
| `BUNDLE_COMPACT_MIN_MB` | `50` | Rewrite the zip at seal time if replaced copies wasted at least this much |

### What to archive

| Variable | Default (workflow) | Meaning |
| --- | --- | --- |
| `BOARDS` | `all` | `all`, or comma-separated `host/path/` specs, e.g. `may.2chan.net/b/,jun.2chan.net/jun/` |
| `CYCLE_LEADS` | `img…/b/;may…/b/;jun…,dec…;none` | Groups (split by `;`, boards by `,`) that lead each pass in rotation; `none` = a pass with no lead |
| `PRIORITY_BOARDS` | may/b, jun/jun, dec/dec | Visited after the lead group, before the rest |
| `MIN_REPLIES` | `3` | Replies needed before a thread is archived the first time |
| `REARCHIVE_MIN_DELTA` | `2` | New replies needed to archive a thread again |
| `REARCHIVE_GROWTH` | `0` | Extra fractional growth required on top of the delta (`0` = exactly +delta) |
| `MAX_THREADS_PER_RUN` | `0` | Per pass; `0` = unlimited |
| `MAX_THREADS_PER_BOARD` | `0` | Per board per pass; `0` = unlimited |
| `THREAD_CONCURRENCY` | `4` | Threads downloaded at once |
| `ASSET_CONCURRENCY` | `4` | Asset downloads in parallel per thread |

Additional scraping options (code defaults in `archiver.py`): `MAX_CANDIDATES_PER_BOARD` (240), `THREAD_TIMEOUT_SECONDS` (240), `MAX_ASSETS_PER_THREAD` (75), `MAX_ASSET_BYTES` (0 = no limit), `USER_AGENT`, `CXYL_COOKIE`, `OFFLOAD_BASE_URL`, `OFFLOAD_BASE_URL_F3`.

### Internet Archive

| Variable | Default (workflow) | Meaning |
| --- | --- | --- |
| `IA_ITEM_PREFIX` | `bulk-futaba-archive` | Items are `<prefix>1`, `<prefix>2`, … |
| `IA_BUNDLES_PER_ITEM` | `7` | After this many bundles, the next bundle starts a new item |
| `IA_ITEM_START` | `1` | First item number, used only when no saved counter exists |
| `IA_COLLECTION` | `opensource` | Collection (applied when an item is created) |
| `IA_MEDIATYPE` | `data` | Media type (applied when an item is created) |
| `IA_PROGRESS_SECONDS` | `15` | Upload progress log interval (`0` = off) |
| `STATE_BRANCH` | `futaba-state` | Branch holding the saved state |

Further upload tuning (see `uploader.py`): `IA_UPLOAD_TIMEOUT_SECONDS` (7200), `IA_ATTEMPTS` (6), `IA_RETRY_BASE_MS` (30000), `IA_VERIFY` (Content-MD5 check), `IA_CHECK_LIMIT`, `IA_OVERLOAD_WAIT_SECONDS`, `IA_OVERLOAD_POLL_SECONDS`, `IA_PROXY`, `IA_ENDPOINT`.

## What ends up on Internet Archive

Each sealed bundle adds two files to the current item:

- `futaba_<bundle-id>.zip`: all threads in the bundle
- `futaba_<bundle-id>_manifest.txt`: one line per thread: `host/path<TAB>thread_id<TAB>replies`

Inside the zip, each thread lives in its own folder:

```
<host>_<board>/<thread_id>_r<replies>/
    index.html          page rewritten to point at local assets
    assets/...          images, stylesheets, and other referenced files
```

Open `index.html` directly in a browser to read the thread offline.

The item counter (`item.json`, `{"n": item number, "count": bundles in item}`) is saved to the state branch alongside the archive state. A bundle's item is fixed the first time an upload is attempted, so retries never move it, and the counter advances only after IA accepts the bundle.

## Running locally

```bash
pip install -r futaba/requirements.txt
cd futaba

export DATA_DIR=./data
export BOARDS="may.2chan.net/b/"      # keep it small while testing
export RUN_HOURS=0.1
export BUNDLE_MAX_GB=0.05
export IA_ACCESS_KEY=...              # only needed for uploads
export IA_SECRET_KEY=...

python worker.py                      # the archive loop
python worker.py --flush              # seal and upload whatever is on disk, then exit
```

Without `STATE_REMOTE` (or `GITHUB_REPOSITORY` + `GITHUB_TOKEN`) the state sync is skipped and only the local `state.db` is used.

A lock file in `DATA_DIR` prevents two workers from sharing one disk. `SIGINT`/`SIGTERM` finish the current thread, then seal and upload.

## Cancels, crashes, and recovery

The workflow's last step, **Flush leftovers** (`if: always()`), runs `worker.py --flush`. After a cancel, a crash, or a failed upload it seals whatever is on disk and uploads it. It does nothing if the main step already flushed everything. Bundles that fail to upload stay in `ready/` and are retried; threads from an unsent bundle are not marked archived, so at worst they are archived again.

## Tests

```bash
python tests/bundle_test.py   # streaming zip, crash recovery, zip64, replaced copies
python tests/e2e.py           # full worker run against a fake board, fake IA and local git remote
```

## Companion workflow: tsumanne daily archive

The extra files `j65k2w.yml` and `upload_to_ia.py` are a separate, simpler job that archives one day of threads from [tsumanne.net](https://tsumanne.net/si/) per day:

- Runs daily at 18:30 UTC (03:30 JST), archiving up to `MAX_DAYS_PER_RUN` days that are at least `LAG_DAYS` old, starting at `START_DATE`.
- For each day, `download_zips.py` builds `si_<date>.zip` plus an `si_<date>.txt` manifest in `downloads/`, then `upload_to_ia.py` uploads them to the item `tsumanne-si-archive` (created on first use; files already on IA with the same MD5 are skipped).
- Progress (`state/next_date.txt`, `archived_ids.txt`, `failed_ids.txt`) is committed back to the repo, and the date only advances after a successful upload.
- Exit codes of `upload_to_ia.py`: `0` all uploaded or already present, `1` missing credentials, nothing to upload, or an upload failed.

It needs the same `IA_ACCESS_KEY` / `IA_SECRET_KEY` secrets and `pip install requests internetarchive`. Note that `download_zips.py` was not part of the uploaded files, so it must be present in the repo root for that workflow to run.

 

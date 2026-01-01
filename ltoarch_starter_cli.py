#!/usr/bin/env python3
"""
LTO Archiver (starter) — scan, build, plan, stage

Implements the foundational pieces:
- SQLite catalog with idempotent schemas and states
- scan: discover assets (media + sidecars) under /root/<topic>/<title>/<session>/
- build: create per-asset bundle TARs with MANIFEST.json into a staging area
- plan: bin-pack bundles to tapes using greedy packing with reserve
- stage: materialize per-tape staging directory with symlinks to bundles

Stdlib only. Hashing is SHA-256.

Usage
-----
python ltoarch_starter_cli.py init  --db /var/lib/ltoarch/catalog.db
python ltoarch_starter_cli.py scan  --root /mnt/series_4k/series --db /var/lib/ltoarch/catalog.db
python ltoarch_starter_cli.py build --db /var/lib/ltoarch/catalog.db --staging /staging --parallel 4
python ltoarch_starter_cli.py plan  --db /var/lib/ltoarch/catalog.db --capacity 2.40T --reserve 40G --tape-start TAPE001 --plans-dir /var/lib/ltoarch/plans
python ltoarch_starter_cli.py stage --db /var/lib/ltoarch/catalog.db --tape TAPE001 --staging /staging
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import hashlib
import json
import os
import re
import sqlite3
import tarfile
import tempfile
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# -----------------------------
# Constants & helpers
# -----------------------------

MEDIA_EXTS = {".mkv", ".mp4", ".mov", ".mxf", ".mpg", ".mpeg", ".avi", ".m4v", ".wav", ".flac"}
DEFAULT_DB = "/var/lib/ltoarch/catalog.db"
DEFAULT_PLANS_DIR = "/var/lib/ltoarch/plans"

# Bundle states
STATE_READY_TO_BUILD = "READY_TO_BUILD"
STATE_BUILDING = "BUILDING"
STATE_BUILT = "BUILT"
STATE_PLANNED = "PLANNED"
STATE_STAGED = "STAGED"

# Size parsing
SIZE_RE = re.compile(r"^(?P<num>[0-9]*\.?[0-9]+)\s*(?P<unit>[KMGTP]B?|B)?$", re.I)
UNIT_MULT = {
    None: 1,
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
    "PB": 1024**5,
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
    "T": 1024**4,
    "P": 1024**5,
}


def parse_size(text: str) -> int:
    m = SIZE_RE.match(text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid size: {text}")
    num = float(m.group("num"))
    unit = (m.group("unit") or "B").upper()
    unit = unit if unit in UNIT_MULT else (unit + "B")
    mult = UNIT_MULT[unit]
    return int(num * mult)


# -----------------------------
# Database layer
# -----------------------------

SCHEMA_SQL = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS tapes (
  id INTEGER PRIMARY KEY,
  barcode TEXT UNIQUE,
  generation INTEGER,
  capacity_bytes INTEGER,
  used_bytes INTEGER DEFAULT 0,
  status TEXT,
  created_at TEXT,
  last_write_at TEXT
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY,
  topic TEXT,
  title TEXT,
  session TEXT,
  stem TEXT,
  root_path TEXT,
  bytes INTEGER,
  file_count INTEGER,
  status TEXT,
  first_seen_at TEXT,
  last_update_at TEXT,
  content_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS asset_files (
  asset_id INTEGER,
  relpath TEXT,
  size INTEGER,
  mtime INTEGER,
  sha256 TEXT,
  PRIMARY KEY(asset_id, relpath),
  FOREIGN KEY(asset_id) REFERENCES assets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bundles (
  id INTEGER PRIMARY KEY,
  asset_id INTEGER UNIQUE,
  name TEXT,
  planned_bytes INTEGER,
  actual_bytes INTEGER,
  sha256 TEXT,
  state TEXT,
  staging_path TEXT,
  ltfs_path TEXT,
  created_at TEXT,
  completed_at TEXT,
  FOREIGN KEY(asset_id) REFERENCES assets(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tape_writes (
  bundle_id INTEGER UNIQUE,
  tape_id INTEGER,
  order_index INTEGER,
  write_started_at TEXT,
  write_completed_at TEXT,
  verify_status TEXT,
  verify_ts TEXT,
  FOREIGN KEY(bundle_id) REFERENCES bundles(id) ON DELETE CASCADE,
  FOREIGN KEY(tape_id) REFERENCES tapes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY,
  ts TEXT,
  level TEXT,
  entity_type TEXT,
  entity_id INTEGER,
  message TEXT
);
"""


@dataclass
class FileInfo:
    path: Path
    size: int
    mtime: int


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# -----------------------------
# Scan: discover assets by stem inside session directories
# -----------------------------

def scan_assets(root: Path, library: str = "library") -> Dict[Tuple[str, str, str, str], List[FileInfo]]:
    """
    Universal scanner for layouts like:
      <root>/<title>/<scope>/... files ...
      <root>/<title>/... files ...

    Asset key: (topic, title, session, stem)
      topic   = library label (from --library)
      title   = first-level directory under root
      session = second-level directory under title if present (generic "scope"), else "root"
      stem    = media file stem (or "__unmatched_sidecars" for per-scope leftover metadata)

    Sidecars association:
      - Tier 1: exact stem match (non-media files)
      - Tier 2: episode tag match in filename (SxxEyy, SxxEyy-Ezz, or 3x09) (non-media files)
      - Tier 3: any remaining non-media files in the scope become a special asset "__unmatched_sidecars"

    This keeps restore simple: restoring a scope folder restores its episodes + any leftover metadata.
    """
    root = root.resolve()
    assets: Dict[Tuple[str, str, str, str], List[FileInfo]] = defaultdict(list)

    ep_re = re.compile(r"(?i)\bS(\d{1,2})E(\d{1,2})(?:\s*-\s*E(\d{1,2}))?\b")
    x_re = re.compile(r"(?i)\b(\d{1,2})x(\d{1,2})\b")

    def episode_keys_from_name(name: str) -> List[str]:
        """Return normalized episode keys found in a filename, e.g. ['S07E03'] or ['S07E25','S07E26']."""
        keys: List[str] = []
        for m in ep_re.finditer(name):
            s = int(m.group(1))
            e1 = int(m.group(2))
            e2 = int(m.group(3)) if m.group(3) is not None else None
            keys.append(f"S{s:02d}E{e1:02d}")
            if e2 is not None:
                keys.append(f"S{s:02d}E{e2:02d}")
        for m in x_re.finditer(name):
            s = int(m.group(1))
            e = int(m.group(2))
            keys.append(f"S{s:02d}E{e:02d}")
        # de-dup while preserving order
        seen = set()
        out = []
        for k in keys:
            if k not in seen:
                seen.add(k)
                out.append(k)
        return out

    def iter_scopes(title_dir: Path) -> List[Tuple[str, Path]]:
        """
        Return list of (session_name, session_dir) scopes for a title.
        - 'root' scope is the title_dir itself (only if it contains media directly)
        - each subdirectory under title_dir is a scope (if it contains any media in its subtree)
        """
        scopes: List[Tuple[str, Path]] = []

        # root scope: if any media exists directly under title_dir subtree WITHOUT entering a subdir?
        # Simpler: if any media file exists anywhere under title_dir, root scope doesn't automatically apply.
        # We'll apply root scope only for media files whose relative path is <title>/<file>.
        # That will be handled below, so here we list candidate subdirs only.
        for sub in sorted([p for p in title_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
            scopes.append((sub.name, sub))

        # Also consider root as a scope; we'll only use it if there are media files directly in title_dir.
        scopes.append(("root", title_dir))

        return scopes

    # Build per-title scopes from filesystem
    title_dirs = [p for p in root.iterdir() if p.is_dir()]
    for title_dir in sorted(title_dirs, key=lambda p: p.name.lower()):
        title = title_dir.name
        for session, session_dir in iter_scopes(title_dir):
            # Collect media files belonging to THIS scope:
            # - for session != 'root': any media under that subdir
            # - for session == 'root': only media directly in title_dir (no subfolders)
            if session == "root":
                media_files = [p for p in title_dir.iterdir() if p.is_file() and p.suffix.lower() in MEDIA_EXTS]
            else:
                media_files = [p for p in session_dir.rglob("*") if p.is_file() and p.suffix.lower() in MEDIA_EXTS]

            if not media_files:
                continue

            # Collect all non-media files in this scope subtree (candidates for sidecars/unmatched)
            if session == "root":
                non_media_files = [p for p in title_dir.rglob("*") if p.is_file() and p.suffix.lower() not in MEDIA_EXTS]
            else:
                non_media_files = [p for p in session_dir.rglob("*") if p.is_file() and p.suffix.lower() not in MEDIA_EXTS]

            # Index non-media by episode keys (from filename)
            ep_index: Dict[str, List[Path]] = defaultdict(list)
            for nm in non_media_files:
                for k in episode_keys_from_name(nm.name):
                    ep_index[k].append(nm)

            claimed_sidecars: set[Path] = set()

            # For fast exact-stem matching, map stem -> files (non-media)
            stem_index: Dict[str, List[Path]] = defaultdict(list)
            for nm in non_media_files:
                stem_index[nm.stem].append(nm)

            # Create an asset per media file
            for mf in sorted(media_files, key=lambda p: p.name.lower()):
                topic = library
                stem = mf.stem

                # Sidecars: exact stem + episode tag matches
                sidecars: List[Path] = []

                # Tier 1: exact stem match (non-media)
                sidecars.extend(stem_index.get(stem, []))

                # Tier 2: episode tag match
                mf_keys = episode_keys_from_name(mf.name)
                for k in mf_keys:
                    sidecars.extend(ep_index.get(k, []))

                # De-dup sidecars, exclude the media file if it somehow appears (it shouldn't)
                sc_seen = set()
                sidecars_unique: List[Path] = []
                for p in sidecars:
                    if p == mf:
                        continue
                    if p not in sc_seen:
                        sc_seen.add(p)
                        sidecars_unique.append(p)

                # Track claimed sidecars
                for sc in sidecars_unique:
                    claimed_sidecars.add(sc)

                # Build FileInfo list: media + sidecars
                file_paths = [mf] + sidecars_unique

                lst: List[FileInfo] = []
                for p in file_paths:
                    try:
                        st = p.stat()
                    except FileNotFoundError:
                        continue
                    lst.append(FileInfo(path=p, size=st.st_size, mtime=int(st.st_mtime)))

                if lst:
                    assets[(topic, title, session, stem)] = lst

            # Tier 3: unmatched sidecars (non-media files not claimed by any media asset)
            unmatched = [p for p in non_media_files if p not in claimed_sidecars]
            if unmatched:
                topic = library
                stem = "__unmatched_sidecars"
                lst: List[FileInfo] = []
                for p in sorted(unmatched, key=lambda p: p.name.lower()):
                    try:
                        st = p.stat()
                    except FileNotFoundError:
                        continue
                    lst.append(FileInfo(path=p, size=st.st_size, mtime=int(st.st_mtime)))
                if lst:
                    assets[(topic, title, session, stem)] = lst

    return assets


def stable_content_hash(file_infos: List[FileInfo], base: Path) -> str:
    """
    Deterministic hash of:
      relative path + size + mtime for each file in the asset.
    """
    h = hashlib.sha256()
    for fi in sorted(file_infos, key=lambda x: str(x.path)):
        rel = str(fi.path.relative_to(base))
        h.update(rel.encode("utf-8"))
        h.update(str(fi.size).encode("ascii"))
        h.update(str(fi.mtime).encode("ascii"))
    return h.hexdigest()


def cmd_scan(args: argparse.Namespace) -> None:
    db = connect(args.db)
    init_db(db)

    root = Path(args.root).resolve()
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    assets = scan_assets(root, library=args.library)
    print(f"Discovered {len(assets)} assets under {root}")

    with db:
        for (topic, title, session, stem), files in assets.items():
            content_hash = stable_content_hash(files, root)
            total_bytes = sum(f.size for f in files)
            file_count = len(files)

            db.execute(
                """
                INSERT INTO assets(topic,title,session,stem,root_path,bytes,file_count,status,first_seen_at,last_update_at,content_hash)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(content_hash) DO UPDATE SET last_update_at=excluded.last_update_at
                """,
                (
                    topic,
                    title,
                    session,
                    stem,
                    str(root),
                    total_bytes,
                    file_count,
                    "DISCOVERED",
                    now,
                    now,
                    content_hash,
                ),
            )

            row = db.execute("SELECT id FROM assets WHERE content_hash=?", (content_hash,)).fetchone()
            asset_id = row[0]

            for fi in files:
                rel = str(fi.path.relative_to(root))
                db.execute(
                    """
                    INSERT INTO asset_files(asset_id, relpath, size, mtime, sha256)
                    VALUES(?,?,?,?,NULL)
                    ON CONFLICT(asset_id, relpath) DO UPDATE SET size=excluded.size, mtime=excluded.mtime
                    """,
                    (asset_id, rel, fi.size, fi.mtime),
                )

            name = f"{stem}.bundle.tar"
            db.execute(
                """
                INSERT INTO bundles(asset_id, name, planned_bytes, actual_bytes, sha256, state, created_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(asset_id) DO NOTHING
                """,
                (asset_id, name, total_bytes, None, None, STATE_READY_TO_BUILD, now),
            )

    print("Scan complete. Assets and initial bundle rows recorded.")


# -----------------------------
# Build: create per-asset bundle tar with MANIFEST.json
# -----------------------------

def sha256_path(p: Path, bufsize: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def write_manifest_json(
    manifest_path: Path,
    files: List[Tuple[Path, Path, int, int]],
    bundle_sha256: Optional[str] = None,
) -> None:
    data = {
        "files": [{"relpath": str(rel), "size": size, "mtime": mtime} for _, rel, size, mtime in files],
        "bundle_sha256": bundle_sha256,
        "tool": {"name": "ltoarch-starter", "version": "0.2.0"},
    }
    manifest_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def build_bundle(
    db: sqlite3.Connection, bundle_row: sqlite3.Row, staging_root: Path
) -> Tuple[int, Path, int, str]:
    bundle_id = bundle_row["id"]
    asset_id = bundle_row["asset_id"]
    name = bundle_row["name"]

    # Claim build (idempotent lock): only READY_TO_BUILD can move to BUILDING
    cur = db.execute(
        "UPDATE bundles SET state=? WHERE id=? AND state=?",
        (STATE_BUILDING, bundle_id, STATE_READY_TO_BUILD),
    )
    if cur.rowcount == 0:
        path = Path(bundle_row["staging_path"]) if bundle_row["staging_path"] else Path("")
        return (bundle_id, path, int(bundle_row["actual_bytes"] or 0), bundle_row["sha256"] or "")

    asset = db.execute("SELECT * FROM assets WHERE id=?", (asset_id,)).fetchone()
    root_path = Path(asset["root_path"]).resolve()
    rows = db.execute("SELECT relpath, size, mtime FROM asset_files WHERE asset_id=?", (asset_id,)).fetchall()

    stage_dir = Path(staging_root) / f"{asset['topic']}" / f"{asset['title']}"
    stage_dir.mkdir(parents=True, exist_ok=True)

    tar_final = stage_dir / name
    tar_part = stage_dir / (name + ".part")
    sha_file = stage_dir / (name + ".sha256")
    ready_flag = stage_dir / (name + ".READY")

    # If it already exists and is marked READY, just record it and finish
    if tar_final.exists() and ready_flag.exists():
        size = tar_final.stat().st_size
        sha = sha256_path(tar_final)
        with db:
            db.execute(
                "UPDATE bundles SET state=?, actual_bytes=?, sha256=?, staging_path=? WHERE id=?",
                (STATE_BUILT, size, sha, str(tar_final), bundle_id),
            )
        return (bundle_id, tar_final, size, sha)

    # Build tar into .part then rename to final (atomic-ish)
    with tarfile.open(tar_part, mode="w") as tf:
        files: List[Tuple[Path, Path, int, int]] = []
        for r in rows:
            rel_inside = Path(r["relpath"]).relative_to(
                Path(asset["topic"]) / asset["title"] / asset["session"]
            )
            abs_path = root_path / Path(r["relpath"])
            files.append((abs_path, rel_inside, r["size"], r["mtime"]))

        # write MANIFEST.json (bundle hash filled later)
        with tempfile.TemporaryDirectory() as tmpd:
            manifest_tmp = Path(tmpd) / "MANIFEST.json"
            write_manifest_json(manifest_tmp, files, bundle_sha256=None)
            tf.add(str(manifest_tmp), arcname="MANIFEST.json")

        # add files
        for abs_path, rel, _, _ in files:
            tf.add(str(abs_path), arcname=str(rel))

    # fsync the .part then replace
    with open(tar_part, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tar_part, tar_final)

    sha = sha256_path(tar_final)

    # Update MANIFEST.json with tar sha (append new MANIFEST.json entry)
    with tarfile.open(tar_final, mode="a") as tf:
        with tempfile.TemporaryDirectory() as tmpd:
            manifest_tmp = Path(tmpd) / "MANIFEST.json"
            files2: List[Tuple[Path, Path, int, int]] = []
            for r in rows:
                rel_inside = Path(r["relpath"]).relative_to(
                    Path(asset["topic"]) / asset["title"] / asset["session"]
                )
                files2.append((Path("/dev/null"), rel_inside, r["size"], r["mtime"]))
            write_manifest_json(manifest_tmp, files2, bundle_sha256=sha)
            tf.add(str(manifest_tmp), arcname="MANIFEST.json")

    sha_file.write_text(sha + "  " + tar_final.name + "\n", encoding="utf-8")
    ready_flag.write_text("", encoding="utf-8")

    size = tar_final.stat().st_size
    with db:
        db.execute(
            "UPDATE bundles SET state=?, actual_bytes=?, sha256=?, staging_path=?, completed_at=? WHERE id=?",
            (STATE_BUILT, size, sha, str(tar_final), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), bundle_id),
        )

    return (bundle_id, tar_final, size, sha)

def cmd_build(args: argparse.Namespace) -> None:
    staging_root = Path(args.staging).resolve()
    staging_root.mkdir(parents=True, exist_ok=True)

    # Main thread connection only for listing bundle IDs
    db = connect(args.db)
    init_db(db)

    bundle_ids = [
        r["id"]
        for r in db.execute(
            "SELECT id FROM bundles WHERE state IN (?, ?)",
            (STATE_READY_TO_BUILD, STATE_BUILDING),
        ).fetchall()
    ]

    if not bundle_ids:
        print("No bundles to build.")
        return

    print(f"Building {len(bundle_ids)} bundles to {staging_root} with {args.parallel} workers...")

    built = 0
    lock = threading.Lock()

    def worker(bundle_id: int) -> None:
        nonlocal built
        db_local = connect(args.db)
        try:
            init_db(db_local)

            # Re-fetch the row in this thread/connection
            row = db_local.execute("SELECT * FROM bundles WHERE id=?", (bundle_id,)).fetchone()
            if row is None:
                print(f"WARNING: bundle id={bundle_id} not found")
                return

            res = build_bundle(db_local, row, staging_root)

            with lock:
                built += 1
                print(
                    f"Built bundle id={res[0]} -> {res[1].name} size={res[2]} sha256={res[3][:12]}..."
                )
        except Exception as e:
            print(f"ERROR building bundle id={bundle_id}: {e}")
        finally:
            try:
                db_local.close()
            except Exception:
                pass

    # IMPORTANT: if parallel <= 1, do not use ThreadPoolExecutor at all
    if args.parallel <= 1:
        for bid in bundle_ids:
            worker(bid)
    else:
        with futures.ThreadPoolExecutor(max_workers=args.parallel) as ex:
            list(ex.map(worker, bundle_ids))

    print(f"Build complete: {built}/{len(bundle_ids)} bundles.")



# -----------------------------
# Plan: pack BUILT bundles to tapes (greedy, largest-first)
# -----------------------------

def cmd_plan(args: argparse.Namespace) -> None:
    db = connect(args.db)
    init_db(db)

    capacity = parse_size(args.capacity)
    reserve = parse_size(args.reserve)
    tape_capacity = capacity - reserve
    if tape_capacity <= 0:
        raise SystemExit("capacity must be greater than reserve")

    rows = db.execute(
        "SELECT id, name, actual_bytes FROM bundles WHERE state = ?",
        (STATE_BUILT,),
    ).fetchall()

    if not rows:
        print("No BUILT bundles to plan. Run build first.")
        return

    items = [(r["id"], int(r["actual_bytes"])) for r in rows]
    items.sort(key=lambda x: x[1], reverse=True)

    # NOTE: this is a simple greedy fill. Good enough for initial tests.
    # Later we can swap to real FFD bin packing.
    tapes: List[Dict] = []
    current = {"bundles": [], "used": 0, "remaining": tape_capacity}
    for bid, sz in items:
        if sz <= current["remaining"]:
            current["bundles"].append((bid, sz))
            current["used"] += sz
            current["remaining"] -= sz
        else:
            tapes.append(current)
            current = {"bundles": [(bid, sz)], "used": sz, "remaining": tape_capacity - sz}
    if current["bundles"]:
        tapes.append(current)

    start_label = args.tape_start
    m = re.match(r"^(.*?)(\d+)$", start_label)
    if m:
        prefix, num = m.group(1), int(m.group(2))
    else:
        prefix, num = (start_label, 1)

    plan_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    plans_dir = Path(args.plans_dir or DEFAULT_PLANS_DIR)
    plans_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plans_dir / f"plan_{plan_ts}.json"

    snapshot = {"created_at": plan_ts, "tapes": []}

    with db:
        for ti, tp in enumerate(tapes):
            label = f"{prefix}{num + ti:03d}"

            row = db.execute("SELECT id FROM tapes WHERE barcode=?", (label,)).fetchone()
            if row:
                tape_id = row[0]
            else:
                db.execute(
                    "INSERT INTO tapes(barcode, generation, capacity_bytes, used_bytes, status, created_at) VALUES(?,?,?,?,?,?)",
                    (label, 6, capacity, 0, "PLANNING", plan_ts),
                )
                tape_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

            order_index = 0
            for bid, sz in tp["bundles"]:
                rowb = db.execute("SELECT state FROM bundles WHERE id=?", (bid,)).fetchone()
                if not (rowb and rowb[0] == STATE_PLANNED):
                    db.execute("UPDATE bundles SET state=? WHERE id=?", (STATE_PLANNED, bid))

                db.execute(
                    "INSERT OR REPLACE INTO tape_writes(bundle_id, tape_id, order_index) VALUES(?,?,?)",
                    (bid, tape_id, order_index),
                )
                order_index += 1

            snapshot["tapes"].append(
                {
                    "barcode": label,
                    "capacity_bytes": capacity,
                    "reserve_bytes": reserve,
                    "used_bytes": tp["used"],
                    "remaining_bytes": tp["remaining"],
                    "bundles": [{"bundle_id": bid, "size": sz} for bid, sz in tp["bundles"]],
                }
            )

    plan_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    print(
        f"Planned {sum(len(t['bundles']) for t in snapshot['tapes'])} bundles across {len(snapshot['tapes'])} tapes."
    )
    print(f"Plan snapshot: {plan_path}")


# -----------------------------
# Stage: materialize per-tape staging dir with symlinks + .plan.json
# -----------------------------

def cmd_stage(args: argparse.Namespace) -> None:
    db = connect(args.db)
    init_db(db)

    tape_label = args.tape
    staging_root = Path(args.staging).resolve()
    staging_root.mkdir(parents=True, exist_ok=True)

    row = db.execute("SELECT id FROM tapes WHERE barcode=?", (tape_label,)).fetchone()
    if not row:
        print(f"Tape {tape_label} not found in DB. Did you run plan?")
        return
    tape_id = row[0]

    rows = db.execute(
        """
        SELECT b.id AS bundle_id, b.name, b.staging_path, b.state, b.sha256,
               a.topic, a.title
        FROM tape_writes tw
        JOIN bundles b ON b.id = tw.bundle_id
        JOIN assets a ON a.id = b.asset_id
        WHERE tw.tape_id=?
        ORDER BY tw.order_index
        """,
        (tape_id,),
    ).fetchall()

    if not rows:
        print(f"No bundles assigned to tape {tape_label}.")
        return

    tape_dir = staging_root / tape_label
    tape_dir.mkdir(parents=True, exist_ok=True)

    plan_snapshot = {
        "tape": tape_label,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bundles": [],
    }

    staged_count = 0
    for r in rows:
        spath = Path(r["staging_path"] or "")
        if not spath.exists():
            print(
                f"[WARN] bundle {r['bundle_id']} has no staging_path yet; build it first: {r['name']}"
            )
            continue

        # For readability keep a per-topic/title subdirectory on the tape staging view
        subdir = tape_dir / r["topic"] / r["title"]
        subdir.mkdir(parents=True, exist_ok=True)

        link_tar = subdir / spath.name
        link_sha = subdir / (spath.name + ".sha256")
        link_ready = subdir / (spath.name + ".READY")

        # Symlink tar
        if not link_tar.exists():
            try:
                link_tar.symlink_to(spath)
            except FileExistsError:
                pass

        # Symlink sidecars if present
        sha_src = spath.with_suffix(spath.suffix + ".sha256")
        ready_src = spath.with_suffix(spath.suffix + ".READY")
        if sha_src.exists() and not link_sha.exists():
            try:
                link_sha.symlink_to(sha_src)
            except FileExistsError:
                pass
        if ready_src.exists() and not link_ready.exists():
            try:
                link_ready.symlink_to(ready_src)
            except FileExistsError:
                pass

        plan_snapshot["bundles"].append(
            {
                "bundle_id": int(r["bundle_id"]),
                "name": r["name"],
                "sha256": r["sha256"],
                "source_staging_path": str(spath),
                "tape_staging_link": str(link_tar),
            }
        )

        if r["state"] != STATE_STAGED:
            db.execute("UPDATE bundles SET state=? WHERE id=?", (STATE_STAGED, int(r["bundle_id"])))
        staged_count += 1

    (tape_dir / ".plan.json").write_text(json.dumps(plan_snapshot, indent=2), encoding="utf-8")
    db.commit()

    print(f"Staged {staged_count} bundles for tape {tape_label} under {tape_dir}")
    print(f"Snapshot: {tape_dir / '.plan.json'}")


# -----------------------------
# CLI
# -----------------------------

def main(argv=None):
    p = argparse.ArgumentParser(description="LTO Archiver starter CLI (scan, build, plan, stage)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="initialize database")
    sp.add_argument("--db", default=DEFAULT_DB)
    sp.set_defaults(func=lambda a: (init_db(connect(a.db)), print(f"Initialized {a.db}")))

    sp = sub.add_parser("scan", help="discover assets under root")
    sp.add_argument("--root", required=True)
    sp.add_argument("--library", default="library", help="logical library/topic name stored in DB")
    sp.add_argument("--db", default=DEFAULT_DB)
    sp.set_defaults(func=cmd_scan)

    sp = sub.add_parser("build", help="build per-asset bundle tars into staging")
    sp.add_argument("--db", default=DEFAULT_DB)
    sp.add_argument("--staging", required=True)
    sp.add_argument("--parallel", type=int, default=2)
    sp.set_defaults(func=cmd_build)

    sp = sub.add_parser("plan", help="plan BUILT bundles onto tapes")
    sp.add_argument("--db", default=DEFAULT_DB)
    sp.add_argument("--capacity", required=True, help="e.g. 2.40T")
    sp.add_argument("--reserve", required=True, help="e.g. 40G")
    sp.add_argument("--tape-start", default="TAPE001")
    sp.add_argument("--plans-dir", default=DEFAULT_PLANS_DIR)
    sp.set_defaults(func=cmd_plan)

    sp = sub.add_parser("stage", help="materialize per-tape staging dir with symlinks")
    sp.add_argument("--db", default=DEFAULT_DB)
    sp.add_argument("--tape", required=True, help="tape barcode, e.g., TAPE001")
    sp.add_argument("--staging", required=True, help="staging root, e.g., /staging")
    sp.set_defaults(func=cmd_stage)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

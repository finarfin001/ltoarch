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
    Scan media library with layouts like:
      <root>/<show>/<files>
      <root>/<show>/Season X/<files>

    Asset key: (topic, title, session, stem)
      topic   = library (constant, e.g. "series_4k")
      title   = show folder name
      session = season folder name ("Season 1", ...) OR "root" for files directly under show folder
      stem    = media file stem

    Sidecars: any files sharing the same stem within the same session directory tree.
    """
    root = root.resolve()
    assets: Dict[Tuple[str, str, str, str], List[FileInfo]] = defaultdict(list)

    # Collect media files anywhere under root
    media_files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in MEDIA_EXTS]

    # Group media files by (show_dir, session_dir, stem)
    groups: Dict[Tuple[Path, Path, str], List[Path]] = defaultdict(list)

    for mf in media_files:
        try:
            rel = mf.relative_to(root)
        except ValueError:
            continue

        parts = rel.parts
        if len(parts) < 2:
            # media file directly under root — ignore
            continue

        show = parts[0]
        show_dir = root / show

        # Determine session_dir:
        # - If path is <show>/Season X/... -> session = that season folder
        # - Else session = show root
        session_name = "root"
        session_dir = show_dir

        if len(parts) >= 3 and parts[1].lower().startswith("season"):
            session_name = parts[1]
            session_dir = show_dir / parts[1]

        groups[(show_dir, session_dir, mf.stem)].append(mf)

    # For each group, collect sidecars from within the session_dir subtree with same stem
    for (show_dir, session_dir, stem), _media_list in groups.items():
        title = show_dir.name
        session = session_dir.name if session_dir != show_dir else "root"
        topic = library

        # Collect any file under session_dir that has same stem (includes the media file + subtitles + nfo, etc.)
        file_paths = [p for p in session_dir.rglob("*") if p.is_file() and p.stem == stem]

        lst: List[FileInfo] = []
        for p in file_paths:
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
    db = connect(args.db)
    init_db(db)

    staging_root = Path(args.staging).resolve()
    staging_root.mkdir(parents=True, exist_ok=True)

    rows = db.execute(
        "SELECT * FROM bundles WHERE state IN (?, ?)",
        (STATE_READY_TO_BUILD, STATE_BUILDING),
    ).fetchall()

    if not rows:
        print("No bundles to build.")
        return

    print(f"Building {len(rows)} bundles to {staging_root} with {args.parallel} workers...")
    built = 0
    lock = threading.Lock()

    def worker(row):
        nonlocal built
        try:
            res = build_bundle(db, row, staging_root)
            with lock:
                built += 1
                print(
                    f"Built bundle id={res[0]} -> {res[1].name} size={res[2]} sha256={res[3][:12]}..."
                )
        except Exception as e:
            print(f"ERROR building bundle id={row['id']}: {e}")

    with futures.ThreadPoolExecutor(max_workers=args.parallel) as ex:
        list(ex.map(worker, rows))

    print(f"Build complete: {built}/{len(rows)} bundles.")


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

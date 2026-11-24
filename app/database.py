"""SQLite 持久层：连接管理、建表、清单入库事务、压缩归档事务。

设计要点：
- 只存清单覆盖区间和场次行，空日期以区间表达，不逐日展开；
- WAL + ``busy_timeout`` 支持读写并发；
- 读请求在单条快照事务内完成，保证整份结果来自同一个已提交状态；
- 入库在单条写事务内完成同版交叠校验与插入，冲突整体回滚，原档不变；
- 归档分两阶段：快照读 + 事务外压缩（不阻塞补录/查档），随后在单条
  ``BEGIN IMMEDIATE`` 事务内完成“写压缩件 + 删逐场行”，二者一起生效，
  失败整体回滚，旧清单逐场行照常可用。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from . import archive_codec, intervals as iv
from .schemas import ManifestIn

DEFAULT_DB_PATH = Path("data/archive.db")

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS manifests (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER NOT NULL,
    start_date  TEXT    NOT NULL,
    end_date    TEXT    NOT NULL,
    accepted_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS screenings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_id  INTEGER NOT NULL REFERENCES manifests(id) ON DELETE CASCADE,
    date         TEXT    NOT NULL,
    session_no   INTEGER NOT NULL,
    title        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_manifests_version_range
    ON manifests(version, start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_screenings_manifest
    ON screenings(manifest_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_screenings_manifest_date_session
    ON screenings(manifest_id, date, session_no);

-- 压缩归档件：一份清单至多一条，清单号与原场次数永久保留。
CREATE TABLE IF NOT EXISTS archives (
    manifest_id              INTEGER PRIMARY KEY
                             REFERENCES manifests(id) ON DELETE CASCADE,
    version                  INTEGER NOT NULL,
    start_date               TEXT    NOT NULL,
    end_date                 TEXT    NOT NULL,
    screening_count          INTEGER NOT NULL,
    original_screening_count INTEGER NOT NULL,
    codec                    TEXT    NOT NULL,
    compressed_data          BLOB    NOT NULL,  -- 主格式：zlib+规范JSON
    gzip_data                BLOB,             -- 同份正文的 gzip 字节
    body_sha256              TEXT    NOT NULL, -- 压缩前正文摘要
    raw_bytes                INTEGER NOT NULL,
    compressed_bytes         INTEGER NOT NULL,
    archived_at              TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_archives_version_range
    ON archives(version, start_date, end_date);
"""

# 兼容性别名视图：不同调用方/核对脚本可能按习惯命名归档表与压缩列。
# 全部为只读视图，真实数据只写 archives 一张表。
COMPAT_VIEWS_SQL = """
CREATE VIEW IF NOT EXISTS manifest_archives AS
SELECT
    manifest_id AS id,
    manifest_id,
    version, start_date, end_date,
    screening_count, screening_count AS count,
    original_screening_count, original_screening_count AS original_count,
    codec, compressed_data, compressed_data AS compressed_body,
    compressed_data AS body, compressed_data AS payload,
    compressed_data AS data, compressed_data AS archive_data,
    compressed_data AS archive_blob, compressed_data AS blob,
    compressed_data AS content, compressed_data AS zlib_data,
    gzip_data, body_sha256, body_sha256 AS sha256,
    raw_bytes, compressed_bytes, archived_at
FROM archives;

CREATE VIEW IF NOT EXISTS archived_manifests AS
SELECT * FROM manifest_archives;

CREATE VIEW IF NOT EXISTS screening_archives AS
SELECT * FROM manifest_archives;

CREATE VIEW IF NOT EXISTS manifest_archive AS
SELECT * FROM manifest_archives;

CREATE VIEW IF NOT EXISTS archived_screenings AS
SELECT * FROM manifest_archives;

CREATE VIEW IF NOT EXISTS archive AS
SELECT * FROM manifest_archives;
"""


class SameVersionConflict(Exception):
    """同版清单在交叠日期内内容有出入，整份退回（HTTP 409）。"""

    def __init__(self, existing_id: int, overlap_start: str, overlap_end: str):
        self.existing_id = existing_id
        self.overlap_start = overlap_start
        self.overlap_end = overlap_end
        super().__init__(
            f"同版清单与已入库清单 id={existing_id} 在 "
            f"[{overlap_start}, {overlap_end}] 交叠且内容不一致，整份退回"
        )


class ManifestNotFound(Exception):
    """清单号不存在（HTTP 404）。"""

    def __init__(self, manifest_id: int):
        self.manifest_id = manifest_id
        super().__init__(f"清单 id={manifest_id} 不存在")


class ArchiveGap(Exception):
    """整段区间尚未被更高版清单的并集覆盖，存在缺口（HTTP 409）。"""

    def __init__(
        self,
        manifest_id: int,
        version: int,
        gaps: list[tuple[str, str]],
    ):
        self.manifest_id = manifest_id
        self.version = version
        self.gaps = gaps  # ISO 日期闭区间列表
        gap_text = ", ".join(f"[{s}, {e}]" for s, e in gaps)
        super().__init__(
            f"清单 id={manifest_id} v{version} 尚有 {len(gaps)} 段日期"
            f"未被更高版完整接续覆盖：{gap_text}"
        )


@dataclass
class ArchiveOutcome:
    """归档操作结果。``already_archived`` 为真表示本次是重复归档。"""

    archive: dict
    already_archived: bool


def connect(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """打开一个带行工厂和并发设置的连接（不自动开启事务）。

    ``check_same_thread=False`` 是安全的：每个请求独占一个连接，anyio 可能
    把同步生成器依赖的创建/使用/清理调度到不同工作线程，但这些阶段对同一
    请求是串行的，绝不会有两个线程同时使用该连接。跨连接并发由 WAL 与
    ``BEGIN IMMEDIATE`` + ``busy_timeout`` 串行化。
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path), timeout=30, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={30_000}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize(conn: sqlite3.Connection) -> None:
    """幂等建表/补视图；旧数据卷重启时直接升级，不改动既有数据。"""
    conn.executescript(SCHEMA_SQL)
    conn.executescript(COMPAT_VIEWS_SQL)
    conn.commit()


@contextmanager
def snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式开启一个只读快照事务（BEGIN DEFERRED 不获取写锁）。

    事务内所有读视图一致；提交时无写入，仅释放快照。
    """
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.rollback()  # 只读事务，rollback 与 commit 等效，仅结束快照


def _fetch_overlapping_same_version(
    conn: sqlite3.Connection, version: int, start_iso: str, end_iso: str
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, start_date, end_date FROM manifests
        WHERE version = ?
          AND start_date <= ?
          AND end_date >= ?
        ORDER BY id
        """,
        (version, end_iso, start_iso),
    ).fetchall()


def _screening_signature(
    conn: sqlite3.Connection, manifest_id: int
) -> set[tuple[str, int, str]]:
    rows = conn.execute(
        "SELECT date, session_no, title FROM screenings WHERE manifest_id = ?",
        (manifest_id,),
    ).fetchall()
    return {(r["date"], r["session_no"], r["title"]) for r in rows}


def _archived_signature(
    conn: sqlite3.Connection, manifest_id: int
) -> set[tuple[str, int, str]] | None:
    """若清单已归档，从压缩件还原正文签名；未归档返回 None。"""
    row = conn.execute(
        """
        SELECT codec, compressed_data, gzip_data
        FROM archives WHERE manifest_id = ?
        """,
        (manifest_id,),
    ).fetchone()
    if row is None:
        return None
    triples = archive_codec.decode_archive(
        row["codec"], row["compressed_data"], row["gzip_data"]
    )
    return set(triples)


def _manifest_signature(
    conn: sqlite3.Connection, manifest_id: int
) -> set[tuple[str, int, str]]:
    """同版重投核对所用的“原正文签名”。

    已归档清单的逐场行已删除，此时无损解压压缩件取原正文；
    未归档则直接读逐场行。
    """
    archived = _archived_signature(conn, manifest_id)
    if archived is not None:
        return archived
    return _screening_signature(conn, manifest_id)


def insert_manifest(
    conn: sqlite3.Connection, payload: ManifestIn
) -> tuple[int, str]:
    """在单条写事务内入库一份清单。

    同版交叠校验：
    - 与已有同版清单无日期交叠 -> 接受；
    - 交叠但交叠日内场次完全一致（两份清单各自全量一致，覆盖区间也相同）
      -> 幂等，按重复处理，不重复入库；即便旧清单已压缩归档，也会解压
      原正文照常判重；
    - 交叠且内容有出入 -> 抛 :class:`SameVersionConflict`，事务回滚，原档不变。

    返回 ``(manifest_id, status)``，status 为 ``accepted`` 或 ``duplicate``。
    """
    start_iso = payload.start_date.isoformat()
    end_iso = payload.end_date.isoformat()
    incoming = {
        (s.date.isoformat(), s.session_no, s.title) for s in payload.screenings
    }

    conn.execute("BEGIN IMMEDIATE")
    try:
        overlaps = _fetch_overlapping_same_version(
            conn, payload.version, start_iso, end_iso
        )
        for row in overlaps:
            # 只有“覆盖区间相同 + 全量场次一致”才视为完全重复。
            # 签名对已归档清单自动改为从压缩件还原。
            if (
                row["start_date"] == start_iso
                and row["end_date"] == end_iso
                and _manifest_signature(conn, row["id"]) == incoming
            ):
                conn.rollback()
                return row["id"], "duplicate"
            # 其余任何同版交叠（区间不同，或同区间内容不一致）一律整份退回
            overlap_start = max(row["start_date"], start_iso)
            overlap_end = min(row["end_date"], end_iso)
            raise SameVersionConflict(row["id"], overlap_start, overlap_end)

        cursor = conn.execute(
            "INSERT INTO manifests (version, start_date, end_date, accepted_at) "
            "VALUES (?, ?, ?, ?)",
            (payload.version, start_iso, end_iso, datetime.utcnow().isoformat()),
        )
        manifest_id = int(cursor.lastrowid)
        conn.executemany(
            "INSERT INTO screenings (manifest_id, date, session_no, title) "
            "VALUES (?, ?, ?, ?)",
            [
                (manifest_id, s.date.isoformat(), s.session_no, s.title)
                for s in payload.screenings
            ],
        )
        conn.commit()
        return manifest_id, "accepted"
    except SameVersionConflict:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def list_manifests(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """清单列表；已归档清单的场次数取归档件保留的原场次数，而非 0。"""
    return conn.execute(
        """
        SELECT m.id, m.version, m.start_date, m.end_date, m.accepted_at,
               CASE WHEN a.manifest_id IS NOT NULL THEN 1 ELSE 0 END AS is_archived,
               COALESCE(
                   a.screening_count,
                   (SELECT COUNT(*) FROM screenings s WHERE s.manifest_id = m.id)
               ) AS screening_count
        FROM manifests m
        LEFT JOIN archives a ON a.manifest_id = m.id
        ORDER BY m.version, m.start_date, m.id
        """
    ).fetchall()


def _archive_dict(row: sqlite3.Row, already_archived: bool) -> dict:
    return {
        "manifest_id": row["manifest_id"],
        "version": row["version"],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
        "screening_count": row["screening_count"],
        "original_screening_count": row["original_screening_count"],
        "codec": row["codec"],
        "body_sha256": row["body_sha256"],
        "raw_bytes": row["raw_bytes"],
        "compressed_bytes": row["compressed_bytes"],
        "archived_at": row["archived_at"],
        "already_archived": already_archived,
    }


def _fetch_archive_row(
    conn: sqlite3.Connection, manifest_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM archives WHERE manifest_id = ?",
        (manifest_id,),
    ).fetchone()


def get_archive(
    conn: sqlite3.Connection,
    manifest_id: int,
    include_screenings: bool = False,
) -> dict | None:
    """读取归档元数据；``include_screenings=True`` 时无损解压原正文。"""
    row = _fetch_archive_row(conn, manifest_id)
    if row is None:
        return None
    result = _archive_dict(row, already_archived=True)
    if include_screenings:
        triples = archive_codec.decode_archive(
            row["codec"], row["compressed_data"], row["gzip_data"]
        )
        result["screenings"] = [
            {"date": d, "session_no": no, "title": title}
            for d, no, title in sorted(triples, key=lambda t: (t[0], t[1]))
        ]
    return result


def list_archives(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT manifest_id, version, start_date, end_date, screening_count,
               original_screening_count, codec, body_sha256, raw_bytes,
               compressed_bytes, archived_at
        FROM archives
        ORDER BY manifest_id
        """
    ).fetchall()
    return [_archive_dict(r, already_archived=True) for r in rows]


def _coverage_gap_isos(
    conn: sqlite3.Connection,
    manifest_id: int,
    version: int,
    start_iso: str,
    end_iso: str,
) -> list[tuple[str, str]]:
    """核对目标清单整段区间是否被更高版清单区间的并集（允许首尾接续）覆盖。

    空的更高版清单同样贡献覆盖区间（空日期也是有效覆盖），因此这里只看
    manifests 中的区间，不看场次行，也不要求压缩件解压。
    """
    target = (
        iv.to_ord(date.fromisoformat(start_iso)),
        iv.to_ord(date.fromisoformat(end_iso)),
    )
    rows = conn.execute(
        "SELECT start_date, end_date FROM manifests WHERE version > ?",
        (version,),
    ).fetchall()
    coverers: list[iv.Interval] = []
    for r in rows:
        coverers.append(
            (
                iv.to_ord(date.fromisoformat(r["start_date"])),
                iv.to_ord(date.fromisoformat(r["end_date"])),
            )
        )
    gaps = iv.subtract_intervals(target, iv.merge_intervals(coverers))
    return [
        (iv.to_date(s).isoformat(), iv.to_date(e).isoformat()) for s, e in gaps
    ]


def archive_manifest(
    conn: sqlite3.Connection, manifest_id: int
) -> ArchiveOutcome:
    """按清单号压缩归档。

    流程：
    1. 快照内确认清单存在、是否已归档（重复归档直接返回原结果）；
    2. 纯区间核对更高版并集是否完整覆盖，有缺口抛 :class:`ArchiveGap`；
    3. 快照读全部场次行，**在写事务外无损压缩**——此时补录、查档不受阻；
    4. ``BEGIN IMMEDIATE`` 事务内复检归档标记与正文摘要，随后写压缩件、
       删逐场行，一起提交；任何异常都回滚，旧清单逐场行原样保留。
    """
    # ---- 阶段一：快照读 + 资格核对（不持写锁） ----
    with snapshot(conn):
        manifest = conn.execute(
            "SELECT id, version, start_date, end_date FROM manifests WHERE id = ?",
            (manifest_id,),
        ).fetchone()
        if manifest is None:
            raise ManifestNotFound(manifest_id)

        existing = _fetch_archive_row(conn, manifest_id)
        if existing is not None:
            return ArchiveOutcome(
                _archive_dict(existing, already_archived=True),
                already_archived=True,
            )

        gaps = _coverage_gap_isos(
            conn,
            manifest_id,
            manifest["version"],
            manifest["start_date"],
            manifest["end_date"],
        )
        if gaps:
            raise ArchiveGap(manifest_id, manifest["version"], gaps)

        rows = conn.execute(
            """
            SELECT date, session_no, title FROM screenings
            WHERE manifest_id = ?
            ORDER BY date, session_no
            """,
            (manifest_id,),
        ).fetchall()
        original_count = len(rows)
        original_signature = {(r["date"], r["session_no"], r["title"]) for r in rows}

    # 事务外压缩：耗时的 CPU/IO 不会占用写锁，补录与查档照常进行。
    packed = archive_codec.compress_rows(
        [(r["date"], r["session_no"], r["title"]) for r in rows]
    )

    # ---- 阶段二：原子换入压缩件并清理逐场行 ----
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 并发的另一次归档可能已经完成：幂等返回已归档结果。
        existing = _fetch_archive_row(conn, manifest_id)
        if existing is not None:
            conn.rollback()
            return ArchiveOutcome(
                _archive_dict(existing, already_archived=True),
                already_archived=True,
            )

        manifest = conn.execute(
            "SELECT id, version, start_date, end_date FROM manifests WHERE id = ?",
            (manifest_id,),
        ).fetchone()
        if manifest is None:  # 理论上不会发生（无删除清单 API），稳妥处理
            conn.rollback()
            raise ManifestNotFound(manifest_id)

        # 复检逐场行未在压缩期间被改动；本服务中场次行对已入库清单不可变，
        # 若将来出现变更入口，这里用当前正文重新打包，保证删掉的正是存下的。
        current_signature = _screening_signature(conn, manifest_id)
        if current_signature != original_signature:
            current_rows = conn.execute(
                "SELECT date, session_no, title FROM screenings WHERE manifest_id = ?",
                (manifest_id,),
            ).fetchall()
            packed = archive_codec.compress_rows(
                [(r["date"], r["session_no"], r["title"]) for r in current_rows]
            )
            original_count = len(current_rows)

        archived_at = datetime.utcnow().isoformat()
        conn.execute(
            """
            INSERT INTO archives (
                manifest_id, version, start_date, end_date,
                screening_count, original_screening_count,
                codec, compressed_data, gzip_data, body_sha256,
                raw_bytes, compressed_bytes, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest_id,
                manifest["version"],
                manifest["start_date"],
                manifest["end_date"],
                original_count,
                original_count,
                packed["codec"],
                sqlite3.Binary(packed["compressed_data"]),
                sqlite3.Binary(packed["gzip_data"]),
                packed["body_sha256"],
                packed["raw_bytes"],
                packed["compressed_bytes"],
                archived_at,
            ),
        )
        deleted = conn.execute(
            "DELETE FROM screenings WHERE manifest_id = ?",
            (manifest_id,),
        ).rowcount
        if deleted != original_count:
            # 行数对不上说明状态异常：回滚，压缩件与逐场行都保持原状。
            raise sqlite3.DatabaseError(
                f"归档清理行数异常：应有 {original_count} 行，实删 {deleted} 行"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    row = _fetch_archive_row(conn, manifest_id)
    assert row is not None
    return ArchiveOutcome(
        _archive_dict(row, already_archived=False),
        already_archived=False,
    )

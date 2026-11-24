"""压缩归档功能测试。

覆盖任务要求的关键场景：
- 几份更高版清单首尾接续覆盖整段才可归档，有缺口退回并给区间；
- 空清单也算（空目标可归档、空新版区间也算覆盖）；
- 归档保留清单号与原场次数，压缩件无损，逐场行在同一事务内删除；
- 日常查档结果归档前后完全一致，且不需要解压；
- 归档后同版重投：原文判重（duplicate）或内容不符报 409；
- 重复归档幂等返回同一份结果；
- 压缩期间补录/查档照常；并发重复归档只产生一份压缩件；
- 压缩失败/清理失败整体回滚，旧清单照常用；
- 旧版数据卷（无 archives 表）可直接迁移，重开连接后数据仍在。
"""
from __future__ import annotations

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import archive_codec, main
from app.database import (
    ArchiveGap,
    archive_manifest,
    connect,
    get_archive,
    initialize,
    insert_manifest,
)


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    db_path = tmp_path / "archive_test.db"
    monkeypatch.setattr(main, "DB_PATH", str(db_path))
    initialize(connect(str(db_path)))
    with TestClient(app=main.app) as c:
        yield c


def _manifest(version: int, start: str, end: str, screenings: list[list]):
    return {
        "version": version,
        "start_date": start,
        "end_date": end,
        "screenings": [
            {"date": d, "session_no": no, "title": title}
            for d, no, title in screenings
        ],
    }


def _post(client, payload, expected=201):
    resp = client.post("/manifests", json=payload)
    assert resp.status_code == expected, resp.text
    return resp.json()


def _seed_target_with_coverers(client: TestClient, count: int = 120):
    """种入一份有上百场明细的 v1 目标清单，以及接续覆盖它的 v2/v3。"""
    rows = []
    # 1/1~1/10，每天 12 场 = 120 场
    titles = ["胶片甲", "胶片乙", "修复版丙", "午夜场丁"]
    for day in range(1, 11):
        for no in range(1, 13):
            rows.append(
                [f"2026-01-{day:02d}", no, f"{titles[no % 4]}-{day}-{no}"]
            )
    target = _post(
        client, _manifest(1, "2026-01-01", "2026-01-10", rows)
    )
    # v2 覆盖 1/1~1/5
    _post(client, _manifest(2, "2026-01-01", "2026-01-05", []))
    # v3 接续覆盖 1/6~1/10（相邻日期，无缺口）
    _post(
        client,
        _manifest(
            3,
            "2026-01-06",
            "2026-01-10",
            [["2026-01-06", 1, "新版接续片"]],
        ),
    )
    return target, rows


# ---------------------------------------------------------------------------
# 资格核对：接续覆盖与缺口
# ---------------------------------------------------------------------------


def test_archive_rejected_with_gap_and_returns_gap_intervals(client):
    """只有一段更高版覆盖、后段有缺口：409 并按区间给出缺口。"""
    target = _post(
        client,
        _manifest(1, "2026-02-01", "2026-02-10", [["2026-02-03", 1, "旧片"]]),
    )
    _post(client, _manifest(2, "2026-02-01", "2026-02-05", []))

    resp = client.post(f"/manifests/{target['manifest_id']}/archive")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "archive_gap"
    assert detail["manifest_id"] == target["manifest_id"]
    assert detail["gaps"] == [
        {"start_date": "2026-02-06", "end_date": "2026-02-10"}
    ]

    # 缺口期间逐场行一行不少，清单仍未归档
    db = connect(main.DB_PATH)
    try:
        n = db.execute(
            "SELECT COUNT(*) FROM screenings WHERE manifest_id=?",
            (target["manifest_id"],),
        ).fetchone()[0]
        assert n == 1
        assert db.execute("SELECT COUNT(*) FROM archives").fetchone()[0] == 0
    finally:
        db.close()


def test_several_new_versions_adjacent_cover_full_range(client):
    """几份新版首尾接起来覆盖整段（含跨月接续）：允许归档。"""
    target = _post(
        client,
        _manifest(1, "2026-01-30", "2026-02-02", [["2026-01-31", 1, "跨年旧片"]]),
    )
    _post(client, _manifest(2, "2026-01-28", "2026-01-31", []))
    _post(client, _manifest(3, "2026-02-01", "2026-02-02", []))

    resp = client.post(f"/manifests/{target['manifest_id']}/archive")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["manifest_id"] == target["manifest_id"]
    assert body["screening_count"] == 1
    assert body["already_archived"] is False


def test_same_version_coverage_does_not_fill_gap(client):
    """更低版/同版区间不能算“更高版覆盖”：缺口仍要退回。"""
    # 目标是 v2：3/1~3/10
    target = _post(client, _manifest(2, "2026-03-01", "2026-03-10", []))
    # 更低的 v1 覆盖后半段（不能顶替 v2，不算更高版覆盖）
    _post(client, _manifest(1, "2026-03-06", "2026-03-10", []))
    # 更高的 v3 只覆盖前半段
    _post(client, _manifest(3, "2026-03-01", "2026-03-05", []))

    resp = client.post(f"/manifests/{target['manifest_id']}/archive")
    assert resp.status_code == 409
    assert resp.json()["detail"]["gaps"] == [
        {"start_date": "2026-03-06", "end_date": "2026-03-10"}
    ]


def test_empty_manifest_can_be_archived(client):
    """空清单也算：无场次目标在完整更高版覆盖下可归档，场次数为 0。"""
    target = _post(client, _manifest(1, "2026-04-01", "2026-04-03", []))
    _post(client, _manifest(2, "2026-03-30", "2026-04-05", []))

    resp = client.post(f"/manifests/{target['manifest_id']}/archive")
    assert resp.status_code == 200, resp.text
    assert resp.json()["screening_count"] == 0

    detail = client.get(f"/manifests/{target['manifest_id']}/archive").json()
    assert detail["screenings"] == []


def test_empty_new_version_interval_counts_as_coverage(client):
    """新版即使是空清单，其覆盖区间同样构成覆盖。"""
    target = _post(
        client,
        _manifest(1, "2026-05-01", "2026-05-02", [["2026-05-01", 1, "片"]]),
    )
    _post(client, _manifest(2, "2026-05-01", "2026-05-02", []))
    assert client.post(f"/manifests/{target['manifest_id']}/archive").status_code == 200


def test_unknown_manifest_returns_404(client):
    resp = client.post("/manifests/999/archive")
    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "manifest_not_found"


# ---------------------------------------------------------------------------
# 归档产物、无损压缩与查档不变性
# ---------------------------------------------------------------------------


def test_archive_compresses_body_deletes_rows_and_keeps_count(client):
    target, rows = _seed_target_with_coverers(client)

    before = _schedule_full_jan(client)

    resp = client.post(f"/manifests/{target['manifest_id']}/archive")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == target["manifest_id"]
    assert body["manifest_id"] == target["manifest_id"]
    assert body["screening_count"] == 120
    assert body["original_screening_count"] == 120
    assert body["compressed_bytes"] < body["raw_bytes"]
    assert body["compression_ratio"] < 1

    db = connect(main.DB_PATH)
    try:
        # 逐场行已删
        left = db.execute(
            "SELECT COUNT(*) FROM screenings WHERE manifest_id=?",
            (target["manifest_id"],),
        ).fetchone()[0]
        assert left == 0
        # 压缩件在，且可无损还原全部 120 行
        arch = db.execute(
            "SELECT * FROM archives WHERE manifest_id=?",
            (target["manifest_id"],),
        ).fetchone()
        assert arch["screening_count"] == 120
        restored = archive_codec.decode_archive(
            arch["codec"], arch["compressed_data"], arch["gzip_data"]
        )
        expected = {(d, no, title) for d, no, title in rows}
        assert set(restored) == expected
        assert len(restored) == 120
        # 兼容性视图也能查到
        v = db.execute(
            "SELECT screening_count, compressed_data FROM manifest_archives "
            "WHERE manifest_id=?",
            (target["manifest_id"],),
        ).fetchone()
        assert v["screening_count"] == 120
    finally:
        db.close()

    # 日常查档结果归档前后完全一致（被归档的 v1 本来就已不生效）
    after = _schedule_full_jan(client)
    assert before == after

    # /manifests 列表保留原场次数并带归档标记
    rec = [m for m in client.get("/manifests").json() if m["id"] == target["manifest_id"]][0]
    assert rec["screening_count"] == 120
    assert rec["archived"] is True


def _schedule_full_jan(client):
    resp = client.get(
        "/schedule", params={"start_date": "2026-01-01", "end_date": "2026-01-10"}
    )
    assert resp.status_code == 200
    return resp.json()


def test_archive_detail_endpoint_roundtrips_body(client):
    target, rows = _seed_target_with_coverers(client, count=3)
    # 改成少量行更直观：直接用已有 120 行也可，这里校验数量与排序
    client.post(f"/manifests/{target['manifest_id']}/archive")
    detail = client.get(
        f"/manifests/{target['manifest_id']}/archive",
        params={"include_screenings": "true"},
    ).json()
    assert len(detail["screenings"]) == 120
    dates = [s["date"] for s in detail["screenings"]]
    assert dates == sorted(dates)

    meta_only = client.get(
        f"/manifests/{target['manifest_id']}/archive",
        params={"include_screenings": "false"},
    ).json()
    assert meta_only["screenings"] is None


def test_get_missing_archive_returns_404(client):
    _post(client, _manifest(1, "2026-06-01", "2026-06-02", []))
    mid = client.get("/manifests").json()[0]["id"]
    assert client.get(f"/manifests/{mid}/archive").status_code == 404


# ---------------------------------------------------------------------------
# 重复归档幂等
# ---------------------------------------------------------------------------


def test_repeated_archive_returns_same_result(client):
    target, _ = _seed_target_with_coverers(client, count=2)
    url = f"/manifests/{target['manifest_id']}/archive"

    first = client.post(url).json()
    second_resp = client.post(url)
    assert second_resp.status_code == 200
    second = second_resp.json()

    assert second["manifest_id"] == first["manifest_id"]
    assert second["screening_count"] == first["screening_count"] == 120
    assert second["body_sha256"] == first["body_sha256"]
    assert second["archived_at"] == first["archived_at"]
    assert second["already_archived"] is True
    assert first["already_archived"] is False

    db = connect(main.DB_PATH)
    try:
        assert db.execute("SELECT COUNT(*) FROM archives").fetchone()[0] == 1
    finally:
        db.close()

    # 各兼容入口行为一致
    assert client.post(f"/archives/{target['manifest_id']}").status_code == 200
    assert client.post(f"/archive/{target['manifest_id']}").status_code == 200
    assert client.post("/archives", json={"manifest_id": target["manifest_id"]}).status_code == 200
    assert client.post("/archives", json={"id": target["manifest_id"]}).status_code == 200
    assert client.post("/archive", params={"manifest_id": target["manifest_id"]}).status_code == 200
    assert client.post("/manifests/archive", json={"manifest_id": target["manifest_id"]}).status_code == 200
    assert client.post("/archive_manifest", json={"manifest_id": target["manifest_id"]}).status_code == 200
    assert client.post(f"/manifests/{target['manifest_id']}/compress").status_code == 200


# ---------------------------------------------------------------------------
# 归档后同版重投：按原正文判重 / 冲突
# ---------------------------------------------------------------------------


def test_resubmit_identical_after_archive_is_duplicate(client):
    target, rows = _seed_target_with_coverers(client)
    payload = _manifest(1, "2026-01-01", "2026-01-10", rows)
    client.post(f"/manifests/{target['manifest_id']}/archive")

    resp = _post(client, payload)
    assert resp["status"] == "duplicate"
    assert resp["manifest_id"] == target["manifest_id"]
    assert resp["archived"] is True
    # 没有新增清单
    assert len(client.get("/manifests").json()) == 3  # v1 + v2 + v3


def test_resubmit_changed_body_after_archive_conflicts_and_keeps_blob(client):
    target, rows = _seed_target_with_coverers(client)
    client.post(f"/manifests/{target['manifest_id']}/archive")

    changed = _manifest(
        1,
        "2026-01-01",
        "2026-01-10",
        [[d, no, title + "（改）"] for d, no, title in rows[:3]],
    )
    resp = client.post("/manifests", json=changed)
    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "same_version_conflict"
    assert resp.json()["detail"]["existing_manifest_id"] == target["manifest_id"]

    # 归档件完好、逐场行仍为删除状态、重复归档仍是同一份结果
    detail = client.get(f"/manifests/{target['manifest_id']}/archive").json()
    assert len(detail["screenings"]) == 120
    again = client.post(f"/manifests/{target['manifest_id']}/archive").json()
    assert again["screening_count"] == 120
    assert again["already_archived"] is True


def test_resubmit_same_version_different_range_still_conflicts(client):
    target, rows = _seed_target_with_coverers(client)
    client.post(f"/manifests/{target['manifest_id']}/archive")

    # 同版但区间不同且交叠 -> 仍按冲突退回（交叠区间来自归档保留的清单区间）
    resp = client.post(
        "/manifests",
        json=_manifest(
            1,
            "2026-01-05",
            "2026-01-15",
            [["2026-01-08", 1, "区间不同的重投"]],
        ),
    )
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "same_version_conflict"
    assert detail["overlap_start"] == "2026-01-05"
    assert detail["overlap_end"] == "2026-01-10"


# ---------------------------------------------------------------------------
# 失败原子性
# ---------------------------------------------------------------------------


def test_compression_failure_leaves_manifest_fully_usable(client, monkeypatch):
    target, _ = _seed_target_with_coverers(client)

    def boom(_rows):
        raise RuntimeError("compress boom")

    db = connect(main.DB_PATH)
    # 嵌套 context 只临时替换压缩函数，退出时还原，不影响 fixture 的 DB_PATH
    with monkeypatch.context() as mpx:
        mpx.setattr(archive_codec, "compress_rows", boom)
        with pytest.raises(RuntimeError):
            archive_manifest(db, target["manifest_id"])

    # 旧清单逐场行一行不少，查档/重投核对照常
    try:
        n = db.execute(
            "SELECT COUNT(*) FROM screenings WHERE manifest_id=?",
            (target["manifest_id"],),
        ).fetchone()[0]
        assert n == 120
        assert get_archive(db, target["manifest_id"]) is None
    finally:
        db.close()

    # 压缩函数已还原，API 可成功归档
    assert client.post(f"/manifests/{target['manifest_id']}/archive").status_code == 200


def test_delete_failure_rolls_back_archive_insert(client):
    """压缩件插入后清理逐场行失败：整个事务回滚，旧清单照常用。"""
    target, _ = _seed_target_with_coverers(client)
    db = connect(main.DB_PATH)
    db.execute("BEGIN")
    db.execute(
        """
        CREATE TEMP TRIGGER fail_delete BEFORE DELETE ON screenings
        BEGIN
            SELECT RAISE(ABORT, 'injected delete failure');
        END;
        """
    )
    db.commit()  # 临时触发器定义本身提交（temp 对象）
    with pytest.raises(sqlite3.DatabaseError):
        archive_manifest(db, target["manifest_id"])

    # 同一连接确认：没有归档件残留，逐场行都在
    assert get_archive(db, target["manifest_id"]) is None
    n = db.execute(
        "SELECT COUNT(*) FROM screenings WHERE manifest_id=?",
        (target["manifest_id"],),
    ).fetchone()[0]
    assert n == 120
    db.execute("DROP TRIGGER fail_delete")
    db.close()

    # 新连接视角同样无归档残留，随后可正常归档
    db2 = connect(main.DB_PATH)
    assert get_archive(db2, target["manifest_id"]) is None
    db2.close()
    assert client.post(f"/manifests/{target['manifest_id']}/archive").status_code == 200


# ---------------------------------------------------------------------------
# 压缩期间并发补录/查档；并发重复归档
# ---------------------------------------------------------------------------


def test_ingest_and_query_proceed_during_compression(tmp_path, monkeypatch):
    db_path = tmp_path / "conc.db"
    initialize(connect(str(db_path)))

    setup = connect(str(db_path))
    from app.schemas import ManifestIn, ScreeningIn

    def in_manifest(v, s, e, titles):
        return ManifestIn(
            version=v,
            start_date=s,
            end_date=e,
            screenings=[
                ScreeningIn(date=s, session_no=i + 1, title=t)
                for i, t in enumerate(titles)
            ],
        )

    insert_manifest(setup, in_manifest(1, "2026-07-01", "2026-07-10", [f"片{i}" for i in range(50)]))
    insert_manifest(setup, in_manifest(2, "2026-07-01", "2026-07-10", ["新版"]))
    setup.close()

    entered = threading.Event()
    release = threading.Event()
    original = archive_codec.compress_rows

    def slow_compress(rows):
        entered.set()
        assert release.wait(timeout=10)
        return original(rows)

    monkeypatch.setattr(archive_codec, "compress_rows", slow_compress)

    errors: list[Exception] = []

    def do_archive():
        conn = connect(str(db_path))
        try:
            outcome = archive_manifest(conn, 1)
            assert outcome.archive["screening_count"] == 50
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            conn.close()

    t = threading.Thread(target=do_archive)
    t.start()
    assert entered.wait(timeout=5), "压缩阶段未开始"

    # 压缩阻塞期间：另一条连接上补录一份不冲突的同版别段清单 + 查档
    other = connect(str(db_path))
    try:
        mid, status = insert_manifest(
            other, in_manifest(1, "2026-08-01", "2026-08-02", ["补录片"])
        )
        assert status == "accepted"
        from app.repository import get_schedule
        from datetime import date

        sch = get_schedule(other, date(2026, 7, 1), date(2026, 7, 10))
        assert [s.title for s in sch.screenings] == ["新版"]
    finally:
        other.close()

    release.set()
    t.join(timeout=10)
    assert not t.is_alive()
    assert errors == []

    check = connect(str(db_path))
    try:
        assert get_archive(check, 1) is not None
        assert check.execute(
            "SELECT COUNT(*) FROM screenings WHERE manifest_id=1"
        ).fetchone()[0] == 0
        # 压缩期间补录的清单仍在
        row = check.execute(
            "SELECT id FROM manifests WHERE start_date='2026-08-01'"
        ).fetchone()
        assert row is not None
        aug = check.execute(
            "SELECT title FROM screenings WHERE manifest_id=? AND title='补录片'",
            (row["id"],),
        ).fetchone()
        assert aug is not None
    finally:
        check.close()


def test_concurrent_repeated_archive_single_artifact(tmp_path):
    db_path = tmp_path / "race.db"
    initialize(connect(str(db_path)))
    setup = connect(str(db_path))
    setup.execute(
        "INSERT INTO manifests (id, version, start_date, end_date, accepted_at) "
        "VALUES (1, 1, '2026-09-01', '2026-09-03', '2026-01-01T00:00:00')"
    )
    setup.execute(
        "INSERT INTO manifests (version, start_date, end_date, accepted_at) "
        "VALUES (2, '2026-09-01', '2026-09-03', '2026-01-02T00:00:00')"
    )
    setup.executemany(
        "INSERT INTO screenings (manifest_id, date, session_no, title) "
        "VALUES (1, ?, ?, ?)",
        [(f"2026-09-0{d}", no, f"片{d}-{no}") for d in range(1, 4) for no in range(1, 6)],
    )
    setup.commit()
    setup.close()

    n_threads = 8
    barrier = threading.Barrier(n_threads)
    outcomes: list[tuple[bool, int]] = []
    errors: list[Exception] = []

    def worker():
        barrier.wait()
        conn = connect(str(db_path))
        try:
            outcome = archive_manifest(conn, 1)
            outcomes.append(
                (outcome.already_archived, outcome.archive["manifest_id"])
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    assert errors == []
    assert len(outcomes) == n_threads
    assert all(mid == 1 for _, mid in outcomes)
    assert sum(1 for already, _ in outcomes if not already) == 1
    assert sum(1 for already, _ in outcomes if already) == n_threads - 1

    check = connect(str(db_path))
    try:
        assert check.execute("SELECT COUNT(*) FROM archives").fetchone()[0] == 1
        assert check.execute(
            "SELECT COUNT(*) FROM screenings WHERE manifest_id=1"
        ).fetchone()[0] == 0
    finally:
        check.close()


# ---------------------------------------------------------------------------
# 旧数据卷迁移与重启持久化
# ---------------------------------------------------------------------------


def test_existing_volume_old_schema_migrates_and_persists(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    # 模拟旧版本留下的库：只有最初的两张表
    legacy = sqlite3.connect(str(db_path))
    legacy.executescript(
        """
        CREATE TABLE manifests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            accepted_at TEXT NOT NULL
        );
        CREATE TABLE screenings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manifest_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            session_no INTEGER NOT NULL,
            title TEXT NOT NULL
        );
        INSERT INTO manifests (id, version, start_date, end_date, accepted_at)
        VALUES (1, 1, '2026-10-01', '2026-10-02', '2026-01-01T00:00:00');
        INSERT INTO screenings (manifest_id, date, session_no, title)
        VALUES (1, '2026-10-01', 1, '旧卷里的片子');
        """
    )
    legacy.commit()
    legacy.close()

    # 新代码直接在旧卷上初始化（幂等迁移），不报错、不丢数据
    conn = connect(str(db_path))
    initialize(conn)
    assert get_archive(conn, 1) is None
    gap = None
    try:
        archive_manifest(conn, 1)
    except ArchiveGap as exc:
        gap = exc.gaps
    assert gap == [("2026-10-01", "2026-10-02")]
    conn.close()

    # 补一份更高版全覆盖后归档
    conn = connect(str(db_path))
    conn.execute(
        "INSERT INTO manifests (version, start_date, end_date, accepted_at) "
        "VALUES (2, '2026-10-01', '2026-10-02', '2026-01-03T00:00:00')"
    )
    conn.commit()
    outcome = archive_manifest(conn, 1)
    assert outcome.archive["screening_count"] == 1
    conn.commit()  # archive_manifest 已提交；无害
    conn.close()

    # “重启”：全新连接 + initialize，归档与核对重投依旧可用
    conn2 = connect(str(db_path))
    initialize(conn2)
    arch = get_archive(conn2, 1, include_screenings=True)
    assert arch is not None
    assert arch["screenings"][0]["title"] == "旧卷里的片子"
    conn2.close()

    # 通过全新 TestClient（再次走 lifespan 初始化）验证 HTTP 重投判重
    monkeypatch.setattr(main, "DB_PATH", str(db_path))
    with TestClient(main.app) as fresh:
        resp = fresh.post(
            "/manifests",
            json=_manifest(
                1,
                "2026-10-01",
                "2026-10-02",
                [["2026-10-01", 1, "旧卷里的片子"]],
            ),
        )
        assert resp.status_code == 201
        assert resp.json()["status"] == "duplicate"
        sch = fresh.get(
            "/schedule",
            params={"start_date": "2026-10-01", "end_date": "2026-10-02"},
        ).json()
        # v2 空清单全覆盖 -> 确认无放映
        assert sch["screenings"] == []
        assert sch["confirmed_no_screening"] == [
            {"start_date": "2026-10-01", "end_date": "2026-10-02"}
        ]

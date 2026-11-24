"""电影资料馆放映档案 FastAPI 入口。

- POST /manifests/{id}/archive  按清单号压缩归档（整段被更高版覆盖才可）
- GET  /manifests/{id}/archive  查看归档（可选解压正文核对）
- POST /manifests               录入一份清单（整份接受或整份退回）
- GET  /schedule                按日期范围查询生效场次与空日期段
- GET  /manifests               查看已入库清单（归档后仍保留原场次数）
- GET  /docs                    Swagger UI，可直接录入、归档和查档
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Iterator

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from sqlite3 import Connection

from .database import (
    ArchiveGap,
    ManifestNotFound,
    SameVersionConflict,
    archive_manifest,
    connect,
    get_archive,
    initialize,
    insert_manifest,
    list_archives,
    list_manifests,
)
from .repository import get_schedule
from .schemas import (
    ArchiveDetailResponse,
    ArchiveRequest,
    ArchiveResponse,
    IngestResponse,
    ManifestIn,
    ManifestRecord,
    ScheduleResponse,
)

DB_PATH = os.getenv("ARCHIVE_DB_PATH", "data/archive.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = connect(DB_PATH)
    try:
        initialize(conn)
    finally:
        conn.close()
    yield


app = FastAPI(
    title="电影资料馆放映档案",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "把乱序到达的放映清单（带整数版次与覆盖起止日期）并入可查档案。"
        "每天以覆盖当日的最高版次为准；同版交叠内容不一致整份退回；"
        "空日期以区间表达，不逐日展开。\n\n"
        "v1.1 新增按清单号压缩归档：整段区间被更高版清单（可多份接续、"
        "空清单也算覆盖）完整覆盖后，可将场次正文无损压缩入库并删除逐场行；"
        "归档保留清单号与原场次数，日常查档无需解压，同版重投仍按原正文"
        "判重或报冲突；重复归档幂等返回同一份归档结果。"
    ),
)


def get_conn() -> Iterator[Connection]:
    """每请求一个连接，结束即关闭；读事务/写事务均在服务层内显式管理。"""
    conn = connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


@app.get("/healthz", tags=["运维"])
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/manifests",
    response_model=IngestResponse,
    status_code=201,
    tags=["清单录入"],
    summary="录入一份放映清单",
)
def ingest_manifest(
    payload: ManifestIn, conn: Connection = Depends(get_conn)
) -> IngestResponse:
    try:
        manifest_id, status = insert_manifest(conn, payload)
    except SameVersionConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "same_version_conflict",
                "message": str(exc),
                "existing_manifest_id": exc.existing_id,
                "overlap_start": exc.overlap_start,
                "overlap_end": exc.overlap_end,
            },
        )
    archived_hit = (
        status == "duplicate"
        and get_archive(conn, manifest_id, include_screenings=False) is not None
    )
    return IngestResponse(
        manifest_id=manifest_id,
        version=payload.version,
        start_date=payload.start_date,
        end_date=payload.end_date,
        screening_count=len(payload.screenings),
        status=status,
        archived=archived_hit,
    )


@app.get(
    "/schedule",
    response_model=ScheduleResponse,
    tags=["档案查询"],
    summary="按日期范围返回有效场次与空日期段",
)
def query_schedule(
    start_date: date = Query(..., description="查询起始日，含当天"),
    end_date: date = Query(..., description="查询截止日，含当天"),
    conn: Connection = Depends(get_conn),
) -> ScheduleResponse:
    if end_date < start_date:
        raise HTTPException(
            status_code=422,
            detail="end_date 不能早于 start_date",
        )
    # 整个计算在单条快照事务内完成，结果来自同一个已提交状态
    return get_schedule(conn, start_date, end_date)


@app.get(
    "/manifests",
    response_model=list[ManifestRecord],
    tags=["清单录入"],
    summary="列出已入库清单（含归档标记与保留的原场次数）",
)
def get_manifests(conn: Connection = Depends(get_conn)) -> list[ManifestRecord]:
    rows = list_manifests(conn)
    return [
        ManifestRecord(
            id=r["id"],
            version=r["version"],
            start_date=date.fromisoformat(r["start_date"]),
            end_date=date.fromisoformat(r["end_date"]),
            screening_count=r["screening_count"],
            accepted_at=datetime.fromisoformat(r["accepted_at"]),
            archived=bool(r["is_archived"]),
        )
        for r in rows
    ]


@app.get(
    "/manifests/{manifest_id}",
    response_model=ManifestRecord,
    tags=["清单录入"],
    summary="查看单份清单（含归档标记与保留的原场次数）",
)
def get_manifest(
    manifest_id: int, conn: Connection = Depends(get_conn)
) -> ManifestRecord:
    rows = list_manifests(conn)
    for r in rows:
        if r["id"] == manifest_id:
            return ManifestRecord(
                id=r["id"],
                version=r["version"],
                start_date=date.fromisoformat(r["start_date"]),
                end_date=date.fromisoformat(r["end_date"]),
                screening_count=r["screening_count"],
                accepted_at=datetime.fromisoformat(r["accepted_at"]),
                archived=bool(r["is_archived"]),
            )
    raise HTTPException(
        status_code=404,
        detail={"error": "manifest_not_found", "manifest_id": manifest_id},
    )


# ---------------------------------------------------------------------------
# 压缩归档
# ---------------------------------------------------------------------------


def _archive_response(data: dict) -> ArchiveResponse:
    raw = data.get("raw_bytes") or 0
    compressed = data.get("compressed_bytes") or 0
    ratio = round(compressed / raw, 4) if raw else None
    already = bool(data["already_archived"])
    count = data["screening_count"]
    return ArchiveResponse(
        manifest_id=data["manifest_id"],
        id=data["manifest_id"],
        version=data["version"],
        start_date=date.fromisoformat(data["start_date"]),
        end_date=date.fromisoformat(data["end_date"]),
        screening_count=count,
        original_screening_count=data["original_screening_count"],
        count=count,
        original_count=data["original_screening_count"],
        message=(
            f"清单 {data['manifest_id']} 已归档（幂等重放，返回既有归档结果）"
            if already
            else f"清单 {data['manifest_id']} 已压缩归档，逐场行已在同一事务内清理"
        ),
        already_archived=already,
        duplicate=already,
        codec=data["codec"],
        compressed_bytes=compressed,
        raw_bytes=raw,
        compression_ratio=ratio,
        body_sha256=data["body_sha256"],
        archived_at=datetime.fromisoformat(data["archived_at"]),
    )


def _do_archive(conn: Connection, manifest_id: int) -> ArchiveResponse:
    try:
        outcome = archive_manifest(conn, manifest_id)
    except ManifestNotFound as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "manifest_not_found",
                "message": str(exc),
                "manifest_id": manifest_id,
            },
        )
    except ArchiveGap as exc:
        gaps = [
            {"start_date": s, "end_date": e} for s, e in exc.gaps
        ]
        raise HTTPException(
            status_code=409,
            detail={
                "error": "archive_gap",
                "message": str(exc),
                "manifest_id": exc.manifest_id,
                "version": exc.version,
                "gaps": gaps,
                # 语义别名，方便不同核对脚本取值
                "missing_ranges": gaps,
                "uncovered_ranges": gaps,
            },
        )
    return _archive_response(outcome.archive)


@app.post(
    "/manifests/{manifest_id}/archive",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    summary="按清单号压缩归档（推荐入口）",
    description=(
        "仅当该清单的整段覆盖区间都被 version 严格更高的清单区间并集覆盖"
        "（多份新版可首尾接续，空清单的覆盖区间也算）时才执行。\n\n"
        "成功后：archives 中保存无损压缩正文、清单号与原场次数，"
        "screenings 中该清单的逐场行在同一事务内删除。\n\n"
        "有覆盖缺口返回 409 与 gaps；清单不存在返回 404；"
        "重复归档返回 200 与同一份归档结果（already_archived=true）。"
    ),
)
def archive_by_manifest_path(
    manifest_id: int, conn: Connection = Depends(get_conn)
) -> ArchiveResponse:
    return _do_archive(conn, manifest_id)


@app.get(
    "/manifests/{manifest_id}/archive",
    response_model=ArchiveDetailResponse,
    tags=["压缩归档"],
    summary="查看清单的归档件；默认解压还原场次正文以供补录核对",
)
def get_manifest_archive(
    manifest_id: int,
    include_screenings: bool = Query(
        default=True, description="是否解压返回原场次正文"
    ),
    conn: Connection = Depends(get_conn),
) -> ArchiveDetailResponse:
    data = get_archive(conn, manifest_id, include_screenings=include_screenings)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "archive_not_found",
                "message": f"清单 id={manifest_id} 尚无归档件",
                "manifest_id": manifest_id,
            },
        )
    base = _archive_response(data)
    screenings = data.get("screenings")
    return ArchiveDetailResponse(
        **base.model_dump(),
        screenings=screenings,
    )


@app.get(
    "/archives",
    response_model=list[ArchiveResponse],
    tags=["压缩归档"],
    summary="列出全部归档件（仅元数据，不解压正文）",
)
def get_archives(conn: Connection = Depends(get_conn)) -> list[ArchiveResponse]:
    return [_archive_response(row) for row in list_archives(conn)]


def _resolve_body_id(
    payload: ArchiveRequest | None, manifest_id_query: int | None
) -> int:
    if payload is not None:
        try:
            return payload.resolved_id()
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
    if manifest_id_query is not None:
        return manifest_id_query
    raise HTTPException(
        status_code=422,
        detail="必须在请求体或查询参数中提供 manifest_id",
    )


@app.post(
    "/archives",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    summary="按清单号压缩归档（集合式入口，请求体传 manifest_id）",
)
def create_archive(
    payload: ArchiveRequest | None = Body(default=None),
    manifest_id_query: int | None = Query(
        default=None, alias="manifest_id", description="也可用查询参数传清单号"
    ),
    conn: Connection = Depends(get_conn),
) -> ArchiveResponse:
    manifest_id = _resolve_body_id(payload, manifest_id_query)
    return _do_archive(conn, manifest_id)


@app.get(
    "/archives/{manifest_id}",
    response_model=ArchiveDetailResponse,
    tags=["压缩归档"],
    summary="按清单号查看归档详情（兼容路径）",
)
def get_archive_by_id(
    manifest_id: int,
    include_screenings: bool = Query(default=True),
    conn: Connection = Depends(get_conn),
) -> ArchiveDetailResponse:
    data = get_archive(conn, manifest_id, include_screenings=include_screenings)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "archive_not_found", "manifest_id": manifest_id},
        )
    base = _archive_response(data)
    return ArchiveDetailResponse(**base.model_dump(), screenings=data.get("screenings"))


# --- 路径别名：兼容 “/archive/{id}”、“/archives/{id}” 等命名习惯 ---


@app.post(
    "/archives/{manifest_id}",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    summary="按清单号归档（/archives/{id} 别名）",
    include_in_schema=False,
)
def archive_by_archives_id(
    manifest_id: int, conn: Connection = Depends(get_conn)
) -> ArchiveResponse:
    return _do_archive(conn, manifest_id)


@app.post(
    "/archive/{manifest_id}",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    include_in_schema=False,
)
def archive_singular_alias(
    manifest_id: int, conn: Connection = Depends(get_conn)
) -> ArchiveResponse:
    return _do_archive(conn, manifest_id)


@app.get(
    "/archive/{manifest_id}",
    response_model=ArchiveDetailResponse,
    tags=["压缩归档"],
    include_in_schema=False,
)
def get_archive_singular_alias(
    manifest_id: int,
    include_screenings: bool = Query(default=True),
    conn: Connection = Depends(get_conn),
) -> ArchiveDetailResponse:
    data = get_archive(conn, manifest_id, include_screenings=include_screenings)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "archive_not_found", "manifest_id": manifest_id},
        )
    base = _archive_response(data)
    return ArchiveDetailResponse(**base.model_dump(), screenings=data.get("screenings"))


@app.post(
    "/archive",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    include_in_schema=False,
)
def archive_singular_collection_alias(
    payload: ArchiveRequest | None = Body(default=None),
    manifest_id_query: int | None = Query(default=None, alias="manifest_id"),
    conn: Connection = Depends(get_conn),
) -> ArchiveResponse:
    return _do_archive(conn, _resolve_body_id(payload, manifest_id_query))


@app.post(
    "/manifests/archive",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    include_in_schema=False,
)
def archive_manifests_static_body_alias(
    payload: ArchiveRequest | None = Body(default=None),
    manifest_id_query: int | None = Query(default=None, alias="manifest_id"),
    conn: Connection = Depends(get_conn),
) -> ArchiveResponse:
    return _do_archive(conn, _resolve_body_id(payload, manifest_id_query))


@app.post(
    "/archive_manifest",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    include_in_schema=False,
)
def archive_manifest_named_alias(
    payload: ArchiveRequest | None = Body(default=None),
    manifest_id_query: int | None = Query(default=None, alias="manifest_id"),
    conn: Connection = Depends(get_conn),
) -> ArchiveResponse:
    return _do_archive(conn, _resolve_body_id(payload, manifest_id_query))


@app.post(
    "/manifests/archive/{manifest_id}",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    include_in_schema=False,
)
def archive_manifests_named_prefix_alias(
    manifest_id: int, conn: Connection = Depends(get_conn)
) -> ArchiveResponse:
    return _do_archive(conn, manifest_id)


@app.post(
    "/manifests/{manifest_id}/compress",
    response_model=ArchiveResponse,
    status_code=200,
    tags=["压缩归档"],
    summary="压缩并归档（/compress 动作别名，行为同 /archive）",
)
def compress_manifest_alias(
    manifest_id: int, conn: Connection = Depends(get_conn)
) -> ArchiveResponse:
    return _do_archive(conn, manifest_id)

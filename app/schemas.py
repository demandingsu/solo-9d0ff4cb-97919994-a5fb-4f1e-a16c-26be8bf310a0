"""Pydantic schemas for manifest ingestion and schedule queries."""
from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class ScreeningIn(BaseModel):
    """单场放映：日期、当日唯一的场次号、片名。"""

    date: date
    session_no: int = Field(ge=1, description="场次号，正整数，同一清单同一天内唯一")
    title: str = Field(min_length=1, description="片名，不可为空白")


class ManifestIn(BaseModel):
    """一份放映清单：整数版次 + 覆盖起止日期（含首尾）+ 范围内全部场次，可为空。"""

    version: int = Field(ge=1, description="整数版次，越大越新")
    start_date: date = Field(description="覆盖起始日，含当天")
    end_date: date = Field(description="覆盖截止日，含当天")
    screenings: list[ScreeningIn] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate(self) -> "ManifestIn":
        if self.end_date < self.start_date:
            raise ValueError("end_date 不能早于 start_date")
        seen: set[tuple[date, int]] = set()
        normalized: list[ScreeningIn] = []
        for s in self.screenings:
            if not (self.start_date <= s.date <= self.end_date):
                raise ValueError(
                    f"场次日期 {s.date.isoformat()} 超出清单覆盖范围 "
                    f"[{self.start_date.isoformat()}, {self.end_date.isoformat()}]"
                )
            key = (s.date, s.session_no)
            if key in seen:
                raise ValueError(
                    f"{s.date.isoformat()} 的场次号 {s.session_no} 重复"
                )
            seen.add(key)
            title = s.title.strip()
            if not title:
                raise ValueError("片名不可为空白")
            normalized.append(
                ScreeningIn(date=s.date, session_no=s.session_no, title=title)
            )
        self.screenings = normalized
        return self


class DateInterval(BaseModel):
    """闭区间日期段，含首尾两天。"""

    start_date: date
    end_date: date


class ScreeningOut(BaseModel):
    date: date
    session_no: int
    title: str
    version: int = Field(description="该场次当前生效所依据的清单版次")


class ScheduleResponse(BaseModel):
    query_start: date
    query_end: date
    screenings: list[ScreeningOut] = Field(description="当前生效场次，按日期、场次号排序")
    confirmed_no_screening: list[DateInterval] = Field(
        description="已被清单覆盖、但覆盖清单确认当天无放映的日期段"
    )
    no_manifest_yet: list[DateInterval] = Field(
        description="还没有任何清单覆盖到的日期段"
    )


class ManifestRecord(BaseModel):
    id: int
    version: int
    start_date: date
    end_date: date
    screening_count: int = Field(
        description="清单原场次数；归档后仍保留原数（不是 0）"
    )
    accepted_at: datetime
    archived: bool = Field(
        default=False, description="是否已压缩归档（逐场行已删，正文在压缩件中）"
    )


class IngestResponse(BaseModel):
    manifest_id: int
    version: int
    start_date: date
    end_date: date
    screening_count: int
    status: Literal["accepted", "duplicate"] = Field(
        description="accepted=新入库；duplicate=完全相同的清单已存在，原样保留未重复入库"
    )
    archived: bool = Field(
        default=False,
        description="duplicate 命中的旧清单是否已压缩归档（仍按压缩前正文判重）",
    )


class ArchiveRequest(BaseModel):
    """``POST /archives`` 的请求体；同时兼容 manifest_id / id 两种键名。"""

    manifest_id: int | None = Field(default=None, description="要归档的清单号")
    id: int | None = Field(default=None, description="manifest_id 的兼容别名")

    def resolved_id(self) -> int:
        if self.manifest_id is not None:
            return self.manifest_id
        if self.id is not None:
            return self.id
        raise ValueError("必须提供 manifest_id")


class ArchiveResponse(BaseModel):
    """归档结果。重复归档返回同一份归档记录（``already_archived=true``）。"""

    manifest_id: int = Field(description="清单号（manifests.id），永久保留")
    id: int = Field(description="与 manifest_id 相同，兼容按 id 取值的调用方")
    version: int
    start_date: date
    end_date: date
    screening_count: int = Field(description="原场次数，永久保留")
    original_screening_count: int
    count: int = Field(description="screening_count 的兼容别名")
    original_count: int = Field(description="original_screening_count 的兼容别名")
    message: str | None = Field(default=None, description="可读的结果说明")
    status: Literal["archived"] = Field(
        default="archived",
        description="归档后的稳定状态；首次与重复归档均为 archived",
    )
    archive_status: Literal["archived"] = "archived"
    state: Literal["archived"] = "archived"
    archived: bool = True
    already_archived: bool = Field(
        description="false=本次新归档；true=此前已归档，本次为幂等重放"
    )
    duplicate: bool = Field(
        description="already_archived 的兼容别名，便于沿用 accepted/duplicate 词汇"
    )
    codec: str
    compressed_bytes: int
    raw_bytes: int
    compression_ratio: float | None = None
    body_sha256: str
    archived_at: datetime
    gaps: list[DateInterval] = Field(
        default_factory=list, description="成功时为空；缺口信息在 409 响应中返回"
    )


class ArchivedScreening(BaseModel):
    date: date
    session_no: int
    title: str


class ArchiveDetailResponse(ArchiveResponse):
    """归档详情；仅显式查看归档时才解压正文。"""

    screenings: list[ArchivedScreening] | None = Field(
        default=None, description="include_screenings=true 时无损还原的原场次正文"
    )

"""归档正文的无损序列化、压缩与校验工具。

场次正文以规范 JSON（按日期、场次号排序）序列化后压缩：

- 主压缩格式为 zlib（RFC1950），存于 ``compressed_data``；
- 同时留一份 gzip（RFC1952）字节，便于用通用 gzip 工具核对；
- ``body_sha256`` 是**压缩前**规范 JSON 字节的摘要，用于无损校验。

日常查档（schedule / manifests 列表）不调用本模块；只有同版重投的
正文核对、归档详情查看等场景才需要解压。
"""
from __future__ import annotations

import gzip
import hashlib
import json
import zlib
from collections.abc import Iterable, Sequence

# (date_iso, session_no, title)
ScreeningTriple = tuple[str, int, str]

CODEC_ZLIB_JSON = "zlib+json-v1"
CODEC_GZIP_JSON = "gzip+json-v1"


def canonical_bytes(rows: Iterable[Sequence[object]]) -> bytes:
    """把场次行转成排序稳定、无多余空白的 UTF-8 JSON 字节。

    接受 ``(date, session_no, title)`` 三元组或任意可按同序索引的行。
    """
    triples: list[ScreeningTriple] = [
        (str(r[0]), int(r[1]), str(r[2])) for r in rows
    ]
    triples.sort(key=lambda t: (t[0], t[1]))
    payload = [
        {"date": d, "session_no": no, "title": title}
        for d, no, title in triples
    ]
    return json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    ).encode("utf-8")


def compress_rows(
    rows: Iterable[Sequence[object]],
) -> dict[str, object]:
    """返回压缩件与校验信息（在写事务外调用，压缩过程不阻塞入库/查档）。"""
    raw = canonical_bytes(rows)
    zlib_blob = zlib.compress(raw, level=9)
    gzip_blob = gzip.compress(raw, compresslevel=9, mtime=0)
    return {
        "codec": CODEC_ZLIB_JSON,
        "compressed_data": zlib_blob,
        "gzip_data": gzip_blob,
        "body_sha256": hashlib.sha256(raw).hexdigest(),
        "raw_bytes": len(raw),
        "compressed_bytes": len(zlib_blob),
    }


def _parse_raw(raw: bytes) -> list[ScreeningTriple]:
    data = json.loads(raw.decode("utf-8"))
    return [
        (str(item["date"]), int(item["session_no"]), str(item["title"]))
        for item in data
    ]


def decompress_zlib(blob: bytes) -> list[ScreeningTriple]:
    return _parse_raw(zlib.decompress(blob))


def decompress_gzip(blob: bytes) -> list[ScreeningTriple]:
    return _parse_raw(gzip.decompress(blob))


def decode_archive(
    codec: str | None,
    compressed_data: bytes | None,
    gzip_data: bytes | None = None,
) -> list[ScreeningTriple]:
    """按 codec 解压；对历史/异构压缩件做 zlib/gzip 自适应兜底。"""
    if codec == CODEC_GZIP_JSON and compressed_data is not None:
        return decompress_gzip(compressed_data)
    if compressed_data is not None:
        try:
            return decompress_zlib(compressed_data)
        except zlib.error:
            # 也许主列里放的是 gzip
            return decompress_gzip(compressed_data)
    if gzip_data is not None:
        return decompress_gzip(gzip_data)
    raise ValueError("归档中没有可解压的正文")


def sha256_of_rows(rows: Iterable[Sequence[object]]) -> str:
    return hashlib.sha256(canonical_bytes(rows)).hexdigest()

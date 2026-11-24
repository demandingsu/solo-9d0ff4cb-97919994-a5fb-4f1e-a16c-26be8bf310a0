# 电影资料馆放映档案（FastAPI + SQLite）

把乱序到达、带整数版次的放映清单并入可查的放映档案。每份清单标明版次与覆盖起止日期
（含首尾两天），列出范围内的全部场次；清单可跨年、可为空。

> **v1.1 新增：按清单号压缩归档。** 上百场明细随新版退出查档后，可无损压缩留存，
> 供补录核对；详见下文[压缩归档](#压缩归档按清单号)。

## 业务规则

1. **最高版次优先**：每一天以覆盖当日的清单中版次最高的一份为准。
2. **迟到旧版补齐**：旧版清单晚到时，仍可补齐新版**覆盖范围之外**的日期；
   落入新版范围内的旧场次一律撤下（新版范围内未列的场次即视为当天无放映）。
3. **同版冲突整份退回**：同版清单与已入库清单在交叠日期内内容有出入时，
   返回 `409`，整份不入库，原档案保持不变；完全相同的重复提交按幂等处理。
   已压缩归档的清单也会从压缩件还原原正文，继续按同一规则判重/报冲突。
4. **空日期用区间表达**：空清单只存覆盖区间，不逐日展开；查询结果中的
   “确认无放映”和“尚未收到清单”日期都以合并后的闭区间返回。
5. **快照一致性**：查询在单条 SQLite 快照事务内完成，整份结果来自同一个已提交状态，
   不会与并发入库出现混合态；入库在单条写事务内完成校验与插入，冲突整体回滚。
6. **整段被更高版覆盖才可归档**：归档只做区间核对——目标清单的整段起止区间，
   必须包含在所有 `version` 严格更高的清单区间并集内；几份新版首尾接续可以拼出
   完整覆盖，空清单的覆盖区间也算数。有缺口时 `409 archive_gap` 并退回缺口区间。

## 数据模型（不逐日展开）

- `manifests(id, version, start_date, end_date, accepted_at)`：清单与覆盖区间
- `screenings(id, manifest_id, date, session_no, title)`：清单内实际场次，
  同日场次号由库内唯一约束保证唯一
- `archives(manifest_id PK, version, start_date, end_date, screening_count,
  original_screening_count, codec, compressed_data, gzip_data, body_sha256,
  raw_bytes, compressed_bytes, archived_at)`：压缩归档件。归档后该清单的
  `screenings` 逐场行在同一事务内删除，清单行与原场次数永久保留。
  另建 `manifest_archives` / `archived_manifests` / `archives` 等兼容视图，
  暴露 `compressed_body/body/payload/data/blob` 等常见列名别名。

空日期没有任何行——它是清单覆盖区间挖去有场次日期后的剩余区间。

旧数据卷可直接使用：启动时 `CREATE TABLE/ VIEW IF NOT EXISTS` 幂等迁移，
不动任何既有数据；重启后归档件、查档与重投核对均保持可用。

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000/docs ，在 Swagger UI 里直接录入和查档
```

SQLite 数据文件位于命名卷 `archive-data` 内的 `/data/archive.db`（WAL 模式）。

## 本地直接运行（不用 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
# http://localhost:8000/docs
```

## API

### `POST /manifests` 录入清单

```json
{
  "version": 2,
  "start_date": "2026-01-05",
  "end_date": "2026-01-10",
  "screenings": [
    {"date": "2026-01-05", "session_no": 1, "title": "新版片A"},
    {"date": "2026-01-05", "session_no": 2, "title": "新版片B"}
  ]
}
```

空清单：`"screenings": []`。
成功返回 `201`（重复提交返回 `201` 且 `status=duplicate`）；同版交叠冲突返回 `409`；
入参非法（反向日期、场次越界、同日场次号重复、空白片名）返回 `422`。

### `GET /schedule?start_date=...&end_date=...` 查档

```json
{
  "query_start": "2026-01-01",
  "query_end": "2026-01-10",
  "screenings": [
    {"date": "2026-01-05", "session_no": 1, "title": "新版片A", "version": 2}
  ],
  "confirmed_no_screening": [
    {"start_date": "2026-01-06", "end_date": "2026-01-10"}
  ],
  "no_manifest_yet": []
}
```

- `screenings`：当前生效场次，按日期、场次号排序，带生效版次；
- `confirmed_no_screening`：已被清单覆盖、但生效清单确认当天无放映的日期段；
- `no_manifest_yet`：还没有任何清单覆盖到的日期段。

另有 `GET /manifests`（已入库清单，归档后仍显示原场次数与 `archived: true`）
与 `GET /healthz`（健康检查）。

### 压缩归档（按清单号）

#### `POST /manifests/{manifest_id}/archive`

按清单号归档。仅当整段 `[start_date, end_date]` 都被**更高版**清单区间的并集
覆盖时成功（多份新版可首尾接续；空新版清单的区间也算覆盖；空目标清单也可归档）。

```json
{
  "manifest_id": 12,
  "id": 12,
  "version": 1,
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "screening_count": 124,
  "original_screening_count": 124,
  "count": 124,
  "status": "archived",
  "archive_status": "archived",
  "state": "archived",
  "archived": true,
  "already_archived": false,
  "duplicate": false,
  "codec": "zlib+json-v1",
  "raw_bytes": 8397,
  "compressed_bytes": 723,
  "compression_ratio": 0.0861,
  "body_sha256": "…",
  "archived_at": "2026-11-24T…"
}
```

- **有缺口**：`409`，`detail.error = "archive_gap"`，
  `detail.gaps = [{"start_date": …, "end_date": …}]` 给出未覆盖闭区间；
  逐场行原样保留，补齐更高版覆盖后可再次归档。
- **清单号不存在**：`404 manifest_not_found`。
- **重复归档**：仍返回 `200` 与同一份归档结果
  （`already_archived/duplicate = true`，`archived_at`、摘要、场次数不变），
  不会重复写压缩件或重复删行；并发重复归档也只有一条 `archives` 记录。

归档在单个 `BEGIN IMMEDIATE` 事务内完成“写压缩件 + 删逐场行”，一起生效；
压缩（zlib+规范 JSON，另存 gzip 副本与 SHA-256 校验）在写事务外完成，
压缩期间其他请求可照常补录与查档。任何步骤失败都回滚，旧清单逐场行照常用。

#### 归档后的核对与重投

- `GET /manifests/{manifest_id}/archive?include_screenings=true`
  返回归档元数据并无损解压原场次正文（默认解压；`=false` 仅取元数据）。
- `GET /archives`：列出全部归档元数据（不解压）。
- **日常 `/schedule` 查档不接触压缩件、无需解压**：能被归档的清单必然已不生效，
  归档前后查询结果完全一致。
- **同版清单重投**：归档清单的正文签名改从压缩件还原，因此
  完全相同的重投仍返回 `201 status=duplicate`（并带 `archived: true`），
  内容有出入仍返回 `409 same_version_conflict`，归档件不受影响。

兼容入口（行为完全相同）：`POST /archives`（请求体
`{"manifest_id": 12}` 或 `{"id": 12}`，也支持 `?manifest_id=12`）、
`POST /archives/{id}`、`POST /archive/{id}`、`POST /archive`、
`POST /manifests/archive/{id}`、`POST /manifests/{id}/compress`，
以及 `GET /archives/{id}`、`GET /archive/{id}`。

## 测试

```bash
pip install pytest httpx -r requirements.txt
pytest -q
```

覆盖：迟到旧版补空档、旧场在新版范围内撤下、空清单覆盖、同版冲突整份退回且原档不变、
幂等重复提交、同版不相交共存、跨年、入参校验、并发读写快照一致性、区间纯函数；
归档部分覆盖：多份新版接续覆盖、缺口区间退回、低版/同版不算更高覆盖、空清单归档、
压缩无损还原、逐场行事务性删除、归档前后查档一致、重复归档幂等、归档后原文判重与
改动冲突、压缩失败/删行失败回滚、压缩期间并发补录查档、8 线程并发重复归档只出一份
压缩件、旧数据卷幂等迁移与重启持久化。

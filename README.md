# 轮轴超声探伤 · 可续传分片上传服务（Python 3.13 + FastAPI）

车间网络不稳定时，探伤设备上传大型扫描文件只需重传**缺失的分片**，不再整包重传。
服务把会话元数据、每片 SHA-256 摘要与接收位图持久化到 SQLite，分片正文落盘；
API 重启后从“已确认位置”继续，绝不把已确认分片误判为缺失。仅当**全部分片到齐
且按序组装后的整文件 SHA-256 等于建档值**时，才原子发布成品文件。

## 目录与运行产物

```
app/            FastAPI 应用（config / db / storage / models / ranges /
                services / content / routes / errors）
scripts/verify.py  一次性端到端验收脚本（含“重启后续传”真实进程验证）
tests/          pytest 测试（续传/重启边界、幂等、冲突、过期、完整性、结构化错误）
data/           运行产物（已 gitignore），Docker 中为命名卷 upload-data
  registry.db   SQLite（WAL）：sessions + chunks（chunks 行集 = 接收位图）
  chunks/<upload_id>/<index>   已确认分片正文（fsync + 原子 rename）
  tmp/          组装暂存
  files/<upload_id>/<filename> 原子发布的成品（fsync + rename）
```

崩溃安全性：分片字节先 `fsync` 落盘，**然后**才在事务中写入确认行；
成品先写入同目录临时文件并 `fsync`，校验摘要通过后 `os.replace` 原子就位，
再在同一发布流程中把会话标记为 `complete`。因此任何时刻重启都不会出现
“数据库说收到了但文件不在”或“半成品被当成成品”。

## 快速开始（Docker Compose）

```bash
# 默认宿主端口 8000；可用 API_PORT 覆盖
API_PORT=18000 docker compose up -d --build api

# 一次性验收服务：跑完即退出（内部会真实地新起一个 API 进程验证重启续传）
docker compose run --rm verify
```

健康检查：`GET /api/v1/health` → `204`。交互式 API 文档：`/docs`（OpenAPI）。

## 本地开发（pytest）

```bash
pip install -r requirements-dev.txt
pytest -q
# 真实进程端到端（可选）：
DATA_DIR=./data uvicorn app.main:app --port 8000 &
API_BASE_URL=http://127.0.0.1:8000 DATA_DIR=./data python scripts/verify.py
```

## 接口规则

所有请求/响应均为 JSON（分片上传的请求体为原始字节 `application/octet-stream`）。
**任何错误都返回统一结构化信封**，HTTP 状态码与机器可读错误码并存：

```json
{ "error": { "code": "checksum_mismatch",
             "message": "Uploaded chunk bytes do not match ...",
             "details": { "chunk_index": 1, "claimed_sha256": "...", "actual_sha256": "..." } } }
```

### 1) 建档 `POST /api/v1/uploads` → `201`

请求体：

| 字段 | 规则 |
| --- | --- |
| `filename` | 非空、不含路径分隔符的纯文件名 |
| `file_size` | 整文件字节数，≥ 0 |
| `chunk_size` | 分片字节数，> 0 |
| `file_sha256` | 整文件 SHA-256，64 位小写十六进制 |
| `expires_at` | ISO-8601 **带时区**的过期时刻；必须晚于当前时间 |

派生规则（服务端计算，不信任客户端）：

- `total_chunks = ceil(file_size / chunk_size)`，分片序号**从 0 开始**；
- 除最后一片外，每片长度必须**恰好等于** `chunk_size`；最后一片长度为
  `file_size - (total_chunks - 1) * chunk_size`（整除时没有短分片）；
- `file_size = 0` 时 `total_chunks = 0`，摘要等于空文件摘要时立即发布成品。

```bash
curl -sS -X POST http://localhost:8000/api/v1/uploads \
  -H 'Content-Type: application/json' \
  -d '{"filename":"axle_001.scan","file_size":524295,"chunk_size":262144,
       "file_sha256":"9f0b...（64 hex）",
       "expires_at":"2026-09-16T00:00:00Z"}'
# -> 201 {"upload_id":"ab12...","total_chunks":3,"status":"open",
#         "chunks_received":0,"missing_chunks":[0,1,2], ...}
```

### 2) 上传分片 `PUT /api/v1/uploads/{upload_id}/chunks/{chunk_index}` → `200`

- 请求体为该片**原始字节**；必带头 `X-Chunk-SHA256: <该片字节的 SHA-256>`。
- 越界序号（含负号、`>= total_chunks`）→ `416 chunk_out_of_range`。
- 长度不符合上面的分片长度规则 → `422 invalid_upload`，**不记入位图**。
- 实际摘要 ≠ `X-Chunk-SHA256` → `422 checksum_mismatch`，**不记入位图**。
- 相同序号、**相同内容**重传 → `200` 且 `"idempotent": true`（断网重传安全）。
- 相同序号、**不同内容** → `409 chunk_conflict`，已存分片原封不动。
- 会话过期后一律 → `410 session_expired`，进度不被污染。
- 当新确认的分片补齐全部缺口时，服务自动组装并校验发布：
  整文件摘要不符 → `422 integrity_error`，会话保持 `open`、分片全部保留、
  不发布任何成品（随后同内容重传仍是幂等 `200`，显式 `finalize` 会再次给出
  可复核的完整性错误）。

```bash
# 切分（示例）
split -b 262144 -d axle_001.scan /tmp/chunk_
# 上传第 0 片（断点后可反复重发同一文件）
digest=$(sha256sum /tmp/chunk_00 | cut -d' ' -f1)
curl -sS -X PUT http://localhost:8000/api/v1/uploads/$UID/chunks/0 \
  -H "X-Chunk-SHA256: $digest" \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @/tmp/chunk_00
# -> 200 {"upload_id":"...","chunk_index":0,"received":true,
#         "chunks_received":1,"total_chunks":3,"complete":false,"idempotent":false}
```

### 3) 查询状态 `GET /api/v1/uploads/{upload_id}` → `200`

`missing_chunks` 始终是**升序**的缺片序号；设备重启/换进程后先查状态，
只重发这些序号即可。另含 `chunks_received`、`status`（`open/complete/expired`）、
`expired`、`assembled_path`（发布后为成品绝对路径）。

```bash
curl -sS http://localhost:8000/api/v1/uploads/$UID
# -> 200 {"status":"open","chunks_received":1,"missing_chunks":[1,2], ...}
```

### 4) 显式完结 `POST /api/v1/uploads/{upload_id}/finalize`

- 有缺片 → `409 incomplete_upload`（details 含升序 `missing_chunks`）；
- 已过期 → `410 session_expired`；
- 齐全但组装摘要不符 → `422 integrity_error`（含 `declared/actual_sha256`）；
- 成功（或此前已发布）→ `200`，含 `status:"complete"` 与 `assembled_path`。

设备在所有分片发完后应调用一次，得到“可复核的完成文件”或“明确的失败”。

### 5) 字节预检 / 下载 `GET /api/v1/uploads/{upload_id}/content`

探伤人员可在大扫描**尚未传完**时按字节预检波形；会话完成后仍访问**同一地址**
取回正式文件。服务把请求区间按 `chunk_size` 映射到已确认分片，**跨片读取直接
逐片流式拼接，不生成任何临时整包**。

`Range` 支持四种写法（可多段，逗号分隔）：

| 写法 | 含义 |
| --- | --- |
| `bytes=200-999` | 单段，闭区间 |
| `bytes=200-` | 开放末端：200 到文件最后一个字节 |
| `bytes=-500` | 后缀：最后 500 个字节 |
| `bytes=0-99,200-299` | 多段；重叠区间会被规范化合并，越界末端自动收敛 |

**开放（未完成）会话**只有在目标区间触及的每一个分片同时满足

1. 在 SQLite 中有确认行（接收位图）；
2. 分片正文在磁盘上存在；
3. 正文 SHA-256 与确认行记录的摘要一致、长度符合分片规则，

时才响应；否则返回 `409 range_unavailable`，错误体给出**升序**的
`missing_chunks` / `corrupt_chunks`（及并集 `unavailable_chunks`），
响应发出前不会泄出任何扫描字节。

- 单段成功 → `206`，带 `Content-Range: bytes <start>-<end>/<size>`；
- 多段成功 → `206 multipart/byteranges`，边界由“会话 + 区间”确定性派生
  （相同请求的边界与分段头稳定），每段带各自的 `Content-Range`；
- 成功响应都带 `ETag: "<整文件 SHA-256>"` 与 `Accept-Ranges: bytes`；
- 非法或全部不可满足的范围 → `416 range_not_satisfiable`，并带
  `Content-Range: bytes */<总大小>`（多段中仅个别越界时忽略越界段）；
- 完成会话不带 `Range` → `200` 流式返回原子发布的成品文件；
- **完成前后对同一区间返回完全相同的字节与 ETag**（预检即可当作正式取数）。

```bash
# 上传中途预检第 1、3 两个波形窗口（可跨片）
curl -sS http://localhost:8000/api/v1/uploads/$UID/content \
  -H 'Range: bytes=1048576-1048675,3145728-3145827' -o preview.bin
# 缺片时：409 {"error":{"code":"range_unavailable",
#   "details":{"missing_chunks":[12],"corrupt_chunks":[], ...}}}
# 全部完成后同一 URL、不带 Range 即下载正式文件
curl -sS http://localhost:8000/api/v1/uploads/$UID/content -o axle_001.scan
```

预检是只读操作：不获取写锁、不修改位图/会话状态，因此与并发分片上传互不阻塞。


## 错误码一览

| HTTP | `error.code` | 触发场景 |
| --- | --- | --- |
| 400/422 | `validation_error` / `invalid_upload` | 请求体字段、分片长度、摘要头格式不合法 |
| 404 | `session_not_found` / `not_found` | 会话不存在 / 路由不存在 |
| 405 | `method_not_allowed` | 方法不允许 |
| 409 | `chunk_conflict` | 同序号不同内容 |
| 409 | `incomplete_upload` | 缺片时尝试完结 |
| 409 | `range_unavailable` | 预检区间触及缺片/丢失或损坏的分片（不返回字节） |
| 410 | `session_expired` | 过期会话拒绝任何新分片 |
| 416 | `chunk_out_of_range` | 分片序号越界 |
| 416 | `range_not_satisfiable` | Range 非法或全部不可满足（带 `Content-Range: bytes */<size>`） |
| 422 | `checksum_mismatch` | 分片摘要与字节不符 |
| 422 | `integrity_error` | 组装后整文件 SHA-256 ≠ 建档值 |
| 500 | `internal_error` | 未预期内部错误（同样结构化） |

## 重启续传语义（关键保证）

1. 分片字节与确认行分别以 fsync/事务落盘，确认行是“接收位图”的唯一事实来源；
2. 重启后状态接口直接从 SQLite + 磁盘重建位图，已确认分片**不会**被报为缺失；
3. 对已确认序号的重传按内容判定：相同 → 幂等成功；不同 → 409 且不动旧数据；
4. 发布是“全部到齐 + 整文件摘要匹配”后的原子动作，失败永远保留可复核的缺片/
   完整性错误，且不污染任何既有进度。

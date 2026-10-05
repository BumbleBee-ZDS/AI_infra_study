# LLM Gateway

OpenAI 兼容的本地 LLM 网关：统一鉴权、限流、模型路由、指标采集与日志，后端转发到
[Ollama](https://ollama.com)。

三件基础设施都已接入，且**每一件都能独立降级**——缺任何一件网关都照常提供推理服务：

| 能力 | 实现 | 作用与降级行为 |
|------|------|----------------|
| 分布式限流 | **Redis**（固定窗口 + Lua 原子计数） | 多实例共享同一份额度；Redis 挂了自动回退进程内计数（`REDIS_FAIL_OPEN=1`）或直接拒绝（`=0`），恢复后自动切回 |
| 请求历史 | **SQLite**（默认）/ **PostgreSQL**（psycopg 3） | 每次请求的 TTFT、TPS、token、Ollama 参数标签落库；异步批量写入，库崩了只丢历史不阻塞请求 |
| 异步事件 | **Kafka**（aiokafka 生产者） | `request.completed` / `request.rate_limited` / `request.auth_failed` / `request.error` 事件外发到 `llm-gateway-events`；broker 挂了降级写 `logs/events.jsonl` 并自动重连 |

## 依赖安装

```bash
pip install -r requirements.txt
```

## 启动

方式一（直接运行，读取 `SERVER_HOST` / `SERVER_PORT` 环境变量，默认 `0.0.0.0:8000`）：

```bash
python main.py
```

方式二（uvicorn 热重载，开发用）：

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

方式三（接上 Redis / PostgreSQL / Kafka，生产姿势）：

```bash
bash scripts/infra.sh install      # 首次：免 root 下载解包三件套（详见「分布式基础设施」）
bash scripts/infra.sh start

REDIS_URL=redis://localhost:6379/0 \
DATABASE_URL=postgresql://gw:gw@localhost:5433/gateway \
KAFKA_BOOTSTRAP=localhost:9092 \
python main.py
```

> 一个都不装也能跑：限流走进程内计数，历史写 `logs/gateway.db`（SQLite），事件直接写
> `logs/events.jsonl`。启动日志会打印实际生效的后端。

方式四（多实例 + nginx 负载均衡，扩容姿势）：

```bash
bash scripts/nginx.sh install      # 首次：免 root 解包 nginx
bash scripts/cluster.sh start      # 3 个实例(8000/8001/8002) + nginx 入口 :8080
bash scripts/cluster.sh scale 5    # 扩到 5 个（热加载，不断连接）
```

（详见「多实例部署 + nginx 负载均衡」。）

> 需要本机已运行 Ollama（默认 `http://localhost:11434`，可用 `OLLAMA_URL` 覆盖），
> 且已拉取模型，例如：`ollama pull qwen2.5:0.5b`。

## 配置（`config.py` / 环境变量）

| 名称 | 默认值 | 说明 |
|------|--------|------|
| `OLLAMA_URL` | `http://localhost:11434` | Ollama 地址 |
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `8000` | 监听端口 |
| `DEFAULT_MODEL` | `qwen0.5b` | 请求未带 `model` 时使用的别名 |
| `GPU_SAMPLE_INTERVAL` | `5` | GPU 指标后台采样间隔（秒） |
| `GATEWAY_INSTANCE` | `gateway-<port>` | 实例标识，写进历史与事件（多实例排障用） |
| `REDIS_URL` | `redis://localhost:6379/0` | 限流用 Redis 地址 |
| `RATE_LIMIT_BACKEND` | `auto` | `auto`（能连 Redis 就用）/ `redis`（强制，连不上按 `REDIS_FAIL_OPEN` 处理）/ `memory`（强制进程内） |
| `RATE_LIMIT_WINDOW` | `60` | 限流窗口秒数，窗口内超过 `RATE_LIMIT[tier]` 即 429 |
| `REDIS_TIMEOUT` | `1.0` | Redis 单次操作超时（限流在主链路上，不能等太久） |
| `REDIS_FAIL_OPEN` | `1` | Redis 不可用时是否放行：`1` 用内存计数兜底，`0` 直接 429（配额优先） |
| `DATABASE_URL` | `sqlite:///logs/gateway.db` | `sqlite:///绝对路径` 或 `postgresql://user:pwd@host:5432/db` |
| `HISTORY_BATCH_SIZE` | `20` | 攒够多少行历史落一次库 |
| `HISTORY_FLUSH_INTERVAL` | `1.0` | 最长等待秒数（先到者触发写入） |
| `HISTORY_QUEUE_SIZE` | `5000` | 历史写入队列上限，满则丢弃并计数 |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPIC` | `llm-gateway-events` | 事件 topic |
| `KAFKA_CLIENT_ID` | `llm-gateway` | 生产者 client.id |
| `KAFKA_ENABLED` | `1` | `0` 则彻底关闭事件发布 |
| `EVENT_FALLBACK_FILE` | `logs/events.jsonl` | broker 不可用时的降级落盘文件 |

模型别名映射（`MODEL_ROUTES`）：

| 别名 | Ollama 模型 |
|------|-------------|
| `qwen0.5b` | `qwen2.5:0.5b` |
| `qwen1.5b` | `qwen2.5:1.5b` |
| `llama1b` | `llama3.2:1b` |

## 接口

### `GET /health`

```json
{"status": "ok", "ollama": "up", "instance": "gateway-8000",
 "rate_limit_backend": "redis", "history_backend": "postgres", "kafka_connected": true}
```

### `GET /infra`

三件基础设施的完整运行状态（端口、DSN（密码已打码）、队列积压、事件计数、降级原因）：

```json
{
  "instance": "gateway-8000",
  "rate_limit": {"mode": "redis", "active": "redis", "redis_up": true,
                 "redis_url": "redis://localhost:6379/0", "window_seconds": 60,
                 "fail_open": true, "error": ""},
  "history": {"enabled": true, "backend": "postgres",
              "dsn": "postgresql://gw:***@localhost:5433/gateway",
              "queued": 0, "written": 128, "batch_size": 20, "flush_interval": 1.0, "error": ""},
  "events": {"enabled": true, "connected": true, "bootstrap": "localhost:9092",
             "topic": "llm-gateway-events", "queued": 0, "published": 131,
             "failed": 0, "fallback_written": 0, "dropped": 0, "error": ""}
}
```

### `GET /history?limit=20&model=&api_key=`

最近 N 条请求历史（倒序，`limit` 上限 200），数据来自 SQLite / PostgreSQL：

```json
{"count": 2, "backend": "postgres", "items": [
  {"id": 128, "ts": "2026-10-03T23:28:50", "api_key": "sk-test-001", "tier": "free",
   "model": "qwen2.5:0.5b", "status": 200, "stream": 1, "wall_time": 0.412,
   "ttft": 0.017, "tps": 74.01, "prompt_tokens": 26, "completion_tokens": 4,
   "total_tokens": 30, "num_ctx": "512", "num_gpu": "default", "num_batch": "default",
   "rate_limit_backend": "redis", "instance": "gateway-8000", "error": null}]}
```

### `GET /history/stats?window=3600`

窗口内聚合（`window` 单位秒）：请求数、错误数、平均/P95 延迟、token 用量、分模型维度：

```json
{"enabled": true, "backend": "postgres", "window_seconds": 3600,
 "totals": {"requests": 28, "errors": 2, "avg_wall_time": 0.104, "avg_ttft": 0.091,
            "avg_tps": 116.8, "p95_wall_time": 0.111, "p95_ttft": 0.42,
            "prompt_tokens": 728, "completion_tokens": 112},
 "by_model": [{"model": "qwen2.5:0.5b", "requests": 26, "avg_ttft": 0.091, "avg_tps": 116.8}],
 "rows_total": 28, "rows_this_instance": 28}
```

### `GET /metrics`

### `POST /v1/chat/completions`

请求头需带 `x-api-key`（演示 key：`sk-test-001` 为 free，`sk-prod-001` 为 pro）。

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"你好"}]}'
```

流式（`"stream": true`）返回 OpenAI 风格的 SSE，以 `data: [DONE]` 结束。

可选请求字段：

| 字段 | 说明 |
|------|------|
| `options` | 透传给 Ollama 的参数（白名单校验，未知 key / 类型 / 越界返回 400）：`num_ctx` `num_gpu` `num_batch` `num_predict` `num_thread` `temperature` `top_k` `top_p` `repeat_penalty` |
| `stream_options.include_usage` | 为 `true` 时流式在 `[DONE]` 前多回一个 `choices: []` 分片，含 `usage`（token 用量）与 `metrics`（网关计算的 `ttft`/`tps`，供调优实验采集） |

`num_ctx` / `num_gpu` / `num_batch` 同时会作为 Prometheus 标签（未指定时为 `default`），
按参数分组的 TTFT / 生成速度曲线见下方「Ollama 参数调优实验」。

```bash
curl -N -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-prod-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"你好"}],
       "stream":true,"stream_options":{"include_usage":true},
       "options":{"num_ctx":2048,"num_batch":128}}'
```

### `GET /metrics`

Prometheus 抓取端点。除自定义的 `gateway_*` 指标外，还包含 `process_*` / `python_*`
运行时指标。

## 目录结构

```
main.py                     # FastAPI 应用：鉴权 → 限流 → 路由 → 转发 → 指标/日志/历史/事件
config.py                   # 全部环境变量、限流配额、模型路由
auth.py / router.py         # API Key 校验、模型别名解析
ratelimit.py                # 分布式限流（Redis 固定窗口 + Lua，进程内兜底）
storage.py                  # 请求历史（SQLite / PostgreSQL，异步批量写入）
events.py                   # Kafka 事件生产者（broker 不可用时降级 JSONL）
prometheus_metrics.py       # 指标定义与记录辅助函数
metrics.py / logger.py      # GPU 采样 / JSONL 文件日志
tuning_experiment.py        # Ollama 参数调优扫描实验
verify_infra.py             # 基础设施验收（双实例限流对照 + 历史 + 事件）
verify_cluster.py           # 多实例 + nginx 负载均衡验收（含故障转移）
study.md                    # 从零上手学习手册（分 11 个阶段 + 12 道练习 + 附录）
scripts/infra.sh            # Redis / PostgreSQL / Kafka 免 root 本地部署
scripts/cluster.sh          # 多实例网关集群（start / scale / status / logs）
scripts/nginx.sh            # nginx 负载均衡层（install / start / reload / status）
nginx/nginx.conf.tpl        # 负载均衡配置模板（由 scripts/nginx.sh 渲染）
scripts/install_monitoring.sh, scripts/monitoring.sh    # Prometheus + Grafana
monitoring/                 # Prometheus 抓取配置 + Grafana 仪表盘与 provisioning
logs/                       # gateway.log / gateway.db / events.jsonl / cluster/ / 实验输出
```

## 监控（Prometheus + Grafana）

本机无 Docker，因此 Prometheus / Grafana 以**原生二进制**方式运行，二进制安装在
`$MON_HOME`（默认 `~/opt/monitoring`），配置与脚本都在本仓库的 `monitoring/`、`scripts/` 下。

```
monitoring/
├── prometheus.yml                                  # 抓取配置（targets 由 GATEWAY_TARGETS 渲染）
└── grafana/
    ├── dashboards/llm-gateway.json                 # 仪表盘（35 个面板 / 5 个行）
    └── provisioning/
        ├── datasources/prometheus.yml              # 数据源
        └── dashboards/dashboards.yml.tpl           # 仪表盘自动加载（启动时渲染绝对路径）
scripts/
├── install_monitoring.sh                           # 下载并解压 Prometheus / Grafana
└── monitoring.sh                                   # start / stop / restart / status / logs / targets / reload
nginx/
└── nginx.conf.tpl                                  # nginx 负载均衡模板（由 scripts/nginx.sh 渲染）
```

### 首次安装

```bash
bash scripts/install_monitoring.sh
# 可自定义安装位置：MON_HOME=/data/monitoring bash scripts/install_monitoring.sh
```

### 启动 / 停止

```bash
bash scripts/monitoring.sh start      # 启动 Prometheus + Grafana
bash scripts/monitoring.sh status
bash scripts/monitoring.sh logs
bash scripts/monitoring.sh stop
bash scripts/monitoring.sh targets    # 看渲染出的抓取目标 + 当前状态
bash scripts/monitoring.sh reload     # 改了 GATEWAY_TARGETS 后热加载
```

抓取目标由 `GATEWAY_TARGETS`（逗号分隔）渲染，默认单实例 `localhost:8000`；
多实例集群写成 `GATEWAY_TARGETS=localhost:8000,localhost:8001,localhost:8002` 即可
（详见下方「多实例部署 + nginx 负载均衡」）。

| 服务 | 地址 | 说明 |
|------|------|------|
| 网关指标 | http://localhost:8000/metrics | 被抓取端点（集群时每个实例各一个） |
| Prometheus | http://localhost:9090 | 抓取目标：http://localhost:9090/targets |
| Grafana | http://localhost:3000 | 匿名只读可看，登录 `admin` / `admin` |
| 仪表盘 | http://localhost:3000/d/llm-gateway | 「LLM Gateway 监控」 |

### 指标一览（`prometheus_metrics.py`）

| 指标 | 类型 | 标签 |
|------|------|------|
| `gateway_requests_total` | Counter | `model` `tier` `status` `stream` |
| `gateway_request_duration_seconds` | Histogram | `model` `tier` |
| `gateway_ttft_seconds` | Histogram | `model` `num_ctx` `num_gpu` `num_batch` |
| `gateway_tokens_per_second` | Histogram | `model` `num_ctx` `num_gpu` `num_batch` |
| `gateway_tokens_total` | Counter | `model` `tier` `kind`(prompt/completion) |
| `gateway_inflight_requests` | Gauge | — |
| `gateway_auth_failures_total` | Counter | — |
| `gateway_rate_limited_total` | Counter | `tier` |
| `gateway_upstream_errors_total` | Counter | `model` |
| `gateway_gpu_utilization_percent` | Gauge | — |
| `gateway_gpu_memory_used_megabytes` | Gauge | — |
| `gateway_ratelimit_backend_up` | Gauge | — （1=Redis 共享计数生效，0=已降级） |
| `gateway_ratelimit_backend_info` | Gauge | `backend`(redis / memory / redis-down) |
| `gateway_ratelimit_redis_errors_total` | Counter | — （Redis 限流失败触发降级的次数） |
| `gateway_history_up` | Gauge | `backend`(sqlite / postgres / none) |
| `gateway_history_writes_total` | Counter | `backend` （写入行数） |
| `gateway_history_write_errors_total` | Counter | `backend` |
| `gateway_history_dropped_total` | Counter | — （队列满丢弃的记录数） |
| `gateway_history_rows` | Gauge | `backend` （表行数） |
| `gateway_events_published_total` | Counter | `event`(request.completed / rate_limited / auth_failed / error) |
| `gateway_events_failed_total` | Counter | `event` |
| `gateway_events_fallback_total` | Counter | — （broker 不可用时降级落盘的事件数） |
| `gateway_events_dropped_total` | Counter | — |
| `gateway_kafka_up` | Gauge | — （1=生产者已连接） |

GPU 指标由 FastAPI lifespan 启动的后台任务每 `GPU_SAMPLE_INTERVAL` 秒采样一次
（`nvidia-smi` 在线程池中执行，不阻塞事件循环），因此空闲时曲线也连续。

仪表盘共 5 行：**总览**、**路由、限流与错误**、**GPU 与进程资源**、
**Ollama 参数调优**、**分布式基础设施**。最后一行 6 个面板分别是限流后端（哪个生效）、
Redis 限流健康度/降级次数、请求历史写入速率、历史表行数与后端可用性、Kafka 事件速率
（按事件类型）、Kafka 连接与降级/丢弃计数——用来快速判断「是哪件基础设施在降级」。

### 排障

**Grafana 页面报 `RangeError: Maximum call stack size exceeded` / "An unexpected error happened"**

原因几乎都是 **`folderUid` 与仪表盘自身的 `uid` 相同**（例如都用 `llm-gateway`）。
此时仪表盘的 `meta.folderUid` 指向自己，Grafana 前端解析面包屑/权限时无限递归。

```bash
# 检查是否存在冲突
curl -s -u admin:admin 'http://localhost:3000/api/search?type=dash-db'     # 看 uid / folderUid
curl -s -u admin:admin 'http://localhost:3000/api/search?type=dash-folder' # 看 uid
```

修复：让 `monitoring/grafana/provisioning/dashboards/dashboards.yml.tpl` 里的 `folderUid`
与 `monitoring/grafana/dashboards/llm-gateway.json` 的 `uid` 不同
（当前分别为 `llm-gateway-folder` 和 `llm-gateway`），然后 `bash scripts/monitoring.sh restart`。
`scripts/monitoring.sh` 在渲染 provisioning 时会自动校验这一约束，冲突会直接报错退出。

> 若之前的错误状态已写入 Grafana 的 SQLite（`$MON_HOME/grafana/data/grafana.db`），
> 而 Grafana 又拒绝通过 API 删除 provisioning 管理的资源，可停止 Grafana 后删除该 db
> 再启动——所有数据源与仪表盘都由文件 provisioning 重建，不会丢失仓库里的配置。

**基础设施（`scripts/infra.sh`）相关**

| 现象 | 原因 / 处理 |
|------|-------------|
| `redis-server: error while loading shared libraries: liblzf.so.1` | 只解包了 Redis 主包；`bash scripts/infra.sh install` 会把 `liblzf1` 一起解包并用 `LD_LIBRARY_PATH` 指好 |
| `initdb: error while loading shared libraries: libpq.so.5` | 同理，PostgreSQL 需要 `libpq5`；脚本里的 `pg_env()` 已统一处理 |
| Kafka 起不来 / `kafka-storage.sh format` 失败 | 先看 `~/opt/infra/logs/kafka-format.log`；Kafka 4.x 要求 Java 17+（本机 Java 25 实测可用） |
| 6379 / 5433 / 9092 端口被占用 | `REDIS_PORT=6380 PG_PORT=5434 KAFKA_PORT=9093 bash scripts/infra.sh start`，网关侧同步改 `REDIS_URL` / `DATABASE_URL` / `KAFKA_BOOTSTRAP` |
| 想换 Kafka topic | `KAFKA_TOPIC=your-topic bash scripts/infra.sh start`，网关侧同步设 `KAFKA_TOPIC` |
| 手改了 `~/opt/infra/kafka/server.properties` 想生效 | 删掉该文件再 `bash scripts/infra.sh start`（脚本以该文件判断是否已生成配置） |
| 网关启动日志里出现 `限流后端: memory`、`事件总线: ... 降级` | 说明对应服务没起来，属于预期降级；`curl localhost:8000/infra` 能看到具体错误 |

> 注意：`/infra`、`/history`、`/history/stats`、`/metrics`、`/health` 都**不做鉴权**
> （演示项目定位）。对公网暴露前请加上与 `/v1/*` 一致的 Key 校验，或只在内网/反代后面暴露。

## 分布式基础设施（Redis 限流 / SQLite·Postgres 历史 / Kafka 事件）

三件套都在 `scripts/infra.sh` 里做「免 root 本地部署」：本机既没有 sudo（要密码）也没有
Docker，所以 Redis / PostgreSQL 走 `apt-get download` + `dpkg-deb -x` 解包到 `$INFRA_HOME`
（默认 `~/opt/infra`），Kafka 用官方 tarball 跑 KRaft 单节点（broker + controller 同进程）。

```bash
bash scripts/infra.sh install    # 下载并解包三件套（幂等，已存在则跳过）
bash scripts/infra.sh start      # 启动 + 首次初始化（建库 / 建 topic）+ 状态一览
bash scripts/infra.sh status
bash scripts/infra.sh logs
bash scripts/infra.sh stop
```

| 服务 | 端口 | 数据目录 | 说明 |
|------|------|----------|------|
| Redis | 6379 | —（`--save ''` 不落盘） | 限流计数，重启即清空 |
| PostgreSQL | 5433 | `~/opt/infra/pgdata` | 用户 `gw` / 库 `gateway`，trust 认证，仅监听 127.0.0.1 |
| Kafka | 9092（controller 9093） | `~/opt/infra/kafka-data` | KRaft 单节点，自动建 `llm-gateway-events`（3 分区） |

> 解包出来的二进制各自缺一个 `.so`：Redis 需要 `liblzf.so.1`、PostgreSQL 需要 `libpq.so.5`。
> 脚本会把这两个包一起解包并用 `LD_LIBRARY_PATH` 指好，不需要往系统里装任何东西。

### 1. 分布式限流：Redis 固定窗口 + Lua

单实例内存计数在水平扩容时会把额度放大成 `实例数 × 配额`。这里的做法是：

- key = `gw:rl:{tier}:{api_key}:{窗口序号}`，窗口序号取 `int(now / RATE_LIMIT_WINDOW)`——
  窗口一过自然换 key，不需要额外的清理任务；
- `INCR` + `PEXPIRE` 放在一段 Lua 脚本里由 Redis 单线程原子执行（redis-py 自动走 EVALSHA），
  避免「加了计数却没设过期」留下永不过期的 key；
- 响应统一带 `X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset`，
  429 额外带 `Retry-After`。

降级策略（`RATE_LIMIT_BACKEND=auto|redis|memory` 搭配 `REDIS_FAIL_OPEN=1|0`）：

| 场景 | `auto` / `fail_open=1`（默认，可用性优先） | `redis` + `fail_open=0`（配额优先） |
|------|------------------------------------------|-------------------------------------|
| 启动时连不上 Redis | 直接用进程内计数，`/health` 显示 `rate_limit_backend=memory` | 每次请求 429 |
| 运行中 Redis 抖动 | 该请求改走进程内计数，后台每 5s 重连，恢复后自动切回 | 该请求 429（`backend=redis-down`） |

对照实验（`python verify_infra.py` 自动执行；free 档 10 次/分钟，两个实例各发 6 次）：

| 后端 | 实例 A | 实例 B | 合计放行 | 结论 |
|------|--------|--------|----------|------|
| `redis` | 6 放行 | 4 放行 / 2 拒绝 | **10** | 额度全局生效，多实例共享 |
| `memory` | 6 放行 | 6 放行 | 12 | 各实例独立计数、额度被放大，只能单实例用 |

### 2. 请求历史：SQLite（默认）或 PostgreSQL

写路径刻意做成异步批量，避免数据库抖动拖慢推理：

```
请求返回 ─► history.submit(record)      只做一次 put_nowait（非阻塞）
                   │
            写入队列（HISTORY_QUEUE_SIZE）
                   │  攒够 HISTORY_BATCH_SIZE 行，或等满 HISTORY_FLUSH_INTERVAL 秒
                   ▼
        asyncio.to_thread(backend.insert_many)      一次多行 INSERT
```

- SQLite 用标准库 `sqlite3`（WAL + `synchronous=NORMAL`，读写互不阻塞），零依赖、开箱即用；
- PostgreSQL 用 `psycopg 3` 同步驱动 + `asyncio.to_thread`，不引入 asyncpg，
  两套方言的差异只体现在占位符（`?` / `%s`）；
- 队列满或写库失败都只计数（`gateway_history_dropped_total` /
  `gateway_history_write_errors_total`），并丢弃坏连接等下一批重连——
  **宁可丢历史，也不阻塞请求**；
- 表结构 `request_history`：`ts, api_key, tier, model, status, stream, messages_count,
  wall_time, ttft, tps, prompt_tokens, completion_tokens, gpu_util, gpu_mem_mb,
  num_ctx, num_gpu, num_batch, rate_limit_backend, instance, error`，
  在 `ts` 与 `(model, ts)` 上建索引，便于按时间窗/模型查。

切换后端只改一个环境变量：

```bash
python main.py                                                   # 默认 SQLite: logs/gateway.db
DATABASE_URL=postgresql://gw:gw@localhost:5433/gateway python main.py
```

### 3. 异步事件：Kafka

`publish()` 只做一次入队，后台任务攒批 `send_and_wait`；事件以 `api_key` 作为分区 key
（同一 key 落同一分区，天然有序）。事件信封：

```json
{"event": "request.completed", "ts": 1791041268.21, "ts_iso": "2026-10-03T23:27:48",
 "instance": "gateway-8000", "host": "dawson-RESCUER-R720-15IKBN",
 "data": {"api_key": "sk-test-001", "tier": "free", "model": "qwen2.5:0.5b",
          "status": 200, "stream": true, "wall_time": 0.412, "ttft": 0.017, "tps": 74.01,
          "prompt_tokens": 26, "completion_tokens": 4, "num_ctx": "512",
          "num_gpu": "default", "num_batch": "default", "rate_limit_backend": "redis"}}
```

| 事件类型 | 触发点 |
|----------|--------|
| `request.completed` | 2xx 正常完成（流式在结束分片后补记） |
| `request.rate_limited` | 限流 429 |
| `request.auth_failed` | API Key 无效 401 |
| `request.error` | 请求体非法 / `options` 校验失败 / 上游 Ollama 错误 |

broker 不可用时降级写 `logs/events.jsonl`（每条多一个 `fallback_reason` 字段），
后台每 10s 重连，恢复后自动继续发 Kafka。

命令行自查（消费端就是个普通 Kafka 客户端，网关不关心谁在消费）：

```bash
~/opt/infra/kafka/kafka_2.13-4.3.1/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic llm-gateway-events --from-beginning --max-messages 5
```

### 4. 降级验证（实机结果）

`bash scripts/infra.sh start` 起好三件套后，把服务打挂实测：

| 场景 | 操作 | 结果 |
|------|------|------|
| Kafka 挂掉 | `kill $(cat ~/opt/infra/run/kafka.pid)` 后发请求 | 请求仍 200；`/infra` 里 `events.connected=false`、`fallback_written` +1；`logs/events.jsonl` 出现带 `fallback_reason` 的事件 |
| Kafka 恢复 | `bash scripts/infra.sh start` | 10s 内自动重连（`connected=true`），后续事件继续进 Kafka（`published` 持续增长） |
| Redis 挂掉 | `kill $(cat ~/opt/infra/run/redis.pid)` 后发请求 | 请求仍 200（fail-open）；`/health` 的 `rate_limit_backend` 变为 `memory`，`gateway_ratelimit_redis_errors_total` +1 |
| Redis 恢复 | `bash scripts/infra.sh start` | 5s 内自动重连，`redis_up=true`，重新使用共享计数 |

### 5. 一键验收

```bash
python verify_infra.py                     # 默认 SQLite + Redis + Kafka，21 项检查
python verify_infra.py --database-url postgresql://gw:gw@localhost:5433/gateway
python verify_infra.py --skip-kafka        # 只验限流与历史
```

脚本会自己拉起两个网关实例（`:8000` / `:8001`）跑上面那张对照表，再检查 `/history`、
`/history/stats`、直连数据库的行数、Kafka 事件类型分布，最后打印 `验收结果: 21/21 项通过`。
（脚本读 `DATABASE_URL` / `REDIS_URL` / `KAFKA_BOOTSTRAP` 环境变量，也可用命令行参数覆盖。）

## 多实例部署 + nginx 负载均衡

单实例撑不住时，网关可以直接水平扩——**进程本身无状态**，状态都在外部组件里：
限流额度在 Redis、请求历史在 PostgreSQL、事件在 Kafka，所以多起几个进程就是多份并发。

```
                        ┌───────────────────┐
   客户端 ───────────▶  │   nginx  :8080    │  least_conn + 被动健康检查
                        └─────────┬─────────┘
              ┌───────────────────┼───────────────────┐
        ┌─────▼─────┐       ┌─────▼─────┐       ┌─────▼─────┐
        │  :8000    │       │  :8001    │       │  :8002    │  同一份代码，
        │gateway-8000      │gateway-8001      │gateway-8002     仅端口/实例名/日志不同
        └─────┬─────┘       └─────┬─────┘       └─────┬─────┘
              └───────────────────┼───────────────────┘
        ┌──────────────┐  ┌───────▼────────┐  ┌──────────────┐
        │ Redis 限流   │  │ PostgreSQL 历史 │  │ Kafka 事件   │
        └──────────────┘  └────────────────┘  └──────────────┘
              ▲ 每个实例各自暴露 /metrics —— Prometheus 直连抓取（不经过 nginx）
```

### 一键起停

```bash
bash scripts/nginx.sh install          # 首次：免 root 解包 nginx 到 ~/opt/nginx
bash scripts/cluster.sh start          # 起 3 个实例(8000/8001/8002) 并接上 nginx(8080)
bash scripts/cluster.sh status         # 实例健康 + nginx 上游 + 经 LB 的分发采样
bash scripts/cluster.sh scale 5        # 扩到 5 个：先起新实例，再 reload nginx
bash scripts/cluster.sh scale 3        # 缩容：先改上游并 reload，再优雅停实例
bash scripts/cluster.sh logs           # 各实例日志 + nginx 访问日志
bash scripts/cluster.sh stop
```

统一入口 `http://localhost:8080`（`/lb-health` 是 nginx 自己的健康端点）。
`curl -i` 看响应头 `x-gateway-instance: gateway-8001` 就知道这次落在哪个实例。

### 观测：多实例怎么抓

每个实例各自暴露 `/metrics`，Prometheus **直连各实例**（同一个 job 多 target，Grafana 看板无需改动）：

```bash
GATEWAY_TARGETS=localhost:8000,localhost:8001,localhost:8002 bash scripts/monitoring.sh start
GATEWAY_TARGETS=localhost:8000,localhost:8001,localhost:8002 bash scripts/monitoring.sh reload
bash scripts/monitoring.sh targets     # 只看渲染出的抓取配置与 target 状态
```

- 经 nginx 抓 `/metrics` 会被 **明确拒绝（404）**：否则所有实例的样本 `instance` 标签都会变成
  LB 地址，分不清是哪个实例、也看不出「某个实例挂了」；
- 限流是跨实例共享的：free 档经 LB 连发 12 次，3 个实例合计只放行 10 次。

### 验收

```bash
python verify_cluster.py               # 34 项检查，约 1 分钟
```

脚本临时拉起 3 个实例（`:8100~:8102`）+ 一个独立前缀的 nginx（`:8180`，不影响你跑的 8080），
验证分发、SSE 未被缓冲、共享限流、多实例写历史、**杀一个实例看自动摘除与恢复**、观测隔离等。

### nginx 配置要点（`nginx/nginx.conf.tpl`）

| 指令 | 值 | 为什么 |
|------|-----|--------|
| `least_conn` | — | LLM 单请求耗时长，按当前连接数分发比轮询更均衡 |
| `max_fails` / `fail_timeout` | `2` / `10s` | 实例挂了自动摘除，10s 后再试；恢复即自动加回 |
| `proxy_buffering` | `off` | **SSE 必须关**，否则逐字输出变成「憋几秒一次吐出」 |
| `proxy_http_version` + `Connection ""` + `keepalive 32` | 1.1 / 空 | 三行齐写才能与上游复用长连接 |
| `proxy_read_timeout` | `300s` | 默认 60s 会把长回答掐断 |
| `proxy_connect_timeout` | `2s` | 死实例快速失败，交给下一个上游 |
| `proxy_next_upstream` | `error timeout` | 连接阶段失败才换机；POST 非幂等默认不重试，避免重复生成 |

配置由 `scripts/nginx.sh` 从模板渲染（端口 / 上游列表 / 日志目录），改完用
`bash scripts/cluster.sh scale <N>` 或 `bash scripts/nginx.sh reload` 平滑生效（不中断连接）。

### 实测结果（本机 GTX 1050 Ti）

| 验证项 | 结果 |
|--------|------|
| 12 次请求经 LB 的分发 | `{8000: 4, 8001: 4, 8002: 4}` |
| 9 次真实请求后 Prometheus 各实例计数 | `localhost:8001=4, localhost:8000=4, localhost:8002=3` |
| 流式经 LB 首字节 / 总时长 | 0.033s / 0.724s（直连 0.033s / 0.155s → 未被缓冲） |
| 杀掉一个实例 | 客户端仍全部 200；访问日志 `"upstream_addr":"127.0.0.1:8100, 127.0.0.1:8101"`、`"upstream_status":"502, 200"` 即自动换机；`nginx-error.log` 记录 `connect() failed` / `temporarily disabled` |
| 实例恢复 | 10s 内（`fail_timeout`）自动加回上游，无需 reload |
| free 档经 LB 连发 12 次 | `200×10 + 429×2`（3 个实例共享额度） |
| `verify_cluster.py` | `34/34 项通过` |

> 单机多实例扩的是「网关层」的并发与可用性；真正的瓶颈通常在 Ollama（只有一块 GPU，
> 请求最终仍排同一个队列）。把 `OLLAMA_URL` 指向多台 Ollama / 再加一层调度，
> 才能让水平扩容真正生效——**负载均衡能消除瓶颈，不能创造算力**。

## Ollama 参数调优实验（GTX 1050 Ti）

`tuning_experiment.py` 经网关对 `num_ctx` / `num_gpu` / `num_batch` 做受控扫描。
每组配置 **1 次 warmup（冷加载，不计入统计）+ 3 次测量**；固定 208 token 的英文
prompt、`num_predict=64`、`temperature=0`，记录端到端 TTFT、网关 TTFT、tokens/s
与 `nvidia-smi` 显存。

```bash
python tuning_experiment.py                 # ctx + gpu + batch 全扫（52 请求，约 2 分钟）
python tuning_experiment.py --sweeps ctx    # 只扫 num_ctx；--dry-run 只看计划
```

- 明细 `logs/tuning_experiment.jsonl`，汇总 `logs/tuning_experiment_summary.json`。
- 实验期间网关把 TTFT/TPS 按 `num_ctx`/`num_gpu`/`num_batch` 标签暴露给 Prometheus，
  仪表盘「Ollama 参数调优」行实时画 6 条曲线；面板查询窗口为 `[3m]`，实验结束后
  曲线还会保留约 3 分钟，想回看更早的实验把时间范围与查询窗口调大即可。
- 每组换参都会触发 Ollama 重载模型（冷启动 TTFT ≈ 2.1s），Prometheus 曲线上的
  尖峰就是重载成本，统计汇总（summary）里已剔除 warmup。

### 实测结果（qwen2.5:0.5b，GTX 1050 Ti 4GB，52 请求）

| 配置 | warm TTFT (s) | tokens/s | 显存 (MiB) |
|------|--------------:|---------:|-----------:|
| num_ctx=512 | 0.014 | 192.9 | 472 |
| num_ctx=1024 | 0.017 | 184.3 | 478 |
| num_ctx=2048 | 0.022 | 182.0 | 490 |
| num_ctx=4096 | 0.013 | 183.9 | 516 |
| num_ctx=8192 | 0.014 | 171.7 | 568 |
| num_gpu=0（纯 CPU） | 0.079 | 71.2 | 2 |
| num_gpu=12（24 层中的半层） | 0.024 | 90.5 | 354 |
| num_gpu=-1（全量 GPU） | 0.016 | 176.3 | 490 |
| num_batch=64 | 0.013 | 188.0 | 460 |
| num_batch=128 | 0.013 | 178.3 | 464 |
| num_batch=256 | 0.014 | 169.8 | 472 |
| num_batch=512 | 0.015 | 186.1 | 490 |
| num_batch=1024 | 0.014 | 192.8 | 526 |

结论：

- **num_ctx**：短 prompt 下 warm TTFT / TPS 基本无感，显存随上下文线性增长
  （512→8192 约 +96 MiB ≈ 12.8 KiB/token 的 KV cache）。长对话才需要大 `num_ctx`，
  否则白白多占显存。
- **num_gpu**：0.5b 小模型要么全放 GPU 要么别放——全量 offload 176 tok/s，
  半层（12/24）只有 90（GPU+CPU 混合反而多一次搬运），纯 CPU 71。
- **num_batch**：64→1024 对 decode 速度无单调影响（差异在测量噪声内），
  但显存随 batch 增大（460→526 MiB）；保持默认 512 即可。
- **换参即重载**：`num_ctx`/`num_gpu`/`num_batch` 任一变化都会让 Ollama 重载模型，
  冷启动 TTFT ≈ 2.1s——生产流量高峰期不要动态换参。

已知坑（脚本已规避）：

1. **Ollama 0.35 的 `num_gpu=-1` 不触发重载**：从显式 `num_gpu=0/12` 切回 `-1`
   时 Ollama 判定「配置未变」，不换 runner（load=2ms、offload 状态原样保留）。
   `unload_model()` 在每组开始前先 `keep_alive=0` 强制卸载来规避。
2. prompt 长度超过 `num_ctx` 会被截断：脚本固定 208 token，最小扫到 512
   （含模板与 64 个生成 token），越界会打印警告。

## 说明

- 限流为进程内存实现（每 key 每分钟），生产环境建议改用 Redis。
- 请求明细（`ttft`、`tps`、token 数、GPU 利用率/显存）会追加写入 `logs/gateway.log`。
- GPU 指标通过 `nvidia-smi` 获取，不可用时返回 0。
- Prometheus 数据保留在 `$MON_HOME/prometheus/data`，Grafana 数据在 `$MON_HOME/grafana/data`。
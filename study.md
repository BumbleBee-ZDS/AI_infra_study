# 从零上手 LLM Gateway —— 小白学习手册

> 配套文档：`README.md`（速查/参考）、`study.md`（本文件，逐步教学）。
> 本手册假设你**完全没有分布式基础设施经验**，所有名词第一次出现都会先解释，
> 每一章都有「概念 → 动手 → 验收 → 踩坑」四段。
>
> 本机实测环境（写文档时的真实状态）：
> Ubuntu 26.04 · Python 3.12.14（conda 环境 `aiinfra`）· Ollama 0.35.0 ·
> GTX 1050 Ti 4GB · OpenJDK 25 · 已拉取 `qwen2.5:0.5b` / `qwen2.5:1.5b` / `llama3.2:1b`。

---

## 目录

| 章节 | 内容 | 大约阅读/动手时间 |
|------|------|------------------|
| 0 | 先看全局：架构、端口、学习路线 | 15 分钟 |
| 1 | 环境准备与第一次跑通（含 4 种错误码） | 40 分钟 |
| 2 | 读懂主链路 `main.py` | 60 分钟 |
| 3 | 流式输出（SSE）与首 token 延迟 | 40 分钟 |
| 4 | 指标与监控（Prometheus + Grafana） | 90 分钟 |
| 5 | 分布式限流（Redis + Lua + 双实例对照） | 90 分钟 |
| 6 | 请求历史（SQLite / PostgreSQL） | 60 分钟 |
| 7 | Kafka 异步事件 | 60 分钟 |
| 8 | 降级演练（最值得学的一章） | 60 分钟 |
| 9 | 自动化验收、压测、参数调优 | 60 分钟 |
| 10 | 12 道进阶动手题 | 按需 |
| 附录 A | 命令速查 | 随时查 |
| 附录 B | 文件地图（改代码前先看） | 随时查 |
| 附录 C | 报错对照表 | 随时查 |
| 附录 D | 术语表 | 随时查 |
| 附录 E | 自测题 15 问 + 打卡表 | 复盘用 |

---

## 0. 先看全局：这个项目到底在做什么

### 0.1 一句话

它是一个 **LLM 网关（Gateway）**：把你的请求统一收口，做「鉴权 → 限流 → 模型路由 →
转发给 Ollama → 记录指标/日志/历史/事件」。
对外的接口长得跟 OpenAI 一模一样（`POST /v1/chat/completions`），所以任何支持 OpenAI 的
客户端只要改一个 `base_url` 就能接上。

### 0.2 为什么要「网关」这一层

直接把 Ollama 暴露出去会有四个问题，网关就是逐个解决它们的：

| 问题 | 不做的后果 | 本项目怎么解决 | 相关文件 |
|------|-----------|----------------|----------|
| 谁都能调？ | 无法区分用户、无法计量 | `x-api-key` 头校验，映射到 tier（档位） | `auth.py` |
| 一个用户刷爆 GPU？ | 别人全被拖死 | 按 key + 档位限流；多实例时用 Redis 共享额度 | `ratelimit.py` |
| 模型名写死难换？ | 客户端改代码 | 别名路由 `qwen0.5b → qwen2.5:0.5b` | `router.py` |
| 出了问题查不到？ | 无法排障、无法做容量规划 | 指标（Prometheus）+ 明细日志（JSONL）+ 历史库（SQLite/PG）+ 事件（Kafka） | `prometheus_metrics.py` / `logger.py` / `storage.py` / `events.py` |

### 0.3 整体架构图

```
                  ┌──────────────────────────────────────────────┐
  客户端 curl ───► │            FastAPI 网关 (main.py)            │
                  │                                              │
                  │ ① 鉴权 auth.py       → 失败 401               │
                  │ ② 限流 ratelimit.py  → 超限 429               │
                  │ ③ 参数校验/路由 router.py → 非法 400           │
                  │ ④ 转发 httpx ─────────────────┐              │
                  │ ⑤ 记录指标/日志/历史/事件      │              │
                  └───────────────────────────────┼──────────────┘
                                                  ▼
                                        ┌──────────────────┐
                                        │  Ollama :11434   │
                                        │ qwen2.5:0.5b ... │
                                        └──────────────────┘

  基础设施（每一件都能独立降级，缺了也不影响推理）：
    ② 限流计数 ──► Redis      :6379   （挂了 → 进程内计数兜底）
    ⑤ 历史明细 ──► SQLite/PG  :5433   （挂了 → 只丢历史，请求照过）
    ⑤ 事件流   ──► Kafka      :9092   （挂了 → 降级写 logs/events.jsonl）

  观测：
    Prometheus :9090  ──抓取──► 网关 /metrics
    Grafana    :3000  ──查询──► Prometheus（5 行仪表盘）
```

### 0.4 端口 / 目录速查

| 服务 | 地址 | 启动脚本 | 数据目录 |
|------|------|----------|----------|
| 网关 | http://localhost:8000 | `python main.py` | `logs/` |
| Ollama | http://localhost:11434 | 系统服务 `ollama serve` | `~/.ollama` |
| Prometheus | http://localhost:9090 | `bash scripts/monitoring.sh start` | `~/opt/monitoring/prometheus/data` |
| Grafana | http://localhost:3000 | 同上 | `~/opt/monitoring/grafana/data` |
| Redis | localhost:6379 | `bash scripts/infra.sh start` | 无（`--save ''` 不落盘） |
| PostgreSQL | localhost:5433 | 同上 | `~/opt/infra/pgdata` |
| Kafka | localhost:9092（controller 9093） | 同上 | `~/opt/infra/kafka-data` |

### 0.5 学习路线（建议按顺序做，每阶段 30~90 分钟）

| 阶段 | 学什么 | 产出/验收 |
|------|--------|-----------|
| 1 | 环境准备、跑通第一条请求 | `curl` 拿到 200，`logs/gateway.log` 有记录 |
| 2 | 读懂 `main.py` 主链路 | 能画出 5 步流程；能改 `DEFAULT_MODEL` 生效 |
| 3 | 流式 SSE | `curl -N` 看到逐字输出 + `[DONE]` |
| 4 | 指标与监控 | `/metrics` 看到 `gateway_*`；Grafana 5 行看板有曲线 |
| 5 | 分布式限流（Redis） | 两台实例共享 10 次额度，合计只放行 10 |
| 6 | 请求历史（SQLite/PG） | `/history` 有你的请求；会直接查库 |
| 7 | Kafka 事件 | 命令行消费者能看到 4 类事件 |
| 8 | 降级演练 | 逐个 kill 服务，请求仍然 200 |
| 9 | 自动化验收 + 压测/调优 | `verify_infra.py` 21/21 通过 |
| 10 | 进阶改造题 | 自己动手做 3 个以上练习 |

**重要心态**：这个项目里最值钱的不是「把 Redis 跑起来」，而是
**「每一件基础设施都能独立降级，推理主链路永不被拖死」** 这条设计原则。
每读一个模块都问自己一句：*它挂了会怎样？*（第 8 章会集中演练）

---

## 1. 阶段 1：环境准备与第一次跑通

### 1.1 本章目标

- 确认「Python 环境 / Ollama / 模型」三件事就绪；
- 用一条 `curl` 打通网关 → Ollama 的通路；
- 知道出问题时该看哪个日志。

### 1.2 概念补课（30 秒版）

- **Ollama**：本地跑大模型的工具，装好后它会在 `localhost:11434` 开一个 HTTP 服务。
  你 `ollama pull qwen2.5:0.5b` 就是把模型文件下载到本地。
- **FastAPI**：Python 的 Web 框架，本项目的网关就是它写的；`uvicorn` 是跑它的服务器。
- **tier（档位）**：本项目把 API Key 映射到档位，档位决定限流配额
  （`sk-test-001` → `free` 档 10 次/分钟，`sk-prod-001` → `pro` 档 60 次/分钟）。

### 1.3 动手：环境自查

```bash
cd /home/dawson/桌面/code/python/ai_gateway

# 1) Python 与 conda 环境（本项目用 aiinfra 环境，Python 3.12）
python -V                     # 期望 Python 3.12.x
echo $CONDA_DEFAULT_ENV       # 期望 aiinfra

# 2) Ollama 是否在跑 + 有哪些模型
curl -s http://localhost:11434/api/tags | head -c 200
ollama list                   # 期望看到 qwen2.5:0.5b 等

# 3) 安装项目依赖（只做一次）
pip install -r requirements.txt

# 4) 确认 8000 端口没被别的程序占用
ss -ltn | grep 8000 || echo "8000 空闲，可以用"
```

`requirements.txt` 里的依赖各自负责什么：

| 依赖 | 作用 |
|------|------|
| `fastapi` / `uvicorn` | Web 框架 + 服务器 |
| `httpx` | 异步 HTTP 客户端（网关转发给 Ollama 用它） |
| `prometheus-client` | 生成 `/metrics` 指标 |
| `redis` | 连 Redis 做分布式限流 |
| `aiokafka` | 异步 Kafka 生产者 |
| `psycopg[binary]` | 连 PostgreSQL（不用 PG 时可以不装，代码里是延迟导入） |
| `requests` | 只被测试/实验脚本用（`verify_infra.py`、`tuning_experiment.py`） |

### 1.4 动手：启动网关（三种方式）

```bash
# 方式一：最朴素（读 SERVER_HOST / SERVER_PORT，默认 0.0.0.0:8000）
python main.py

# 方式二：开发热重载（改代码自动重启）
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 方式三：接上三件基础设施（第 5~7 章再学，先知道有这条命令）
bash scripts/infra.sh install      # 首次：免 root 下载解包 Redis/PG/Kafka
bash scripts/infra.sh start
REDIS_URL=redis://localhost:6379/0 \
DATABASE_URL=postgresql://gw:gw@localhost:5433/gateway \
KAFKA_BOOTSTRAP=localhost:9092 \
python main.py
```

> 建议开两个终端：一个跑网关看输出，一个发请求。
> 网关启动时会打印「实际生效的后端」（以下为本机真实输出）：
> `限流后端: Redis（redis://localhost:6379/0，窗口 60s，多实例共享计数）`
> `请求历史: postgresql://gw:***@localhost:5433/gateway（批量 20 行 / 1.0s）`
> `事件总线: Kafka localhost:9092 -> topic llm-gateway-events`
> 任一后端连不上时会打印警告并降级，例如
> `限流后端: Redis 不可用（...），改用进程内计数（fail-open）`、
> `请求历史不可用（...），/history 将返回空并继续计数丢弃`、
> `Kafka 发送失败，降级写本地文件: ...`。

### 1.5 动手：发第一条请求

```bash
curl -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"用一句话解释什么是网关"}]}'
```

期望返回（OpenAI 兼容格式）：

```json
{"id":"chatcmpl-1759500000000","object":"chat.completion","model":"qwen2.5:0.5b",
 "usage":{"prompt_tokens":26,"completion_tokens":30,"total_tokens":56},
 "choices":[{"index":0,"message":{"role":"assistant","content":"..."},"finish_reason":"stop"}]}
```

顺手看三处「留痕」：

```bash
# ① 明细日志（每行一个 JSON）
tail -n 1 logs/gateway.log

# ② 健康状态（一眼看出三件基础设施是否在正常工作）
curl -s http://localhost:8000/health

# ③ 请求历史（需要网关接上 SQLite/PG；见第 6 章）
curl -s 'http://localhost:8000/history?limit=1'
```

`/health` 的返回可以这样读：

```json
{"status":"ok",              // ok = Ollama 通；degraded = Ollama 不通
 "ollama":"up",
 "instance":"gateway-8000",  // 实例标识（环境变量 GATEWAY_INSTANCE）
 "rate_limit_backend":"redis",   // redis=共享计数 / memory=已降级
 "history_backend":"postgres",   // postgres / sqlite / none
 "kafka_connected":true}
```

### 1.6 ✅ 阶段 1 验收清单

- [ ] `python -V` 是 3.12.x，`CONDA_DEFAULT_ENV=aiinfra`
- [ ] `ollama list` 能列出 `qwen2.5:0.5b`
- [ ] `curl` 那条命令返回 200 且 `choices[0].message.content` 非空
- [ ] `tail -n 1 logs/gateway.log` 能看到刚发的请求
- [ ] `curl /health` 里 `ollama` 是 `up`

### 1.7 常见坑

| 现象 | 原因 | 处理 |
|------|------|------|
| `422 Unprocessable Entity` | 没带 `x-api-key` 头（`Header(...)` 是必填） | 加上 `-H 'x-api-key: sk-test-001'` |
| `401 Invalid API Key` | key 不在 `config.API_KEYS` 里 | 用 `sk-test-001` / `sk-prod-001` |
| `502 Ollama unreachable` | Ollama 没起，或 `OLLAMA_URL` 写错 | `ollama serve`；或改 `OLLAMA_URL` 环境变量 |
| 请求很久没反应 | 首次请求要冷加载模型（GTX 1050 Ti 上 0.5b 约 2 秒） | 正常，等；多问几次就热了 |
| `Address already in use` | 8000 已被占用（比如你之前启的网关没关） | `ss -ltnp \| grep 8000` 找到进程再决定杀掉或用 `SERVER_PORT=8002` |

---

## 2. 阶段 2：读懂主链路 `main.py`

### 2.1 本章目标

- 能画出「鉴权 → 限流 → 解析 → 转发 → 留痕」这条链路，并说出每步在代码哪一行；
- 理解 FastAPI 的 `lifespan`（启动/关闭钩子）为什么重要；
- 亲手触发 4 种错误码（422 / 401 / 429 / 400），建立「错误码 ↔ 代码位置」的对应关系。

### 2.2 应用的「启动与关闭」：`lifespan`（main.py:72~88）

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_gpu_sampler())   # 后台任务：每 5s 采一次 GPU
    await rate_limiter.start()   # 连 Redis（连不上→进程内计数）
    await history.start()        # 建表/连库（失败→历史功能自动关闭）
    await events.start()         # 连 Kafka（连不上→降级写 JSONL）
    try:
        yield                    # ← 这里开始对外服务
    finally:
        task.cancel()
        await events.close()     # 关闭顺序有讲究 ↓
        await history.close()
        await rate_limiter.close()
```

三个要点，都是分布式系统的通用套路：

1. **启动时"预热"**：连接、建表、开后台任务都在 `yield` 之前完成，请求进来时已经就绪；
2. **启动失败不能崩**：三个 `start()` 内部全部自己 try/except，任何一个失败都只打印警告并降级，
   绝不 `raise`（否则网关起不来，推理也跟着没了）；
3. **关闭顺序**：先把队列里积压的事件发完/落盘（`events.close`），再停历史写入，
   最后断开 Redis。这样保证「退出时不丢数据」。

> **名词：协程（coroutine）/ async-await**
> `async def` 定义的函数不会立刻执行，要 `await` 它才开始跑；`await` 期间事件循环可以去处理别的请求。
> 网关里所有 I/O（HTTP 转发、Redis、Kafka、写库）都走这套，所以单进程能同时处理多个请求。

### 2.3 模块级单例（main.py:93~112）

```python
rate_limiter = RateLimiter(url=REDIS_URL, window=RATE_LIMIT_WINDOW, backend=RATE_LIMIT_BACKEND,
                           timeout=REDIS_TIMEOUT, fail_open=REDIS_FAIL_OPEN)
history = RequestHistory(DATABASE_URL, batch_size=..., flush_interval=..., queue_size=...)
events = EventBus()
```

三个对象都在模块顶层创建，参数**全部来自 `config.py`**（而 `config.py` 全部来自环境变量）。
这解释了为什么改配置不用改代码：`DATABASE_URL=... python main.py` 就能换后端。

### 2.4 一次请求的完整链路

```
POST /v1/chat/completions          main.py:294  @app.post(...)
  │
  ├─① 鉴权      verify_api_key(x_api_key)  main.py:300~309 / auth.py
  │             key 不在 API_KEYS → 401 + 指标 + 事件 request.auth_failed
  │
  ├─② 限流      await rate_limiter.check(key, tier, RATE_LIMIT[tier])  main.py:311~325
  │             写响应头 X-RateLimit-Limit/Remaining/Reset
  │             不允许 → 429 + 指标 + 事件 request.rate_limited
  │
  ├─③ 解析      body = await request.json()        main.py:326~333  非法 JSON → 400
  │             model = resolve_model(...)          router.py
  │             options, config = _parse_options()  main.py:164~197  白名单校验 → 400
  │
  ├─④ 转发      stream=True  → _stream_chat()       main.py:199~292 / 361~369
  │             stream=False → httpx.post()         main.py:371~397
  │
  └─⑤ 留痕      _persist(record, event)             main.py:115~124 / 404~420
                → log_request()  写 logs/gateway.log（JSONL）
                → history.submit() 入队，后台批量写库
                → events.publish() 入队，后台批量发 Kafka
```

### 2.5 `_persist`：一次请求三处留痕（main.py:115）

```python
def _persist(record: dict, event: str = "request.completed") -> None:
    log_request(record)          # ① 文件：谁都能看，最简单
    history.submit(record)       # ② 库：可聚合查询（/history/stats）
    events.publish(event, record)  # ③ 事件流：给下游系统消费（Kafka）
```

三个调用**都只做入队、不做等待**（`submit`/`publish` 是非阻塞的，内部队列满就丢弃并计数）。
这是本章最重要的一句话：

> **观测数据（日志/历史/事件）永远不能反压业务主链路。**
> 宁可丢掉一条历史，也不能让用户多等 100ms。

### 2.6 动手实验：触发 4 种错误码

```bash
# ① 422：压根没带 header（Header(...) 必填，FastAPI 自动拦）
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -d '{"messages":[]}'

# ② 401：key 是假的
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: wrong-key' -d '{"messages":[]}'

# ③ 400：options 里塞了不认识/越界的参数
curl -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"temperature":9}}'

# ④ 429：用 free 档连发 12 次（配额 10 次/分钟）
for i in $(seq 1 12); do
  curl -s -o /dev/null -w "%{http_code} " -X POST http://localhost:8000/v1/chat/completions \
    -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
    -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"num_predict":2}}'
done; echo
```

期望：`422 / 401 / 400 / 200×10 429×2`（也可能是 200×10 后全是 429）。

然后观察「每种错误都进了哪里」：

```bash
# 429 的响应头里带限流信息
curl -s -D - -o /dev/null -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[]}' | grep -i 'ratelimit\|retry-after'
```

### 2.7 顺带认识两个 FastAPI 概念

| 概念 | 在本项目的用法 | 说明 |
|------|----------------|------|
| `Header(...)` | `x_api_key: str = Header(...)`（main.py:298） | 声明「这个 HTTP 头是必填参数」，缺了 FastAPI 自动返回 422；`...` 表示必填 |
| `Response` 对象 | `response.headers.update(rl.headers())`（main.py:313） | 往里写响应头（限流信息）；`StreamingResponse` 则用 `headers=` 传 |
| `HTTPException` | 401/429/400 都用它抛出 | `raise HTTPException(status_code=429, detail=..., headers=...)` |
| 自动文档 | 启动后访问 http://localhost:8000/docs | FastAPI 自动生成的交互式 API 文档，可直接点着试，**小白友好度最高** |

### 2.8 ✅ 阶段 2 验收清单

- [ ] 打开 http://localhost:8000/docs，能用「Try it out」发出成功请求
- [ ] 能不看文档说出 `status=200` 的非流式请求依次经过了哪 5 步
- [ ] 亲手复现 422 / 401 / 400 / 429 四个码，并说出各由哪一行代码产生
- [ ] 能解释为什么 `_persist` 里三个调用都不用 `await`（提示：它们只入队）

### 2.9 小实验（选做）

把 `DEFAULT_MODEL` 换成别的别名，验证「不传 model 也能用」：

```bash
DEFAULT_MODEL=llama1b python main.py
curl -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
  -d '{"messages":[{"role":"user","content":"hi"}]}' | head -c 200
# 返回里 "model" 应该变成 llama3.2:1b（说明 router.resolve_model 生效）
```

---

## 3. 阶段 3：流式输出（SSE）与「首 token 延迟」

### 3.1 本章目标

- 明白为什么 LLM 必须用「流式」；
- 看懂 Ollama 的 NDJSON 与 OpenAI 的 SSE 两种格式，以及网关做的转换；
- 亲手用 `curl -N` 观察逐字输出，并读懂 `include_usage` 分片。

### 3.2 概念：两种流格式

| | Ollama 原生 | OpenAI 兼容（网关对外） |
|---|---|---|
| 端点 | `POST /api/chat` | `POST /v1/chat/completions` |
| 传输 | 一行一个 JSON（NDJSON） | `text/event-stream`，每行 `data: {...}`，空行分隔 |
| 结束 | `{"done":true,...}` | `data: [DONE]` |

所以网关的 `_stream_chat()`（main.py:199）本质是一个**格式转换器**：
一边读 Ollama 的 NDJSON，一边把每个分片翻译成 OpenAI 的 chunk 吐给客户端。

**为什么要流式**：LLM 是逐个 token 生成的。非流式要等全部生成完才返回
（用户盯着空白屏幕好几秒）；流式则每个 token 一出来就发走，体感「秒回」。
这也是为什么监控里要单独看 **TTFT（Time To First Token，首 token 延迟）**——
它才是用户感知的「快慢」。

### 3.3 代码走读（main.py:199~292）

```python
async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
    async with client.stream("POST", f"{OLLAMA_URL}/api/chat", json=ollama_payload) as resp:
        async for line in resp.aiter_lines():          # 逐行读 NDJSON
            chunk = json.loads(line)
            content = chunk.get("message", {}).get("content", "")
            if content:
                yield _sse(_stream_chunk(completion_id, model, content))  # 翻译成 SSE
            if chunk.get("done"):
                final_chunk = chunk                   # 记住最后一片，用来算指标
                break
yield _sse(_stream_chunk(completion_id, model, "", finish_reason="stop"))
yield "data: [DONE]\n\n"
```

`finally` 块（main.py:273~291）里做善后：无论成功失败，
都要 `INFLIGHT.dec()`、记指标、`_persist(...)` 落三处留痕。
**流式请求也是先返回数据、后记账**，因为 `StreamingResponse` 是边生成边发。

### 3.4 动手：看一条真实的流

```bash
curl -N -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-prod-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"数三个数"}],
       "stream":true,"stream_options":{"include_usage":true}}'
```

预期输出（节选）：

```
data: {"id":"chatcmpl-...","object":"chat.completion.chunk","model":"qwen2.5:0.5b","choices":[{"index":0,"delta":{"content":"1"},"finish_reason":null}]}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk",...,"delta":{"content":"，"},...}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":26,"completion_tokens":8,"total_tokens":34},"metrics":{"ttft":0.017,"tps":88.4}}

data: [DONE]
```

注意那个 `"choices":[]` 的分片——它是网关自己加的（`include_usage=true` 时），
里面既符合 OpenAI 的 usage 约定，又多了 `metrics.ttft/tps`，
**第 9 章的调优实验脚本就是靠它采集数据**。

### 3.5 名词补充

- **NDJSON**：Newline Delimited JSON，一行一个 JSON 对象。好处是可以边收边解析。
- **SSE（Server-Sent Events）**：HTTP 上的「服务器单向推流」标准，格式固定为
  `data: <内容>` + 空行。浏览器 `EventSource` / OpenAI SDK 都懂。
- **TTFT**：`load_duration + prompt_eval_duration`（Ollama 上报的纳秒值 /1e9），
  见 `metrics.py:compute_metrics`。**TPS** 则是 `eval_count / eval_duration`。
- **INFLIGHT**：当前正在处理的请求数（Gauge 指标），流式请求在生成期间一直占着。

### 3.6 ✅ 阶段 3 验收清单

- [ ] `curl -N` 能看到一行行 `data: {...}`，最后是 `data: [DONE]`
- [ ] 能说出 `finish_reason":"stop"` 的分片和 `usage` 分片的区别
- [ ] 说得出 TTFT 与「总耗时」为什么是两个不同指标
- [ ] 用 `curl -N ... | head -5` 对比「有流式」和「`"stream":false`」的等待感受差异

---

## 4. 阶段 4：指标与监控（Prometheus + Grafana）

### 4.1 本章目标

- 分清 Counter / Gauge / Histogram 三种指标，知道本项目各用在哪；
- 会用 `curl /metrics` 和 PromQL 查数据；
- 把 Prometheus + Grafana 跑起来，看懂 5 行仪表盘。

### 4.2 概念：为什么要「指标」而不只是「日志」

日志是**一条条明细**（谁、什么时候、干了什么），量大了存不下也查不动；
指标是**聚合后的数字**（每秒多少请求、P95 延迟多少、错误率多少），
便宜、可长期保存、可画图、可告警。两者互补，本项目两者都有：

- 明细：`logs/gateway.log`（JSONL）+ 历史库 `request_history` 表；
- 指标：`/metrics` → Prometheus → Grafana。

**Pull 模型**：Prometheus 主动每隔几秒来「抓」一次 `/metrics`
（配置见 `monitoring/prometheus.yml`，`scrape_interval: 5s`）。
所以网关不需要知道 Prometheus 在哪，它只负责「把当前数字摆出来」。

### 4.3 三种指标类型（`prometheus_metrics.py`）

| 类型 | 语义 | 只能… | 本项目例子 |
|------|------|-------|-----------|
| **Counter** | 只增不减的累计数 | `.inc(n)` | `gateway_requests_total`、`gateway_tokens_total`、`gateway_events_published_total` |
| **Gauge** | 可增可减的瞬时值 | `.set(v)` / `.inc()/.dec()` | `gateway_inflight_requests`、`gateway_gpu_utilization_percent`、`gateway_kafka_up` |
| **Histogram** | 分布（分桶统计） | `.observe(v)` | `gateway_request_duration_seconds`、`gateway_ttft_seconds`、`gateway_tokens_per_second` |

Histogram 的小知识：`observe(0.42)` 之后，你在 `/metrics` 里会看到三条相关序列——

```
gateway_request_duration_seconds_bucket{le="0.5"} 12   ← 累积到各桶的计数
gateway_request_duration_seconds_sum 5.2               ← 所有观测值之和
gateway_request_duration_seconds_count 12              ← 观测次数
```

有了 `bucket` 就能算任意分位数（P50/P95/P99），这是「平均值会骗人」的解药：
平均 0.4s 但 P99 是 30s，说明有少量用户被拖得很惨，平均值看不出来。

**标签（label）**：`gateway_requests_total{model="qwen2.5:0.5b",tier="free",status="200",stream="false"}`。
同一个指标名 + 不同标签 = 不同时间序列。代价是**标签基数爆炸**风险
（比如把 `api_key` 当标签会让序列数爆炸），所以本项目只用 model/tier/status/stream 这类小基数维度。

**零值预初始化**（本项目的实用小技巧）：带标签的 Counter 只有被 `.inc()` 过才会出现在
`/metrics` 里，否则 Grafana 面板显示 "No data"。所以 `storage.start()`、`events.start()`
启动时会先 `labels(...).inc(0)` 把四种事件类型、各后端都登记一遍
（见 `events.py:101~104`、`storage.py:271~275`）。

### 4.4 动手：看网关暴露了什么

```bash
# 全部 gateway_ 指标
curl -s http://localhost:8000/metrics | grep '^gateway_'

# 只看限流后端当前是谁在生效（1=生效，0=不是）
curl -s http://localhost:8000/metrics | grep ratelimit_backend_info

# 只看运行时指标（prometheus_client 自动带的）
curl -s http://localhost:8000/metrics | grep '^process_resident_memory_bytes'
```

### 4.5 动手：启动 Prometheus + Grafana

```bash
bash scripts/install_monitoring.sh   # 首次：下载 Prometheus / Grafana 原生二进制到 ~/opt/monitoring
bash scripts/monitoring.sh start     # 启动，自动渲染 provisioning 配置并校验 folderUid
bash scripts/monitoring.sh status
```

| 服务 | 地址 | 登录 |
|------|------|------|
| Prometheus | http://localhost:9090 | 无需登录；`/targets` 看抓取目标是否 UP |
| Grafana | http://localhost:3000 | 匿名只读；登录 `admin` / `admin` |
| 仪表盘 | http://localhost:3000/d/llm-gateway | 「LLM Gateway 监控」 |

可自定义端口/目录：`MON_HOME=/data/mon PROM_PORT=9091 GRAFANA_PORT=3001 bash scripts/monitoring.sh start`。

### 4.6 PromQL 入门（照抄就能用）

PromQL 是 Prometheus 的查询语言。先记住 3 个函数：

- `rate(counter[1m])`：把「累计数」换算成「每秒速率」——**Counter 几乎都要套 rate**；
- `sum by (label) (...)`：按某标签分组求和；
- `histogram_quantile(0.95, ...)`：从 bucket 算 P95。

在 http://localhost:9090/graph 里逐条粘贴试试：

```promql
# 1. 当前每秒请求数
sum(rate(gateway_requests_total[1m]))

# 2. 按模型分组的 QPS
sum by (model) (rate(gateway_requests_total[5m]))

# 3. P95 端到端延迟（Histogram 标准写法）
histogram_quantile(0.95, sum by (le) (rate(gateway_request_duration_seconds_bucket[5m])))

# 4. 错误率（注意 or vector(0)：没有错误序列时给个 0，图不会断）
(sum(rate(gateway_requests_total{status=~"5.."}[5m])) or vector(0))
  / sum(rate(gateway_requests_total[5m]))

# 5. 当前限流后端是谁
gateway_ratelimit_backend_info == 1

# 6. 三件基础设施是否都在正常工作（1=正常）
gateway_ratelimit_backend_up
gateway_history_up
gateway_kafka_up
```

### 4.7 看懂仪表盘（5 行，35 个面板）

| 行 | 面板 | 回答什么问题 |
|----|------|--------------|
| 总览 | 请求速率、P95 延迟、错误率、延迟分位数(P50/P90/P99)、TTFT P95、TPS P50/P95、Token 吞吐 | 服务整体健康吗？ |
| 路由、限流与错误 | 各模型 QPS、各 tier QPS、状态码分布、限流&鉴权失败、上游错误、并发数 | 谁在用？谁被拒了？ |
| GPU 与进程资源 | GPU 利用率/显存、进程 RSS、CPU、流式 vs 非流式 | 硬件到瓶颈了吗？ |
| Ollama 参数调优 | 按 `num_ctx`/`num_gpu`/`num_batch` 分组的 TTFT/TPS 共 6 条 | 哪组参数最适合这张卡？ |
| 分布式基础设施 | 限流后端、Redis 健康度/降级次数、历史写入速率、历史行数/可用性、Kafka 事件速率、Kafka 连接与降级丢弃 | **是哪件基础设施在降级？** |

最后一行是本次新增的重点：任何一条曲线异常，都能立刻定位到具体组件。

### 4.8 常见坑

| 现象 | 原因 | 处理 |
|------|------|------|
| 面板 "No data" | 指标从没被 inc 过（尤其带标签的 Counter） | 已在代码里零值预初始化；先发几条请求再看 |
| Grafana 报 `RangeError: Maximum call stack size exceeded` | 仪表盘 `uid` 与 provisioning 的 `folderUid` 撞名（都叫 `llm-gateway`） | 保持 `llm-gateway` vs `llm-gateway-folder` 不同；`scripts/monitoring.sh` 启动时会校验并报错 |
| Prometheus `/targets` 里 llm-gateway 是 DOWN | 网关没跑，或 8000 端口不同 | 先 `curl localhost:8000/metrics` 自测 |
| 曲线是断的、有空洞 | 该标签组合当前没数据 | 用 `or vector(0)`；或调大查询窗口 |

### 4.9 ✅ 阶段 4 验收清单

- [ ] `curl /metrics | grep gateway_` 至少能数出 20 条 `gateway_*` 指标
- [ ] Prometheus `/targets` 两个 job 都是 UP
- [ ] 在 Prometheus 里跑通上面 6 条 PromQL
- [ ] Grafana 打开 5 行看板，能指出「现在限流后端是 redis」的那块面板
- [ ] 发 20 条请求后，总览行的「请求速率」出现尖峰

---

## 5. 阶段 5：分布式限流（Redis）

### 5.1 本章目标

- 说清「单机内存限流」为什么在多实例下失效；
- 看懂固定窗口算法、key 设计和那段 Lua 脚本；
- 亲手做「两实例对照实验」，亲眼看到 Redis 让额度全局生效；
- 理解 `fail-open` / `fail-closed` 的取舍。

### 5.2 为什么需要分布式限流（本章核心动机）

假设 free 档配额是 10 次/分钟，你把网关部署 3 个实例做负载均衡：

```
内存计数（每个实例各记一份）              Redis 计数（全局一份）
   实例A: 10 次       实例B: 10 次            ┌──────────────┐
   实例C: 10 次                              │ Redis INCR=10 │ ← 三个实例都读它
   ───────────────────────────────           └──────────────┘
   用户实际能发 30 次 ❌ 配额被放大 3 倍      用户实际能发 10 次 ✅
```

这就是**必须外置计数器**的原因：配额是「用户的」属性，不是「实例的」属性。

### 5.3 限流算法速览（知道取舍就够）

| 算法 | 做法 | 优点 | 缺点 |
|------|------|------|------|
| **固定窗口**（本项目） | 每个窗口（60s）一个计数器，满了就拒 | 实现简单、1 个 key 搞定、易解释 | 窗口边界突刺：59s 发 10 次 + 01s 发 10 次 = 2 秒内 20 次 |
| 滑动窗口 | 用有序集合记录每个请求时间戳，统计「最近 60s」 | 平滑 | 内存随请求数增长，清理复杂 |
| 令牌桶 | 以恒定速率往桶里放令牌，请求消耗令牌 | 允许突发流量 | 需要额外的补充逻辑 |
| 漏桶 | 请求进桶、匀速流出 | 出口绝对平滑 | 会排队增加延迟 |

本项目选**固定窗口**：演示够用、语义清晰、Redis 上只需 2 条命令。
代价要记住：**边界处最坏能放到 2 倍配额**（面试常问）。

### 5.4 Redis 是什么（30 秒版）

- 内存键值数据库，读写都是微秒级；**命令执行是单线程的**——这正是它容易做原子操作的原因；
- 常用命令：`INCR key`（计数+1）、`PEXPIRE key ms`（设置毫秒级过期）、`PTTL key`（剩余存活时间）；
- 因为单线程，**一条命令天然原子**，但「多条命令的组合」不原子 → 所以需要 Lua 脚本。

### 5.5 本项目的实现（`ratelimit.py`）

**① key 设计**（`ratelimit.py:109`）：

```python
key = f"{self.prefix}:{tier}:{api_key}:{int(now // self.window)}"
# 例：gw:rl:free:sk-test-001:29571234
```

把 **窗口序号**（`当前时间 / 窗口长度` 取整）拼进 key，好处是：
到了下一个窗口 key 自然就变了，**不需要任何定时清理任务**——
旧 key 靠 `PEXPIRE` 自动消失。

**② Lua 脚本**（`ratelimit.py:38~44`）：

```lua
local count = redis.call('INCR', KEYS[1])      -- 计数 +1
if count == 1 then                             -- 第一次（key 刚创建）
  redis.call('PEXPIRE', KEYS[1], ARGV[1])      -- 设置窗口长度的过期时间
end
return {count, redis.call('PTTL', KEYS[1])}    -- 返回计数 + 剩余毫秒
```

为什么非要用 Lua：如果分两步发 `INCR` 和 `PEXPIRE`，在两步之间进程崩溃/网络断开，
就会留下一个**永不过期的 key**，该用户从此被永久限流。
Lua 在 Redis 里一次性执行完，不存在中间态。redis-py 还会自动用 `EVALSHA`
复用脚本缓存（只发一次脚本体，后续只发哈希值）。

**③ 门面类 `RateLimiter`**（`ratelimit.py:125`）：三种模式 + 自动重连。

```python
async def check(self, api_key, tier, limit):
    if self.mode == "memory":                    # RATE_LIMIT_BACKEND=memory 强制走内存
        return self._memory.check(...)
    if self.redis_up and self._redis is not None:
        try:
            return await self._redis.check(...)  # 正常路径
        except Exception:
            RATE_LIMIT_REDIS_ERRORS_TOTAL.inc()  # 计数 +1（监控能告警）
            self.redis_up = False
            self._ensure_reconnect()             # 后台每 5s 重连，恢复自动切回
    # 降级路径
    if self.fail_open:
        r = self._memory.check(...)              # 用进程内计数兜底，请求放行
        r.reason = f"redis unavailable: {self.redis_error}"
        return r
    return RateLimitResult(allowed=False, backend="redis-down", ...)  # 配额优先：直接 429
```

**④ 响应头**（`ratelimit.py:58~67`，遵循 draft-ietf-httpapi-ratelimit-headers）：

```
X-RateLimit-Limit: 10          # 配额
X-RateLimit-Remaining: 3       # 还剩几次
X-RateLimit-Reset: 42          # 多少秒后重置
Retry-After: 42                # 仅 429 时附带
```

### 5.6 动手 A：在 Redis 里看限流计数

```bash
# redis-cli 需要 LD_LIBRARY_PATH（脚本里的 redis_cli() 就是这么做的）
RCLI() { LD_LIBRARY_PATH=~/opt/infra/redis/lib ~/opt/infra/redis/bin/redis-cli -p 6379 "$@"; }

# 发几次请求（free 档）
for i in 1 2 3; do
  curl -s -o /dev/null -X POST http://localhost:8000/v1/chat/completions \
    -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
    -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"num_predict":2}}'
done

RCLI KEYS 'gw:rl:*'          # 例：gw:rl:free:sk-test-001:29571234
RCLI GET  'gw:rl:free:sk-test-001:29571234'    # 期望 3
RCLI PTTL 'gw:rl:free:sk-test-001:29571234'    # 期望几千毫秒（窗口剩余）
RCLI DBSIZE                  # 一共有多少个 key
```

> Redis 是 `--save '' --appendonly no`（不落盘）启动的：**重启即清空计数**，
> 这对演示很友好；生产上你会希望它开 AOF/RDB 持久化。

### 5.7 动手 B：两实例对照实验（本项目的「招牌实验」）

同时起两个网关实例，用**同一个 free key** 各发 6 次（配额 10 次/分钟）：

```bash
# 终端 1：实例 A
REDIS_URL=redis://localhost:6379/0 RATE_LIMIT_BACKEND=redis \
RATE_LIMIT_WINDOW=60 GATEWAY_INSTANCE=gw-a SERVER_PORT=8000 python main.py

# 终端 2：实例 B
REDIS_URL=redis://localhost:6379/0 RATE_LIMIT_BACKEND=redis \
RATE_LIMIT_WINDOW=60 GATEWAY_INSTANCE=gw-b SERVER_PORT=8001 python main.py

# 终端 3：先清空上次的计数，再各发 6 次
RCLI() { LD_LIBRARY_PATH=~/opt/infra/redis/lib ~/opt/infra/redis/bin/redis-cli -p 6379 "$@"; }
RCLI FLUSHDB
hit() { curl -s -o /dev/null -w '%{http_code} ' -X POST "http://localhost:$1/v1/chat/completions" \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"num_predict":2}}'; }
for i in $(seq 1 6); do hit 8000; done; echo " <- 实例A"
for i in $(seq 1 6); do hit 8001; done; echo " <- 实例B"
```

**预期（redis 模式）**：

```
200 200 200 200 200 200  <- 实例A（6 次全过，Redis 计数到 6）
200 200 200 200 429 429  <- 实例B（全局只剩 4 个名额，后 2 次被拒）
                 ↑ 合计放行 10 次 = 配额
```

再把两个实例都改成 `RATE_LIMIT_BACKEND=memory` 重跑（记得先 `RCLI FLUSHDB` 并**等窗口过去**）：

```
200 200 200 200 200 200  <- 实例A（自己数到 6）
200 200 200 200 200 200  <- 实例B（自己数到 6）
                 ↑ 合计 12 次，额度被放大 → 只能单实例用
```

这个「反证」非常重要：它证明共享确实生效了，而不是碰巧数字对上。

> 懒人版：`python verify_infra.py` 会自动跑这套对照（含内存模式反证）+ 21 项检查，
> 见第 9 章。跑完它会清理自己起的两个实例。

### 5.8 `fail-open` 还是 `fail-closed`？

| 配置 | Redis 挂掉时的行为 | 适合场景 |
|------|-------------------|----------|
| `RATE_LIMIT_BACKEND=auto` + `REDIS_FAIL_OPEN=1`（**默认**） | 该实例改用进程内计数，请求继续放行；后台 5s 一次重连 | 可用性优先（LLM 推理服务通常是这样） |
| `RATE_LIMIT_BACKEND=redis` + `REDIS_FAIL_OPEN=0` | 每次请求直接 429（`backend=redis-down`） | 配额/成本优先（按量计费的对外 API） |
| `RATE_LIMIT_BACKEND=memory` | 永远用进程内计数 | 单实例部署 / 做对照实验 |

**为什么默认是 fail-open**：限流是保护措施，不是业务功能。
如果因为它挂掉就把所有推理请求拒了，等于「为了防洪水把水龙头全关了」。

### 5.9 ✅ 阶段 5 验收清单

- [ ] 能在 Redis 里找到 `gw:rl:*` 的 key 并解释它是怎么拼出来的
- [ ] 能说出 Lua 脚本里 `if count == 1 then PEXPIRE` 的必要性
- [ ] 亲手跑出「redis=10 次放行 / memory=12 次放行」的对照结果
- [ ] 知道 3 个响应头的含义，并能在 `curl -D -` 里看到它们
- [ ] 能解释固定窗口的「边界 2 倍突刺」问题

### 5.10 常见坑

| 现象 | 原因 | 处理 |
|------|------|------|
| 对照实验数字不对 | 上一轮窗口的计数还在 / 两个实例 tier 不同 | `RCLI FLUSHDB`，并等一个窗口（`RATE_LIMIT_WINDOW` 秒）过去再跑 |
| `redis-server: error while loading shared libraries: liblzf.so.1` | 只解包了 Redis 主包 | 用 `bash scripts/infra.sh install`（会一起解包 liblzf1 并设好 `LD_LIBRARY_PATH`） |
| 启动日志显示 `限流后端: memory` | Redis 没起 / 端口不对 | `bash scripts/infra.sh start`；看 `~/opt/infra/logs/redis.log` |
| 429 出现得太快 | `RATE_LIMIT_WINDOW` 太小或用了 free 档 | 换 `sk-prod-001`（60 次/分钟）或调大窗口 |
| `redis-cli: error while loading shared libraries` | 没设 `LD_LIBRARY_PATH` | 用上面的 `RCLI()` 函数 |

---

## 6. 阶段 6：请求历史（SQLite / PostgreSQL）

### 6.1 本章目标

- 说出「为什么日志文件不够、还要一个库」；
- 看懂异步批量写入这条流水线，以及它为什么不会拖慢请求；
- 会直接查库（SQLite / Postgres 两套）；
- 会用 `/history` 和 `/history/stats` 排查问题。

### 6.2 为什么不只用日志文件

`logs/gateway.log` 是 JSONL 纯文本：好写好读，但**不好聚合**。
想回答「最近一小时 P95 的首 token 延迟是多少」「哪个模型用的人最多」，
用 `grep`/`awk` 也能硬算，但那是自找麻烦。数据库负责这类**结构化查询**：

| | `logs/gateway.log` | `request_history` 表 |
|---|---|---|
| 形式 | 追加写文本 | 行 + 列，带索引 |
| 适合 | 看单条明细、事后取证 | 聚合、分位数、按维度分组 |
| 保留 | 无限（人工清理） | 可加 TTL/分区清理策略 |
| 挂了会怎样 | 磁盘满会影响主链路 | 只影响 `/history`，请求照过 |

### 6.3 两个后端、一个接口

```
DATABASE_URL=sqlite:///logs/gateway.db                     ← 默认，零依赖（标准库 sqlite3）
DATABASE_URL=postgresql://gw:gw@localhost:5433/gateway     ← 生产建议（psycopg 3）
```

`storage.py` 用「同一套方法、两个实现」的写法（`_SqliteBackend` / `_PostgresBackend`），
每一个都提供 `ensure_schema / insert_many / query / scalar / close`，
差异只收敛在一处：占位符 `?`（SQLite）vs `%s`（Postgres）。
上层 `RequestHistory` 完全不知道自己连的是哪个库。

> 为什么用同步驱动 `psycopg` 而不是 `asyncpg`？因为写库走 `asyncio.to_thread()`
> 丢到线程池，不阻塞事件循环，这样**两套方言可以共用一份 SQL**，少维护一半代码。

### 6.4 写路径：异步批量（`storage.py:311~361`）

```
请求返回 ─► history.submit(record)        只做一次 put_nowait（非阻塞，微秒级）
                  │
            写入队列（上限 HISTORY_QUEUE_SIZE = 5000）
                  │   攒够 HISTORY_BATCH_SIZE(20) 行，或等满 HISTORY_FLUSH_INTERVAL(1.0) 秒
                  ▼
       asyncio.to_thread(backend.insert_many)   一次多行 INSERT（executemany）
                  │
            成功 → HISTORY_WRITES_TOTAL += 行数
            失败 → HISTORY_WRITE_ERRORS_TOTAL += 1，丢弃这批并关掉坏连接（下批重连）
            队列满 → HISTORY_DROPPED_TOTAL += 1（丢历史，不阻塞请求）
```

关键代码（`storage.py:321~334`，写队列的经典「攒批」模式）：

```python
async def _writer_loop(self):
    while True:
        first = await asyncio.wait_for(self._queue.get(), self.flush_interval)  # 等第一条
        batch = [first]
        while len(batch) < self.batch_size:        # 尽力多捞，最多 batch_size 条
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        await self._flush(batch)                   # 一次 INSERT 多行
```

**为什么批量**：单条 INSERT 的开销主要在事务往返（网络/磁盘 fsync），
一次写 20 行和写 1 行的成本差不多，吞吐却能提升一个数量级。
代价是「最多延迟 1 秒才落库」——对分析场景完全可以接受。

### 6.5 表结构（`storage.py:44~72`）

```sql
CREATE TABLE IF NOT EXISTS request_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,   -- Postgres 用 BIGSERIAL
    ts REAL NOT NULL,                       -- epoch 时间（float）
    api_key TEXT, tier TEXT, model TEXT, status INTEGER, stream INTEGER,
    messages_count INTEGER, wall_time REAL, ttft REAL, tps REAL,
    prompt_tokens INTEGER, completion_tokens INTEGER,
    gpu_util INTEGER, gpu_mem_mb INTEGER,
    num_ctx TEXT, num_gpu TEXT, num_batch TEXT,      -- Ollama 参数标签（default 表示未指定）
    rate_limit_backend TEXT, instance TEXT, error TEXT
);
CREATE INDEX idx_history_ts         ON request_history (ts);
CREATE INDEX idx_history_model_ts   ON request_history (model, ts);
```

两个索引对应两种常见查询：按时间倒序看最近记录、按模型 + 时间做分组统计。
`instance` 列（`GATEWAY_INSTANCE`）用来区分多实例——排障时能看出「这条是哪个实例处理的」。

### 6.6 SQLite 的 WAL 模式（`storage.py:123~134`）

```python
conn = sqlite3.connect(self.path, timeout=10)
conn.execute("PRAGMA journal_mode=WAL")     # 写不阻塞读
conn.execute("PRAGMA synchronous=NORMAL")   # 牺牲一点落盘强度换速度
conn.execute("PRAGMA busy_timeout=5000")    # 锁冲突时最多等 5s 再报错
```

- **WAL（Write-Ahead Logging）**：SQLite 把改动先写 `-wal` 文件，读操作仍然读主库，
  于是「一边写一边读」不再互相卡死（默认的回滚日志模式下写会锁住读）。
  你会看到 `logs/gateway.db-wal` 和 `-shm` 两个伴随文件，属正常现象；
- **每线程一条连接**（`threading.local()`）：SQLite 连接不能跨线程共享，
  而写库跑在线程池里，所以按线程各留一条；
- Postgres 那边同理用了 `threading.local()`（`storage.py:172~181`）。

### 6.7 读路径与两个端点

```bash
# 最近 20 条（可按 model / api_key 过滤，limit 上限 200）
curl -s 'http://localhost:8000/history?limit=3'
curl -s 'http://localhost:8000/history?model=qwen0.5b&limit=3' | head -c 600

# 窗口聚合（window 单位秒，默认 3600）
curl -s 'http://localhost:8000/history/stats?window=600'
```

`/history/stats` 返回的正是「日志算起来很麻烦」的那些数：

```json
{"totals":{"requests":28,"errors":2,"avg_wall_time":0.104,"avg_ttft":0.091,
           "p95_wall_time":0.111,"p95_ttft":0.42,
           "prompt_tokens":728,"completion_tokens":112},
 "by_model":[{"model":"qwen2.5:0.5b","requests":26,"avg_ttft":0.091,"avg_tps":116.8}],
 "rows_total":28, "rows_this_instance":28}
```

**P95 是怎么算的**（`storage.py:440~454`）：SQLite 没有内置百分位函数，
Postgres 的 `percentile_cont` 语法又不通用，所以用可移植写法：
先 `COUNT(*)` 得到 n，再 `ORDER BY field LIMIT 1 OFFSET int(n*0.95)` 取那一条。

```sql
SELECT COUNT(*) FROM request_history WHERE ts >= ? AND wall_time IS NOT NULL;
SELECT wall_time FROM request_history WHERE ts >= ? AND wall_time IS NOT NULL
  ORDER BY wall_time LIMIT 1 OFFSET 47;
```

代价是「近似 + 两次查询」，好处是**一套代码通吃两种数据库**。
数据量大时应换成 `percentile_cont`（PG）或预聚合表。

### 6.8 动手：直接查库

**SQLite（默认，零依赖）**：

```bash
sqlite3 logs/gateway.db "SELECT COUNT(*) FROM request_history;"
sqlite3 -header -column logs/gateway.db \
  "SELECT id, model, status, ttft, tps, num_ctx FROM request_history ORDER BY id DESC LIMIT 5;"
```

**PostgreSQL（脚本解包的 PG 需要 LD_LIBRARY_PATH）**：

```bash
PSQL() { LD_LIBRARY_PATH=~/opt/infra/postgres/usr/lib/x86_64-linux-gnu \
  ~/opt/infra/postgres/usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -p 5433 -U gw gateway "$@"; }

PSQL -c "SELECT count(*) FROM request_history;"
PSQL -c "SELECT model, count(*), round(avg(ttft)::numeric,3) AS avg_ttft
         FROM request_history GROUP BY model ORDER BY 2 DESC;"
PSQL -c "SELECT instance, count(*) FROM request_history GROUP BY instance;"
```

切换后端只需换一个环境变量（网关侧）并重启：

```bash
DATABASE_URL=postgresql://gw:gw@localhost:5433/gateway python main.py
```

### 6.9 降级行为

| 场景 | 表现 |
|------|------|
| 建表/连库失败（如 PG 没起） | `start()` 里捕获，`/health` 的 `history_backend` 变 `none`，`gateway_history_up=0`；请求照常 200 |
| 运行中写库失败 | 该批丢弃 + `gateway_history_write_errors_total+1`，下批自动重连尝试 |
| 队列积压（写得太慢） | 超过 5000 条后新记录直接丢，`gateway_history_dropped_total+1` |
| 关闭网关 | `close()` 会把队列里剩余的记录刷完再退出（不丢最后几条） |

### 6.10 ✅ 阶段 6 验收清单

- [ ] `sqlite3 logs/gateway.db "SELECT COUNT(*) FROM request_history;"` 有数
- [ ] `/history?limit=5` 能看到 `ttft`/`tps`/`num_ctx`/`instance` 字段
- [ ] `/history/stats?window=600` 里 `p95_ttft` 不是 null
- [ ] 把 `DATABASE_URL` 切到 Postgres 后重启，`/health` 显示 `history_backend=postgres`
- [ ] 能解释「为什么写库不会拖慢模型推理」

### 6.11 常见坑

| 现象 | 原因 | 处理 |
|------|------|------|
| `/history` 一直 `count:0` 但请求是 200 | 历史后端没起来 / 刚写完还没 flush | 看 `/infra` 的 `history.enabled`；或等 1 秒（`HISTORY_FLUSH_INTERVAL`）再查 |
| `history_backend: none` | PG 没启动或 DSN 写错 | `bash scripts/infra.sh start`；`/infra` 的 `history.error` 里有具体原因 |
| `initdb: error while loading shared libraries: libpq.so.5` | PG 解包缺 libpq5 | `bash scripts/infra.sh install` 已处理；手工安装时注意 `LD_LIBRARY_PATH` |
| SQLite 报 `database is locked` | 别的进程长时间持锁 | 已设 `busy_timeout=5000`；确认没有其他进程占用该 db 文件 |
| PG 连接被拒（5432 连不上） | 本项目 PG 用的是 **5433**（避开系统默认 5432） | `DATABASE_URL` 里写 5433 |

---

## 7. 阶段 7：Kafka 异步事件

### 7.1 本章目标

- 理解 Kafka 的核心名词（broker / topic / partition / offset / producer / consumer）；
- 知道「网关发事件」和「下游消费事件」是如何解耦的；
- 会用命令行消费者亲眼看到 4 类事件；
- 理解 `api_key` 作为分区 key 的意义。

### 7.2 为什么还要一条"事件流"

日志是**给自己看的**，历史库是**给自己查的**，Kafka 是**给别人用的**：

- 同一份事件可以被多个下游各自消费、互不干扰：数仓做离线分析、告警系统做实时检测、
  BI 做看板刷新；
- 网关只负责「生产（produce）」，消费端慢或挂掉都**不会反压网关**（消息留在 Kafka 里等它）；
- 这是「事后分析」通道：请求已经返回给用户了，分析再慢也不影响用户体验。

### 7.3 Kafka 核心名词（用快递站类比）

| 名词 | 类比 | 本项目取值 |
|------|------|-----------|
| **broker** | 快递站点（一台服务器） | 单节点 `localhost:9092` |
| **topic** | 一个快递通道的名字 | `llm-gateway-events` |
| **partition** | 通道里的若干条并行流水线 | 3 条（0/1/2） |
| **offset** | 每条流水线上的包裹编号（递增） | 由 Kafka 维护 |
| **producer** | 寄件人 | 网关里的 `EventBus` |
| **consumer / consumer group** | 收件人 / 一个收件小组 | 命令行消费者、未来的分析服务 |
| **key** | 包裹上的「分拣码」 | `api_key` |

**分区 key 的作用**：同 key 的消息进同一分区，而单分区内**严格有序**。
把 `api_key` 当 key，就意味着「同一个用户的事件按发生顺序排列」——
下游要还原某个用户的完整轨迹时非常方便。反之如果不设 key，
消息会被轮询打散到 3 个分区，全局有序性就没了。

**KRaft**：老版本 Kafka 需要额外跑一个 ZooKeeper 来管集群元数据；
新版用 KRaft 模式把这份职责收进 Kafka 自己（本项目就是 broker + controller 同进程的单节点）。
好处是部署少一个组件，坏处是「单节点」在生产上不具备高可用——本项目的定位是演示。

### 7.4 事件的信封格式（`events.py:169~185`）

```python
def publish(self, event_type: str, payload: dict) -> None:
    envelope = {
        "event": event_type,                       # 事件类型（用字段区分，而不是分 topic）
        "ts": round(time.time(), 3),               # epoch 秒
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "instance": self.instance,                 # gateway-8000（多实例排障）
        "host": socket.gethostname(),
        "data": payload,                           # 请求的完整记录（ttft/tps/tokens/...）
    }
    self._queue.put_nowait((event_type, envelope))  # 非阻塞入队，满了就丢弃+计数
```

四种事件类型（`events.py:62` 的 `EVENT_TYPES`，与 `main.py` 里 `_persist(...)` 的调用点一一对应）：

| 事件类型 | 触发点 | 何时最有用 |
|----------|--------|-----------|
| `request.completed` | 200 正常完成（流式在结束分片后补记） | 用量统计、性能分析 |
| `request.rate_limited` | 429 被限流 | 发现被刷、判断配额是否合理 |
| `request.auth_failed` | 401 key 无效 | 安全审计、探测攻击 |
| `request.error` | 请求体非法 / options 校验失败 / 上游 Ollama 错误 | 错误归因 |

**只有一个 topic，用 `event` 字段区分类型**，这是常见取舍：
少量类型放同一 topic 更省心（不用为每种事件建 topic + ACL）；
如果想要「每种事件独立保留策略/独立消费组」，就该拆成多个 topic。

### 7.5 生产者的可靠性参数（`events.py:113~124`）

```python
AIOKafkaProducer(
    bootstrap_servers=self.bootstrap,
    client_id=self.client_id,
    acks=1,                    # leader 写入即算成功（1=快，all=更稳）
    linger_ms=20,              # 攒 20ms 再发，提高批量效率
    compression_type="gzip",   # 压缩，省带宽
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
    key_serializer=lambda k: str(k).encode() if k else None,
)
await asyncio.wait_for(producer.start(), timeout=self.start_timeout)  # 启动超时 5s
```

发送时用 `send_and_wait`（`events.py:210`）——**等 broker 确认**再算成功，
失败则整批降级写文件并触发重连。注意 `producer.start()` 包了 `wait_for`：
broker 不在时 5 秒就放弃，不会把网关启动拖死。

### 7.6 动手 A：起 Kafka 并看 topic

```bash
bash scripts/infra.sh install     # 首次：下载官方 tarball（需要系统有 java 17+，本机 Java 25）
bash scripts/infra.sh start

KAFKA_BIN=~/opt/infra/kafka/kafka_2.13-4.3.1/bin
$KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --list
$KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic llm-gateway-events
# 期望：PartitionCount: 3  ReplicationFactor: 1
```

### 7.7 动手 B：消费事件

```bash
# 看最新 5 条（-from-beginning 从头，去掉则只看新消息）
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic llm-gateway-events \
  --from-beginning --max-messages 5

# 只统计各类型事件条数（把 JSON 的 event 字段抠出来）
$KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic llm-gateway-events \
  --from-beginning --timeout-ms 5000 2>/dev/null \
  | python -c "import sys,json,collections; \
print(collections.Counter(json.loads(l)['event'] for l in sys.stdin if l.strip().startswith('{')))"
```

预期能看到（先发几条正常请求 + 一条错 key）：

```
Counter({'request.completed': 8, 'request.auth_failed': 1, 'request.rate_limited': 2})
```

一条真实事件长这样（节选）：

```json
{"event":"request.completed","ts":1759500000.123,"ts_iso":"2026-10-03T23:28:50",
 "instance":"gateway-8000","host":"dawson-RESCUER-R720-15IKBN",
 "data":{"api_key":"sk-test-001","tier":"free","model":"qwen2.5:0.5b","status":200,
         "stream":true,"wall_time":0.412,"ttft":0.017,"tps":74.01,
         "prompt_tokens":26,"completion_tokens":4,"num_ctx":"512",
         "rate_limit_backend":"redis","instance":"gateway-8000"}}
```

### 7.8 降级：broker 挂了会怎样

```
publish() 入队 ─► _worker_loop 攒批 ─► _send()
                                       ├─ 成功 → published+1，EVENTS_PUBLISHED_TOTAL+1
                                       └─ 失败/未连接 → _write_fallback()
                                                       写 logs/events.jsonl（多一个 fallback_reason 字段）
                                                       EVENTS_FALLBACK_TOTAL+1，后台每 10s 重连
```

```bash
# 亲眼看看降级文件
tail -n 3 logs/events.jsonl | python -m json.tool --no-ensure-ascii 2>/dev/null | head -20
grep -o '"fallback_reason":"[^"]*"' logs/events.jsonl | sort | uniq -c   # 每条降级原因
```

设计要点：**投递失败也不重试发 Kafka**（避免队列无限膨胀），直接落盘保底；
恢复连接后只发新事件，历史降级文件留给人工/脚本补发。

### 7.9 ✅ 阶段 7 验收清单

- [ ] `kafka-topics.sh --list` 能看到 `llm-gateway-events`（3 分区）
- [ ] 命令行消费者能看到 `request.completed` 事件，且 `key` 是 api_key
- [ ] 用错 key 发一次请求，Kafka 里出现 `request.auth_failed`
- [ ] 能说出「4 种事件类型都在一个 topic 里，靠 `event` 字段区分」
- [ ] 能解释为什么用 `api_key` 做分区 key

### 7.10 常见坑

| 现象 | 原因 | 处理 |
|------|------|------|
| Kafka 起不来 / `kafka-storage.sh format` 失败 | 缺 Java 17+ / 磁盘权限 | `java -version` 确认；看 `~/opt/infra/logs/kafka-format.log` |
| 网关日志出现 `事件总线: ... 降级` | broker 没起或端口不对 | `bash scripts/infra.sh start`；`curl /infra` 看 `events.error` |
| 消费者一直没输出 | 都是旧消息/或用错 port | 加 `--from-beginning`，或先发几条新请求 |
| 手改 server.properties 不生效 | 脚本以该文件是否存在判断「是否已初始化」 | 删掉 `~/opt/infra/kafka/server.properties` 再 start |
| topic 名字想改 | 网关与 Kafka 两侧都要改 | `KAFKA_TOPIC=your-topic bash scripts/infra.sh start` + 网关侧同变量 |

---

## 8. 阶段 8：降级演练（本项目最值得学的一章）

### 8.1 本章目标

把三件基础设施**逐个打挂**，亲眼验证「推理主链路不受影响」，
并学会从 `/health`、`/infra`、`/metrics` 三个地方读出「是谁在降级」。

### 8.2 先说清楚三件套各自的降级策略

| 组件 | 挂掉后的行为 | 你会丢什么 | 观测点 |
|------|--------------|-----------|--------|
| Redis（限流） | `fail_open=1` 时改用进程内计数放行；`=0` 时 429 | 多实例共享额度（退化成每个实例各算一份） | `/health.rate_limit_backend=memory`、`gateway_ratelimit_redis_errors_total` |
| 历史库（SQLite/PG） | 写入失败/队列满时丢弃并计数，请求照过 | 这部分请求的历史明细 | `/infra.history.error`、`gateway_history_write_errors_total` |
| Kafka（事件） | 降级写 `logs/events.jsonl`，每 10s 重连 | Kafka 里的实时事件（文件里还留着） | `/infra.events.connected=false`、`gateway_events_fallback_total` |

一句话总结设计哲学：**基础设施是「增强」，不是「依赖」**。

### 8.3 演练前的快照

先记录「健康时的样子」，后面好对比：

```bash
curl -s http://localhost:8000/health | python -m json.tool
curl -s http://localhost:8000/infra  | python -m json.tool
bash scripts/infra.sh status
```

### 8.4 演练一：打挂 Kafka

```bash
# ① 记下现在的计数
curl -s http://localhost:8000/infra | python -c "import sys,json;d=json.load(sys.stdin);print('fallback=',d['events']['fallback_written'],'published=',d['events']['published'])"

# ② 杀掉 Kafka
kill "$(cat ~/opt/infra/run/kafka.pid)"

# ③ 连发 3 条请求（正常应该都是 200）
for i in 1 2 3; do curl -s -o /dev/null -w '%{http_code} ' -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-prod-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"num_predict":2}}'; done; echo

# ④ 看降级状态
curl -s http://localhost:8000/infra | python -c "import sys,json;e=json.load(sys.stdin)['events'];print({k:e[k] for k in ('connected','published','fallback_written','error')})"
curl -s http://localhost:8000/metrics | grep -E 'gateway_kafka_up|gateway_events_fallback_total'
tail -n 1 logs/events.jsonl | head -c 300
```

**预期**：3 条请求都是 `200`；`connected=false`；`fallback_written` 增加；
`gateway_kafka_up 0`；`logs/events.jsonl` 末尾出现带 `"fallback_reason"` 的事件。

### 8.5 演练二：恢复 Kafka（验证自动重连）

```bash
bash scripts/infra.sh start        # 只把 Kafka 拉回来也行（脚本会跳过已在跑的）
sleep 12                           # 重连间隔 10s
curl -s http://localhost:8000/infra | python -c "import sys,json;e=json.load(sys.stdin)['events'];print('connected=',e['connected'],'error=',e['error'])"
curl -s http://localhost:8000/metrics | grep gateway_kafka_up   # 期望 1
```

**预期**：10 秒左右自动重连成功，之后的事件继续进 Kafka（`published` 继续增长），
**不需要重启网关**。这就是「后台重连循环」的价值。

### 8.6 演练三：打挂 Redis

```bash
# 先清空计数（保证窗口是干净的），再打挂 Redis
LD_LIBRARY_PATH=~/opt/infra/redis/lib ~/opt/infra/redis/bin/redis-cli -p 6379 FLUSHDB
kill "$(cat ~/opt/infra/run/redis.pid)"

# 用 free 档（sk-test-001）连发 12 次：配额 10 次/分钟
for i in $(seq 1 12); do curl -s -o /dev/null -w '%{http_code} ' -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"num_predict":2}}'; done; echo

curl -s http://localhost:8000/health | python -c "import sys,json;print(json.load(sys.stdin)['rate_limit_backend'])"
curl -s http://localhost:8000/metrics | grep -E 'gateway_ratelimit_backend_up|gateway_ratelimit_redis_errors_total'
```

**预期（`RATE_LIMIT_BACKEND=auto`，默认）**：前 10 次 200（进程内计数兜底），
第 11、12 次 429 —— 注意**此时限流仍然生效**，只是额度不再全局共享；
`/health` 的 `rate_limit_backend` 变成 `memory`，
`gateway_ratelimit_backend_up 0`，`gateway_ratelimit_redis_errors_total` ≥ 1。

```bash
# 对照：恢复 Redis 后（共享计数回来）再跑一次
bash scripts/infra.sh start && sleep 6
LD_LIBRARY_PATH=~/opt/infra/redis/lib ~/opt/infra/redis/bin/redis-cli -p 6379 FLUSHDB
curl -s http://localhost:8000/health | python -c "import sys,json;print(json.load(sys.stdin)['rate_limit_backend'])"  # redis
```

**如果想看 fail-closed**（配额优先）：

```bash
# 另起一个实例，强制 Redis + fail-closed
REDIS_URL=redis://localhost:6379/0 RATE_LIMIT_BACKEND=redis REDIS_FAIL_OPEN=0 \
SERVER_PORT=8003 python main.py
# 此时 Redis 还挂着 → 每个请求都会 429，detail 里写明 redis unavailable (fail-closed)
curl -s -X POST http://localhost:8003/v1/chat/completions -H 'Content-Type: application/json' \
  -H 'x-api-key: sk-prod-001' -d '{"model":"qwen0.5b","messages":[]}' -w '\n%{http_code}\n'
```

### 8.7 演练四：恢复 Redis + 演练五：打挂历史库

```bash
# 恢复 Redis：等 5 秒自动重连（RECONNECT_INTERVAL）
bash scripts/infra.sh start
sleep 6
curl -s http://localhost:8000/health | python -c "import sys,json;print(json.load(sys.stdin)['rate_limit_backend'])"   # 期望 redis

# 打挂 PostgreSQL（网关正在用它写历史）
~/opt/infra/postgres/usr/lib/postgresql/18/bin/pg_ctl -D ~/opt/infra/pgdata stop 2>/dev/null \
  || bash scripts/infra.sh stop    # 简单粗暴：全停

# 再发几条请求
for i in 1 2 3; do curl -s -o /dev/null -w '%{http_code} ' -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-prod-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"hi"}],"options":{"num_predict":2}}'; done; echo

curl -s http://localhost:8000/infra | python -c "import sys,json;h=json.load(sys.stdin)['history'];print({k:h[k] for k in ('enabled','backend','written','error')})"
curl -s http://localhost:8000/metrics | grep -E 'gateway_history_up|gateway_history_write_errors_total'
```

**预期**：请求仍然 200；`history.error` 出现具体错误；`gateway_history_up 0`；
`gateway_history_write_errors_total` 增长。历史丢了就丢了——这部分数据本来就是「近似」的。

最后全部恢复：

```bash
bash scripts/infra.sh start
sleep 6
curl -s http://localhost:8000/health | python -m json.tool   # 三件套应全部回到正常
```

> 注意：历史后端**不会**自动「重建连接」——代码在写失败后会关掉坏连接，
> 下一批 INSERT 会用新连接重试（`storage.py:_flush` 的 `backend.close()`），
> 所以 PG 恢复后就能继续写，不需要重启网关。

### 8.8 读懂 `/infra` 这个排障入口（下面是节选）

```json
{"instance":"gateway-8000",
 "rate_limit":{"mode":"auto","active":"redis","redis_up":true,"fail_open":true,"error":""},
 "history":{"enabled":true,"backend":"postgres","dsn":"postgresql://gw:***@localhost:5433/gateway",
            "queued":0,"written":128,"error":""},
 "events":{"enabled":true,"connected":true,"topic":"llm-gateway-events",
           "queued":0,"published":131,"failed":0,"fallback_written":0,"dropped":0,"error":""}}
```

> 完整字段还包括 `redis_url`/`window_seconds`、`batch_size`/`flush_interval`、
> `bootstrap`/`fallback_file` 等；这里只留了排障真正会看的字段。

排障顺序建议：**先看 `error` 字段 → 再看 `connected/redis_up/up` → 最后看积压 `queued`**。
`queued` 一直很大说明下游太慢，正在靠丢弃保护主链路。

### 8.9 ✅ 阶段 8 验收清单

- [ ] 四个演练全部亲手做过，请求在任一组件挂掉时都返回 200
- [ ] 能不看文档说出「Redis 挂了之后限流变成什么语义」
- [ ] 能在 `/metrics` 里指出对应的降级计数指标
- [ ] 恢复服务后**没有重启网关**就自动恢复
- [ ] 能解释为什么历史/kafka 用「丢弃 + 计数」而不是「重试到成功」

---

## 9. 阶段 9：自动化验收、压测与参数调优

### 9.1 本章目标

- 会跑 `verify_infra.py` 做「一键验收」（它会自己起两个实例做对照实验）；
- 会用 `load_test.py` 感受并发；
- 会用 `tuning_experiment.py` 找出本机 GPU 的最佳 Ollama 参数。

### 9.2 一键验收：`verify_infra.py`

它做的事（读完这段你就知道它为什么值 21 项检查）：

1. 检查 `/health`、`/infra`：限流后端是不是 redis、历史后端启没启、Kafka 连没连；
2. 发一次非流式 + 一次流式请求，检查 200、限流响应头、SSE 是否完整；
3. 检查 `/history`（是否带 `num_ctx/num_gpu/num_batch`、ttft/tps）与 `/history/stats`；
4. 直连数据库核对行数与聚合（SQLite 用 `sqlite3`，PG 用 `psycopg`）；
5. **限流对照实验**：两个实例各发 6 次 → redis 模式只放行 10 次；
   再用 memory 模式跑一次 → 12 次全放行（反证）；
6. 消费 Kafka，检查各事件类型条数与 `instance` 字段。

```bash
python verify_infra.py                     # 默认 SQLite + Redis + Kafka，21 项
python verify_infra.py --database-url postgresql://gw:gw@localhost:5433/gateway
python verify_infra.py --skip-kafka        # 只验限流与历史
python verify_infra.py --keep-instances    # 跑完保留两个实例（排障用）
python verify_infra.py --port 8100         # 换起始端口（实例占用 :8100 与 :8101）
```

期望输出（结尾）：

```
...
  [PASS] 限流后端 = redis（共享计数生效）
  ...
  [PASS] 12 次请求（各 6 次）只放行 10 次  放行=10
  [PASS] 内存后端下 12 次请求全部放行（反证 Redis 才是共享的）  放行=12
  ...
验收结果: 21/21 项通过
```

> 脚本会自己拉起两个网关实例（`:8000` / `:8001`），跑完自动关掉；
> **运行时不要占用这两个端口**（比如你手工起的网关）。跑一次大约 1~2 分钟。

### 9.3 并发压测：`load_test.py`

```bash
python load_test.py
# === concurrency=1 ===  avg: 0.31 s
# === concurrency=2 ===  avg: 0.42 s
# === concurrency=4 ===  avg: 0.79 s
# === concurrency=8 ===  avg: 1.35 s
```

这个脚本很简单（线程池并发发请求 + 统计平均/伪 P95），但能直观看到
**GPU 只有一个，并发上去延迟就会线性变差**——这正是需要限流的原因。

压测时同时观察：

```bash
curl -s http://localhost:8000/metrics | grep -E 'gateway_inflight_requests|gateway_gpu_utilization'
# 或在 Grafana 看「并发处理中的请求数」与「GPU 利用率」两条曲线
```

注意 `load_test.py` 用的是 `sk-prod-001`（pro 档 60 次/分钟），
否则会被限流影响统计结果。

### 9.4 参数调优实验：`tuning_experiment.py`

```bash
python tuning_experiment.py --dry-run        # 先看实验计划（不真跑）
python tuning_experiment.py --sweeps ctx     # 只扫 num_ctx
python tuning_experiment.py --runs 3         # 全扫：ctx + gpu + batch（52 请求，约 2 分钟）
```

实验设计要点（这也是「做实验」的通用套路，值得抄）：

- 每组配置：**1 次 warmup（冷加载，不计入统计）+ 3 次测量**；
- 固定 208 token 的 prompt、`num_predict=64`、`temperature=0`，保证各组可比；
- 记录四个量：端到端 TTFT（client 侧）、网关 TTFT、tokens/s、`nvidia-smi` 显存；
- 产出 `logs/tuning_experiment.jsonl`（明细）+ 汇总 `logs/tuning_experiment_summary.json`。

它靠的是网关在 `include_usage` 时额外回的那个分片里的 `metrics.ttft/tps`——
所以**实验脚本不需要自己解析 SSE 的 token 流**，网关已经把关键数字算好了。
这就是第 3 章埋的伏笔。

本机实测（qwen2.5:0.5b / GTX 1050 Ti 4GB / 52 请求）：

| 配置 | warm TTFT (s) | tokens/s | 显存 (MiB) |
|------|--------------:|---------:|-----------:|
| num_ctx=512 | 0.014 | 192.9 | 472 |
| num_ctx=8192 | 0.014 | 171.7 | 568 |
| num_gpu=0（纯 CPU） | 0.079 | 71.2 | 2 |
| num_gpu=12（半层） | 0.024 | 90.5 | 354 |
| num_gpu=-1（全量 GPU） | 0.016 | 176.3 | 490 |
| num_batch=64 | 0.013 | 188.0 | 460 |
| num_batch=1024 | 0.014 | 192.8 | 526 |

结论（可以直接背下来）：

1. **num_ctx**：短 prompt 下速度几乎无感，但显存线性增长
   （512→8192 约 +96 MiB ≈ 12.8 KiB/token 的 KV cache）——长对话才需要大 ctx；
2. **num_gpu**：小模型要么全放 GPU 要么别放。半 offload（12/24 层）反而最差
   （GPU↔CPU 多一次搬运）：176 → 90 tok/s；
3. **num_batch**：64→1024 对解码速度无单调影响（噪声内），但显存会涨；
   保持默认即可；
4. **换参即重载**：`num_ctx/num_gpu/num_batch` 任一变化都会让 Ollama 重载模型
   （冷启动 TTFT ≈ 2.1s）——**生产高峰不要动态换参**。

**已知坑（脚本已规避，值得学）**：

- Ollama 0.35 的 `num_gpu=-1` 从显式值切回时不触发重载（它以为配置没变），
  所以脚本每组开始前先 `keep_alive=0` 强制卸载模型；
- prompt 长度超过 `num_ctx` 会被静默截断，所以最小扫到 512（208 + 模板 + 64 生成）。

### 9.5 ✅ 阶段 9 验收清单

- [ ] `python verify_infra.py` 打印 `21/21 项通过`
- [ ] `--database-url postgresql://...` 再跑一遍也是 21/21（两种后端都验过）
- [ ] 跑一次 `load_test.py`，能说出并发从 1 升到 4 时延迟为什么变长
- [ ] `python tuning_experiment.py --sweeps ctx` 跑完能看到 summary 文件
- [ ] 能在 Grafana「Ollama 参数调优」行看到自己刚跑出来的曲线

### 9.6 常见坑

| 现象 | 原因 | 处理 |
|------|------|------|
| 验收脚本卡在「等待实例启动」 | 8000/8001 被占用，或 Ollama 没起 | 先关掉手工实例；确认 `curl localhost:11434/api/tags` 通 |
| 验收里限流项 FAIL，放行数不等于 10 | 上一分钟的 Redis 计数还在 | `redis-cli FLUSHDB` 后重跑；或把 `RATE_LIMIT_WINDOW` 设小 |
| Kafka 项 FAIL（收不到事件） | topic 里没有新事件 / broker 没起 | `bash scripts/infra.sh start`；先手工发几条请求 |
| 调优实验很慢 | 每组都触发模型重载 | 正常；用 `--sweeps ctx` 缩小范围，或 `--runs 1` |

---

## 10. 阶段 10：进阶动手题（把知识变成能力）

> 建议按顺序做，每题都给出「改哪里 / 关键点 / 怎么验收」。
> 做完 3 题以上，你对这套架构的理解就超过「跑起来」的水平了。

### 练习 1：加一个新档位和新 API Key

- **改哪里**：`config.py` 的 `API_KEYS`、`RATE_LIMIT`
- **要求**：新增 `sk-enterprise-001` → 档位 `enterprise`，配额 600 次/分钟
- **关键点**：`main.py` 用 `RATE_LIMIT[tier]` 取值，如果 key 的 tier 不在 `RATE_LIMIT` 里会 `KeyError` → 顺手改成 `.get(tier, 10)` 更健壮
- **验收**：新 key 能发请求；Grafana「各档位请求速率」出现新曲线；`/history` 里 `tier=enterprise`

### 练习 2：给 `/history` 加分页

- **改哪里**：`storage.py:recent()`、`main.py:history_recent()`
- **要求**：支持 `?before_id=123&limit=20`（游标分页比 `OFFSET` 更适合大表）
- **关键点**：SQL 加 `AND id < ?`；注意 SQLite/Postgres 占位符用 `backend.placeholder`
- **验收**：连续翻三页，记录不重不漏

### 练习 3：把固定窗口换成滑动窗口（进阶）

- **改哪里**：`ratelimit.py` 的 `RedisRateLimiter.check()`
- **思路**：用 `ZSET` —— `ZREMRANGEBYSCORE key 0 (now-window)` 清理旧请求 → `ZCARD` 得到窗口内数量 → `ZADD` 记当前请求 → `PEXPIRE`
- **关键点**：仍然要用 Lua 保证原子；key 不再需要窗口序号
- **验收**：写出一个脚本，在第 60 秒前后各发 10 次，验证不会再出现「2 秒 20 次」的边界突刺

### 练习 4：加一个「令牌桶」模式供对比

- **要求**：`RATE_LIMIT_ALGO=fixed|token_bucket` 可选，并在 Grafana 上对比两种算法的拒绝曲线
- **关键点**：令牌桶要存「上次补充时间 + 当前令牌数」，用 Lua 实现即可
- **验收**：突发流量下令牌桶允许短时突发、固定窗口直接拒

### 练习 5：给观测端点加鉴权

- **改哪里**：`main.py` 四个 `@app.get`（`/infra`、`/history`、`/history/stats`，`/metrics` 视需求保留开放）
- **关键点**：用 FastAPI 的 `Depends(verify_api_key)`；`/metrics` 通常留给 Prometheus 内网抓取
- **验收**：不带 key 访问 `/history` 得到 401；Prometheus 抓取仍正常

### 练习 6：给一条请求加 `trace_id`，贯穿三处留痕

- **改哪里**：`main.py`（生成 `trace_id` 塞进 record）、`storage.py`（加一列）、`events.py`（`publish` 时放进信封）
- **关键点**：`trace_id` 用 `uuid4().hex[:16]` 即可；改完记得给 `/history` 返回该字段
- **验收**：用 `trace_id` 能同时在 `logs/gateway.log`、`request_history` 表、Kafka 事件里找到同一次请求

### 练习 7：加 Prometheus 告警规则

- **改哪里**：新建 `monitoring/alerts.yml`，在 `monitoring/prometheus.yml` 里加 `rule_files`
- **建议规则**：
  - `gateway_ratelimit_backend_up == 0`（限流降级，5m）
  - `rate(gateway_history_write_errors_total[5m]) > 0`
  - `gateway_kafka_up == 0`
  - `sum(rate(gateway_requests_total{status=~"5.."}[5m])) / sum(rate(gateway_requests_total[5m])) > 0.05`
- **验收**：Prometheus → Alerts 页能看到规则；`kill` 掉 Redis 后规则变 Pending → Firing

### 练习 8：给历史表加清理任务

- **要求**：每天删掉 7 天前的记录，或把老数据归档到文件
- **关键点**：用 lifespan 里的后台任务（照抄 `_gpu_sampler` 的写法）；SQL 用 `DELETE FROM request_history WHERE ts < ?`
- **验收**：把保留期改成 60 秒，观察行数自动下降

### 练习 9：补一个 `/v1/models` 端点

- **要求**：返回 `MODEL_ROUTES` 里的别名列表，格式对齐 OpenAI 的 `{"object":"list","data":[{"id":"qwen0.5b",...}]}`
- **关键点**：只有十几行，是熟悉 FastAPI 返回结构的好练习
- **验收**：OpenAI SDK 的 `client.models.list()` 能读到

### 练习 10：多 Ollama 后端 + 健康检查

- **要求**：`OLLAMA_URLS` 用逗号分隔多个地址，网关按「轮询 + 失败切换」转发
- **关键点**：记录每个后端的健康状态，暴露成 `gateway_upstream_up{url=...}` 指标
- **验收**：起两个 Ollama（不同端口），停掉一个后请求自动走另一个

### 练习 11：把「降级 JSONL」重放回 Kafka

- **要求**：写一个 `scripts/replay_events.py`，读 `logs/events.jsonl` 逐条重发到 topic，成功后把文件归档
- **关键点**：复用 `EventBus` 的序列化逻辑（信封结构要一致）；注意幂等（下游按 `ts+instance` 去重）
- **验收**：把 Kafka 停掉发 5 条 → 文件里有 5 条 → 起 Kafka → 重放 → 消费者收到这 5 条

### 练习 12：把限流结果也做成事件

- **要求**：新增 `request.allowed` 事件（或在 `request.completed` 里带 `remaining`），用于分析「用户是否长期贴着配额跑」
- **关键点**：`events.py` 的 `EVENT_TYPES` 要同步加进去（否则指标不会预初始化，Grafana 面板显示 No data）
- **验收**：Grafana「Kafka 事件速率（按类型）」多出一条曲线

---

## 附录 A：命令速查

### A.1 启动 / 停止

```bash
# 网关
python main.py                                            # 最简单
uvicorn main:app --host 0.0.0.0 --port 8000 --reload       # 开发热重载
REDIS_URL=... DATABASE_URL=... KAFKA_BOOTSTRAP=... python main.py   # 接三件套

# 基础设施（Redis / PostgreSQL / Kafka）
bash scripts/infra.sh install     # 首次安装（免 root，解包到 ~/opt/infra）
bash scripts/infra.sh start
bash scripts/infra.sh status
bash scripts/infra.sh logs
bash scripts/infra.sh stop
bash scripts/infra.sh restart

# 监控（Prometheus / Grafana）
bash scripts/install_monitoring.sh
bash scripts/monitoring.sh start|stop|restart|status|logs
```

### A.2 发请求

```bash
# 非流式
curl -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-test-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"你好"}]}'

# 流式 + 用量
curl -N -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-prod-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"你好"}],
       "stream":true,"stream_options":{"include_usage":true}}'

# 带 Ollama 参数
curl -s -X POST http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'x-api-key: sk-prod-001' \
  -d '{"model":"qwen0.5b","messages":[{"role":"user","content":"你好"}],
       "options":{"num_ctx":2048,"num_predict":64,"temperature":0}}'
```

### A.3 观测与排障

```bash
curl -s http://localhost:8000/health | python -m json.tool        # 健康 + 三件套状态
curl -s http://localhost:8000/infra  | python -m json.tool        # 详细状态与错误原因
curl -s 'http://localhost:8000/history?limit=5' | python -m json.tool
curl -s 'http://localhost:8000/history/stats?window=600' | python -m json.tool
curl -s http://localhost:8000/metrics | grep '^gateway_'          # 指标
tail -f logs/gateway.log                                          # 明细日志
tail -f logs/events.jsonl                                         # 事件降级文件（有内容说明 Kafka 降级过）
```

### A.4 直连各基础设施

```bash
# Redis
RCLI() { LD_LIBRARY_PATH=~/opt/infra/redis/lib ~/opt/infra/redis/bin/redis-cli -p 6379 "$@"; }
RCLI PING; RCLI KEYS 'gw:rl:*'; RCLI DBSIZE

# PostgreSQL
PSQL() { LD_LIBRARY_PATH=~/opt/infra/postgres/usr/lib/x86_64-linux-gnu \
  ~/opt/infra/postgres/usr/lib/postgresql/18/bin/psql -h 127.0.0.1 -p 5433 -U gw gateway "$@"; }
PSQL -c "SELECT count(*) FROM request_history;"

# SQLite
sqlite3 logs/gateway.db "SELECT count(*) FROM request_history;"

# Kafka
KB=~/opt/infra/kafka/kafka_2.13-4.3.1/bin
$KB/kafka-topics.sh --bootstrap-server localhost:9092 --list
$KB/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic llm-gateway-events --from-beginning --max-messages 5

# 用 Python 按 event 类型统计 Kafka 事件
$KB/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic llm-gateway-events --from-beginning --timeout-ms 5000 2>/dev/null \
  | python -c "import sys,json,collections; print(collections.Counter(
      json.loads(l)['event'] for l in sys.stdin if l.strip().startswith('{')))"
```

### A.5 测试 / 实验

```bash
python verify_infra.py                 # 21 项基础设施验收
python load_test.py                    # 并发压测
python tuning_experiment.py --dry-run  # 看看调优实验计划
python tuning_experiment.py --sweeps ctx
```

---

## 附录 B：文件地图（改代码前先看这张表）

| 文件 | 职责 | 关键位置 |
|------|------|----------|
| `config.py` | 所有环境变量、API Key/档位、模型路由 | 限流与历史的配置在 40~76 行 |
| `auth.py` | API Key 校验 → 返回 tier | 8 行，极短 |
| `router.py` | 模型别名 → Ollama 模型名 | 6 行 |
| `ratelimit.py` | 分布式限流门面 | Lua 脚本 38~44；`check()` 230；`status()` 274 |
| `storage.py` | 请求历史（SQLite/PG） | DDL 44~72；写队列 321；`stats()` 388；P95 440 |
| `events.py` | Kafka 事件生产者 | `EVENT_TYPES` 62；`publish()` 169；降级落盘 228 |
| `prometheus_metrics.py` | 指标定义 + 记录函数 | 请求类 13~46；质量类 54~72；基础设施类 139~209 |
| `metrics.py` | GPU 采样 + TTFT/TPS 计算 | `compute_metrics()` 15 |
| `logger.py` | JSONL 文件日志 | 11 行 |
| `main.py` | FastAPI 应用与请求链路 | lifespan 72；`_persist` 115；`_parse_options` 164；`_stream_chat` 199；`chat_completions` 294；观测端点 457~510 |
| `tuning_experiment.py` | 参数扫描实验 | 参数 163~170 |
| `verify_infra.py` | 21 项验收（含双实例对照） | `check()` 43；参数在文件末尾 `main()` |
| `load_test.py` | 并发压测 | 21 行 |
| `scripts/infra.sh` | Redis/PG/Kafka 免 root 部署 | `install_*` 96~175；`start_*`；`cmd_status` 335 |
| `scripts/install_monitoring.sh` | 下载 Prometheus/Grafana | 版本 14~15 |
| `scripts/monitoring.sh` | 启动监控栈（含 provisioning 渲染） | 环境变量 20~30 |
| `monitoring/prometheus.yml` | 抓取配置（5s） | 6~26 |
| `monitoring/grafana/dashboards/llm-gateway.json` | 5 行 35 面板 | row 面板的 `title` |
| `logs/` | 运行产物 | `gateway.log` / `gateway.db*` / `events.jsonl` / `tuning_*` |

---

## 附录 C：报错对照表（贴上来一搜就能定位）

| 报错 / 现象 | 最可能的原因 | 处理 |
|-------------|--------------|------|
| `422 Unprocessable Entity` | 缺 `x-api-key` 头 | 补上 header |
| `401 Invalid API Key` | key 不在 `config.API_KEYS` | 用 `sk-test-001` / `sk-prod-001` |
| `400 unsupported option: xxx` | `options` 里放了白名单外的参数 | 只用 `OPTION_SPECS` 里的 9 个参数 |
| `400 option temperature out of range [0.0, 2.0]` | 参数越界 | 按范围改值 |
| `429 Rate limit exceeded` | 超出档位配额 | 换 pro key、等窗口过去、或调大配额 |
| `502 Ollama unreachable at http://localhost:11434` | Ollama 没起 | `ollama serve`；`curl localhost:11434/api/tags` |
| `502 Ollama returned 404: model ... not found` | 模型没拉 | `ollama pull qwen2.5:0.5b` |
| 启动日志 `限流后端: Redis 不可用（...）` 或 `/health` 显示 `rate_limit_backend=memory` | Redis 连不上 | `bash scripts/infra.sh start`；`curl /infra` 看 `rate_limit.error` |
| 启动日志 `请求历史不可用（...）` | DB 连不上 / DSN 写错 | 检查 `DATABASE_URL`；PG 端口是 5433 |
| 启动日志 `Kafka 发送失败，降级写本地文件: ...`（或 `/infra` 里 `events.connected=false`） | Kafka 连不上 | `bash scripts/infra.sh start`；`curl /infra` 看 `events.error` |
| `redis-server: error while loading shared libraries: liblzf.so.1` | 解包缺 liblzf1 | `bash scripts/infra.sh install` |
| `initdb/postgres: error while loading shared libraries: libpq.so.5` | 解包缺 libpq5 | 同上；手工运行时设 `LD_LIBRARY_PATH` |
| `kafka-storage.sh format` 失败 | Java 版本低 / 目录权限 | `java -version` ≥ 17；看 `~/opt/infra/logs/kafka-format.log` |
| Grafana `RangeError: Maximum call stack size exceeded` | 仪表盘 uid 与 folderUid 重名 | 保持 `llm-gateway` vs `llm-gateway-folder`；`monitoring.sh` 会校验 |
| Grafana 面板 `No data` | 指标尚无数据（带标签 Counter 未初始化） | 代码已零值预初始化；先发几条请求 |
| `Address already in use` | 端口被占 | `ss -ltnp \| grep <端口>`；或用 `SERVER_PORT=800x` |
| `/history` 空但请求 200 | 历史没启/还没 flush | `curl /infra` 看 `history.enabled`；等 1 秒 |
| `database is locked`（SQLite） | 其它进程持锁 | 检查有没有第二个网关进程写同一 db |

---

## 附录 D：术语表

| 术语 | 一句话解释 |
|------|-----------|
| **网关（Gateway）** | 所有请求的统一入口，做鉴权/限流/路由/观测 |
| **OpenAI 兼容** | 接口形状与 OpenAI API 一致，客户端不改代码即可切换 base_url |
| **tier（档位）** | API Key 对应的服务等级，决定限流配额 |
| **限流（Rate limiting）** | 限制单位时间内的请求数 |
| **固定窗口 / 滑动窗口 / 令牌桶** | 三种限流算法，实现复杂度与平滑度递增 |
| **fail-open / fail-closed** | 依赖不可用时「放行」还是「拒绝」 |
| **TTFT** | Time To First Token，首 token 延迟，用户体感的关键指标 |
| **TPS** | Tokens Per Second，生成速度 |
| **P95 / P99** | 95%/99% 的请求都快于该值（比平均值更能反映真实体验） |
| **SSE / NDJSON** | 两种流式传输格式，前者是 `data:` 事件流，后者是一行一个 JSON |
| **WAL** | SQLite 的预写日志模式，让读写不互相阻塞 |
| **DSN** | 数据库连接串（`scheme://user:pwd@host:port/db`） |
| **攒批（batching）** | 先入队、攒够一批再一次性写，提高吞吐 |
| **topic / partition / offset** | Kafka 的消息通道 / 并行流水线 / 消息编号 |
| **producer / consumer group** | 生产消息的一方 / 一组协同消费的消费者 |
| **KRaft** | Kafka 自带的元数据管理（取代 ZooKeeper） |
| **降级（degradation）** | 依赖不可用时退到功能较弱的备用路径，保证主功能可用 |
| **指标 / 日志 / 事件** | 聚合数字 / 明细文本 / 可被多方消费的结构化消息 |
| **PromQL** | Prometheus 的查询语言 |
| **provisioning** | Grafana 用配置文件自动创建数据源/仪表盘（而非手工点） |

---

## 附录 E：自测题（做完就能确认自己真的会了）

1. **为什么限流计数要放 Redis，而不是放进程内存？**
   → 多实例时内存计数各记一份，额度被放大成 `实例数 × 配额`；Redis 让所有实例共享一份。
2. **Lua 脚本里 `if count == 1 then PEXPIRE` 解决什么问题？**
   → 防止「INCR 成功但 PEXPIRE 没执行」导致 key 永不过期、用户被永久限流。
3. **固定窗口的固有缺陷是什么？**
   → 窗口边界处最坏可放行 2 倍配额。
4. **`REDIS_FAIL_OPEN=0` 时 Redis 挂了，用户看到什么？**
   → 429，`backend=redis-down`（配额优先，宁可拒绝也不超额）。
5. **历史写入为什么要攒批？丢了会不会影响用户？**
   → 减少 I/O 次数提升吞吐；丢弃只影响分析数据，不影响请求（有 `history_dropped_total` 计数）。
6. **SQLite 的 WAL 有什么用？**
   → 写不阻塞读；配合 `busy_timeout` 减少 `database is locked`。
7. **Kafka 里为什么用 `api_key` 作为消息 key？**
   → 同一用户的消息进同一分区，分区内有序，便于按用户还原轨迹。
8. **Kafka 挂掉时事件去哪了？**
   → 降级写 `logs/events.jsonl`（多一个 `fallback_reason`），每 10s 尝试重连。
9. **`/health` 和 `/infra` 的区别？**
   → `/health` 是简版状态（3 个字段 + Ollama 是否可用）；`/infra` 是完整状态（含 DSN、队列积压、错误原因）。
10. **`gateway_history_rows` 和 `gateway_history_writes_total` 分别是什么？**
    → 前者是表行数（Gauge），后者是累计写入行数（Counter）。
11. **为什么带标签的 Counter 要 `inc(0)` 预初始化？**
    → 否则序列不存在，Grafana 面板显示 No data。
12. **`/history/stats` 的 P95 是怎么算的？有什么代价？**
    → `COUNT(*)` + `ORDER BY ... LIMIT 1 OFFSET n*0.95`；代价是近似值和两次查询，换来跨数据库可移植。
13. **验证「Redis 真在做共享限流」的关键实验是哪两个结果？**
    → redis 模式 12 次请求只放行 10 次；memory 模式 12 次全放行（反证）。
14. **`num_gpu` 扫描的结论是什么？**
    → 小模型要么全放 GPU（`-1`）要么纯 CPU；半 offload 最差。
15. **为什么改 `num_ctx` 会看到 TTFT 尖峰？**
    → 参数变化触发 Ollama 重载模型（冷启动 ≈ 2.1s）。

### 学习打卡表

| 阶段 | 内容 | 完成 | 备注 |
|------|------|:----:|------|
| 1 | 环境准备 + 第一条请求 | ☐ | |
| 2 | 读懂 main.py 主链路 + 4 个错误码 | ☐ | |
| 3 | 流式 SSE | ☐ | |
| 4 | 指标与 Grafana | ☐ | |
| 5 | Redis 分布式限流 + 两实例对照 | ☐ | |
| 6 | 请求历史（SQLite/PG） | ☐ | |
| 7 | Kafka 事件 | ☐ | |
| 8 | 降级演练 | ☐ | |
| 9 | verify_infra 21/21 + 压测 + 调优 | ☐ | |
| 10 | 进阶练习（≥3 题） | ☐ | |

---

## 写在最后：这套项目的「三个设计原则」

1. **观测是增强，不是依赖**：日志/历史/事件/限流全都允许失败，
   推理主链路永远优先——每个模块你都能找到「失败只记数、不抛错」的代码。
2. **一切自动恢复**：Redis 5s、Kafka 10s 后台重连；历史写失败后下批重连。
   不需要「半夜起来重启网关」。
3. **可观测优先于功能**：`/health`、`/infra`、`/metrics`、Grafana 最后一行，
   任何时刻都能回答「现在哪件基础设施在降级、降了多久、丢了多少」。

把这三条原则用在你自己的项目里，比记住任何一条命令都值钱。


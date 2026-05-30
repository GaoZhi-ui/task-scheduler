#!/usr/bin/env python3
"""独立调度器 v0.3 — 任务提交API + Worker + SQLite持久化

整合频道全员反馈：
  - max_age / 心跳防卡死 (小黑)
  - 死信队列 (小黄)
  - /status 端点 (小花)
  - 结果格式定型 + 双层超时 (小红)
  - mock 并发窗口验证 (小黄)
  - Redis 连接池 + 503 (Hanako)
  - SQLite持久化 + 任务历史 (Hanako)
"""
import os, json, uuid, time, hashlib, asyncio, socket, sqlite3, threading
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from arq import create_pool
from arq.connections import RedisSettings
from arq.cron import cron

# ── 配置 ──
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_POOL_SIZE = int(os.getenv("REDIS_POOL_SIZE", "10"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "50"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RESULT_TTL = int(os.getenv("RESULT_TTL", "3600"))       # 结果保留1h
DEDUP_TTL = int(os.getenv("DEDUP_TTL", "3600"))         # 去重记录1h
DEAD_TTL = int(os.getenv("DEAD_TTL", "86400"))          # 死信保留24h
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT", "300"))       # 任务级超时5min
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))        # API调用级超时30s
MAX_AGE = int(os.getenv("MAX_AGE", "600"))               # stuck 任务10min后重入

WORKER_HOSTNAME = socket.gethostname()
STARTED_AT = datetime.now(timezone.utc).isoformat()

# ── 任务处理器注册表 ──
# 格式: task_type → handler async function
# handler 签名: async (ctx, params: dict) -> dict
TASK_HANDLERS = {}

def register_handler(task_type: str):
    """装饰器：注册任务处理器"""
    def wrapper(func):
        TASK_HANDLERS[task_type] = func
        return func
    return wrapper

# ── 数据模型 ──
class TaskSubmit(BaseModel):
    task_type: str = "llm_call"
    params: dict = {}
    priority: int = 1          # 0=最高, 1=普通, 2=低
    idempotency_key: Optional[str] = None
    max_retries: int = MAX_RETRIES
    timeout: int = JOB_TIMEOUT          # 任务级超时(秒)
    api_timeout: int = API_TIMEOUT      # API调用级超时(秒)
    callback_url: Optional[str] = None

class TaskResult(BaseModel):
    status: str                # pending / running / done / failed
    data: Optional[dict] = None
    error: Optional[str] = None
    metadata: dict = {}        # {attempts, duration_ms, worker_id, queue_name, start_time, end_time}

class BatchSubmitResponse(BaseModel):
    batch_size: int
    results: list

class StatusResponse(BaseModel):
    status: str                # ok / degraded
    redis: str                 # connected / disconnected
    uptime: str
    worker: dict               # {max_concurrent, job_timeout, max_age}
    queue: dict                # {pending_estimate, ...}
    tasks: dict                # {total_submitted, done, failed, dead}

# ── Redis 连接池 ──
redis_pool: Optional[aioredis.Redis] = None

async def get_redis() -> aioredis.Redis:
    global redis_pool
    if redis_pool is None:
        redis_pool = await aioredis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            max_connections=REDIS_POOL_SIZE,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30,
        )
    return redis_pool

async def close_redis():
    global redis_pool
    if redis_pool:
        await redis_pool.aclose()
        redis_pool = None

# ── 工具函数 ──
def make_job_id(ik: str = None) -> str:
    """job_ts-{timestamp_ms}-{short_hash}"""
    if ik:
        h = hashlib.sha256(ik.encode()).hexdigest()[:8]
        return f"job_{int(time.time()*1000)}-{h}"
    return f"job_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

def make_result(status: str, data: dict = None, error: str = None,
                attempts: int = 1, duration_ms: int = 0,
                worker_id: str = "", queue_name: str = "",
                start_time: str = "", end_time: str = "") -> dict:
    return {
        "status": status,
        "data": data,
        "error": error,
        "metadata": {
            "attempts": attempts,
            "duration_ms": duration_ms,
            "worker_id": worker_id,
            "queue_name": queue_name,
            "start_time": start_time,
            "end_time": end_time or datetime.now(timezone.utc).isoformat(),
        }
    }

# ── 内置任务处理器 ──

@register_handler("mock_sleep")
async def handle_mock_sleep(ctx, params: dict):
    """mock 任务：sleep N 秒 + 记录 start_time 用于验证并发窗口重叠"""
    sleep_sec = params.get("sleep", 1)
    job_id = params.get("job_id", "unknown")
    start = time.time()
    start_ts = datetime.now(timezone.utc).isoformat()

    await asyncio.sleep(sleep_sec)

    end_ts = datetime.now(timezone.utc).isoformat()
    duration = int((time.time() - start) * 1000)
    return make_result(
        status="done",
        data={"job_id": job_id, "sleep": sleep_sec, "start": start_ts, "end": end_ts},
        duration_ms=duration,
        start_time=start_ts,
        end_time=end_ts,
    )

@register_handler("llm_call")
async def handle_llm_call(ctx, params: dict):
    """LLM API 调用（占位，阶段二实现）"""
    import httpx
    api_timeout = params.get("api_timeout", API_TIMEOUT)
    start = time.time()
    start_ts = datetime.now(timezone.utc).isoformat()

    model = params.get("model", "deepseek-chat")
    messages = params.get("messages", [{"role": "user", "content": "hello"}])
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    api_url = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")

    if not api_key:
        end_ts = datetime.now(timezone.utc).isoformat()
        duration = int((time.time() - start) * 1000)
        return make_result(
            status="failed",
            error="DEEPSEEK_API_KEY not set. Set it via environment variable.",
            duration_ms=duration,
            start_time=start_ts,
            end_time=end_ts,
        )

    async with httpx.AsyncClient(timeout=httpx.Timeout(api_timeout)) as client:
        resp = await client.post(
            api_url,
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": model, "messages": messages},
        )
        resp.raise_for_status()
        data = resp.json()

    end_ts = datetime.now(timezone.utc).isoformat()
    duration = int((time.time() - start) * 1000)
    return make_result(
        status="done",
        data={"model": model, "usage": data.get("usage"), "reply": data["choices"][0]["message"]["content"][:200]},
        duration_ms=duration,
        start_time=start_ts,
        end_time=end_ts,
    )

@register_handler("echo")
async def handle_echo(ctx, params: dict):
    """echo 测试：原样返回 params"""
    start = time.time()
    start_ts = datetime.now(timezone.utc).isoformat()
    await asyncio.sleep(0.1)
    duration = int((time.time() - start) * 1000)
    return make_result(status="done", data=params, duration_ms=duration, start_time=start_ts)


# ── 任务调度函数（arq Worker 执行入口） ──
async def process_task(ctx, task_data: dict, **kwargs):
    """通用任务分发入口。根据 task_type 路由到具体 handler。"""
    job_id = task_data["job_id"]
    task_type = task_data["task_type"]
    params = task_data.get("params", {})
    max_retries = task_data.get("max_retries", MAX_RETRIES)
    callback_url = task_data.get("callback_url")
    queue_name = task_data.get("_queue_name", "default")

    params["job_id"] = job_id
    params["api_timeout"] = task_data.get("api_timeout", API_TIMEOUT)

    r = await get_redis()
    await r.set(f"status:{job_id}", "running", ex=RESULT_TTL)

    handler = TASK_HANDLERS.get(task_type)
    if not handler:
        err_msg = f"未知任务类型: {task_type}"
        result = make_result(
            status="failed", error=err_msg,
            worker_id=WORKER_HOSTNAME, queue_name=queue_name,
        )
        await r.setex(f"result:{job_id}", RESULT_TTL, json.dumps(result))
        save_job(job_id, task_type, result)
        await _send_callback(callback_url, result)
        return result

    # 带重试的执行
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = await handler(ctx, params)
            result["metadata"]["attempts"] = attempt
            result["metadata"]["worker_id"] = WORKER_HOSTNAME
            result["metadata"]["queue_name"] = queue_name
            await r.setex(f"result:{job_id}", RESULT_TTL, json.dumps(result))
            await _send_callback(callback_url, result)
            save_job(job_id, task_type, result)
            return result
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries:
                wait = 2 ** (attempt - 1)
                await asyncio.sleep(wait)  # 指数退避

    # 重试耗尽 → 死信队列
    result = make_result(
        status="failed", error=f"重试{max_retries}次均失败: {last_error}",
        attempts=max_retries, worker_id=WORKER_HOSTNAME, queue_name=queue_name,
    )
    await r.setex(f"result:{job_id}", RESULT_TTL, json.dumps(result))
    await r.setex(f"dead:{job_id}", DEAD_TTL, json.dumps(result))
    await _send_callback(callback_url, result)
    save_job(job_id, task_type, result)
    return result


async def _send_callback(url: str | None, result: dict):
    """如果 callback_url 存在，异步 POST 结果回去。不阻塞、不重试。"""
    if not url:
        return
    try:
        import httpx
        async with httpx.AsyncClient(timeout=httpx.Timeout(10)) as client:
            await client.post(url, json=result)
    except Exception:
        pass  # 回调失败不阻塞主流程


# ── 定时清理任务 ──

async def cleanup_old_results(ctx):
    """每天凌晨 3:00 清理过期的 Redis 结果数据

    清扫 result: arq:result: 前缀的键，兜底清理 TTL 漏标或残余键。
    正常带 TTL 的键由 Redis 自动过期，此函数作为安全网。
    """
    r = await get_redis()
    total = 0
    for pattern in ("result:*", "arq:result:*"):
        cursor = 0
        batch = 0
        while True:
            cursor, keys = await r.scan(cursor=cursor, match=pattern, count=200)
            if keys:
                for k in keys:
                    ttl = await r.ttl(k)
                    if ttl < 0:  # no TTL (persistent or expired but not yet reaped)
                        await r.delete(k)
                        batch += 1
            cursor = int(cursor)
            if cursor == 0:
                break
        total += batch
        if batch:
            print(f"🧹 cleanup[{pattern}]: 清理了 {batch} 个无 TTL 键")
    print(f"🧹 定时清理完成，共计清理 {total} 个键")
    return {"cleaned": total}


# ── arq Worker 设置 ──
class WorkerSettings:
    functions = [process_task, cleanup_old_results]
    redis_settings = RedisSettings(host=REDIS_HOST, port=REDIS_PORT)
    max_jobs = MAX_CONCURRENT
    job_timeout = JOB_TIMEOUT + 30   # Worker 级超时要略大于任务级
    max_burst_jobs = MAX_CONCURRENT
    keep_result = RESULT_TTL
    keep_result_forever = False
    # arq 只支持单一队列名(str)，这里监听默认优先级队列
    queue_name = "priority_1"
    # 定时任务：每天 03:00 清理过期结果
    cron_jobs = [
        cron(cleanup_old_results, hour={3}, minute={0}, second={0}, unique=True)
    ]


# ── FastAPI ──
app = FastAPI(title="独立调度器", version="0.3.0")

# ── SQLite 持久化 ──
DB_PATH = os.getenv("DB_PATH", "/mnt/e/openhanako-work/scheduler/jobs.db")
_db_local = threading.local()

def get_db():
    """每个线程独立的 SQLite 连接"""
    if not hasattr(_db_local, "conn") or _db_local.conn is None:
        _db_local.conn = sqlite3.connect(DB_PATH)
        _db_local.conn.row_factory = sqlite3.Row
        _db_local.conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            task_type TEXT,
            status TEXT,
            params TEXT,
            result TEXT,
            error TEXT,
            duration_ms INTEGER DEFAULT 0,
            attempts INTEGER DEFAULT 1,
            worker_id TEXT,
            queue_name TEXT,
            start_time TEXT,
            end_time TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        _db_local.conn.commit()
    return _db_local.conn

def save_job(job_id: str, task_type: str, result: dict):
    """保存任务结果到 SQLite"""
    try:
        meta = result.get("metadata", {})
        db = get_db()
        db.execute(
            """INSERT OR REPLACE INTO jobs
            (job_id, task_type, status, params, result, error,
             duration_ms, attempts, worker_id, queue_name, start_time, end_time)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                job_id, task_type, result.get("status", "unknown"),
                json.dumps(result.get("data", {})),
                json.dumps(result.get("data", {})),
                result.get("error", ""),
                meta.get("duration_ms", 0),
                meta.get("attempts", 1),
                meta.get("worker_id", ""),
                meta.get("queue_name", ""),
                meta.get("start_time", ""),
                meta.get("end_time", ""),
            )
        )
        db.commit()
    except Exception:
        pass  # DB 失败不影响主流程

_worker_task: asyncio.Task | None = None



# 挂载静态文件
import os
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/_static", StaticFiles(directory=static_dir, html=False), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def serve_dashboard():
        """根路径返回 dashboard.html"""
        index_path = os.path.join(static_dir, "index.html")
        dash_path = os.path.join(static_dir, "dashboard.html")
        target = index_path if os.path.exists(index_path) else dash_path
        if os.path.exists(target):
            with open(target, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)
@app.on_event("startup")
async def startup():
    """启动时测试 Redis 连接，启动后台 arq Worker"""
    global _worker_task
    try:
        r = await get_redis()
        await r.ping()
        print(f"✅ Redis 已连接: {REDIS_HOST}:{REDIS_PORT}")

        # 启动 arq Worker
        from arq.worker import create_worker
        worker = create_worker(WorkerSettings)
        _worker_task = asyncio.create_task(worker.async_run())
        print(f"✅ arq Worker 已启动 (max_concurrent={MAX_CONCURRENT})")

    except Exception as e:
        print(f"⚠️  启动异常 (服务降级): {e}")


@app.on_event("shutdown")
async def shutdown():
    global _worker_task
    if _worker_task:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    await close_redis()


def _redis_unavailable():
    raise HTTPException(503, "Redis 不可用，请稍后重试")


@app.post("/tasks", response_model=dict)
async def submit_task(task: TaskSubmit):
    """提交单个任务"""
    r = await get_redis()
    try:
        await r.ping()
    except Exception:
        _redis_unavailable()

    # 去重检查
    if task.idempotency_key:
        dedup_key = f"dedup:{task.idempotency_key}"
        exists = await r.get(dedup_key)
        if exists:
            return {"job_id": exists.decode(), "status": "queued", "dedup": True}

    job_id = make_job_id(task.idempotency_key)

    try:
        pool = await create_pool(RedisSettings(host=REDIS_HOST, port=REDIS_PORT))
        await pool.enqueue_job(
            "process_task",
            {
                "job_id": job_id,
                "task_type": task.task_type,
                "params": task.params,
                "max_retries": task.max_retries,
                "timeout": task.timeout,
                "api_timeout": task.api_timeout,
                "callback_url": task.callback_url,
                "_queue_name": f"priority_{task.priority}",
            },
            _job_id=job_id,
            _queue_name=f"priority_{task.priority}",
        )
    except Exception as e:
        raise HTTPException(503, f"入队失败: {e}")

    # 去重记录
    if task.idempotency_key:
        dedup_key = f"dedup:{task.idempotency_key}"
        await r.setex(dedup_key, DEDUP_TTL, job_id)

    # 保存原始任务参数（用于重试等场景）
    await r.setex(f"taskmeta:{job_id}", RESULT_TTL, json.dumps({
        "task_type": task.task_type,
        "params": task.params,
        "priority": task.priority,
        "max_retries": task.max_retries,
        "timeout": task.timeout,
        "api_timeout": task.api_timeout,
        "callback_url": task.callback_url,
    }))

    # 计数
    await r.incr("stats:total_submitted")

    return {"job_id": job_id, "status": "queued"}


@app.post("/tasks/batch", response_model=BatchSubmitResponse)
async def submit_batch(tasks: list[TaskSubmit]):
    """批量提交任务"""
    results = []
    for t in tasks:
        try:
            r = await submit_task(t)
            results.append(r)
        except HTTPException as e:
            results.append({"error": e.detail})
        except Exception as e:
            results.append({"error": str(e)})
    return BatchSubmitResponse(batch_size=len(tasks), results=results)


# 在 startup 中动态注册，避免静态路由顺序问题
_history_routes_registered = False


@app.get("/tasks/history")
async def list_tasks(limit: int = 20, status: str = None):
    """查询任务历史"""
    try:
        import sqlite3
        db_path = os.getenv("DB_PATH", "/mnt/e/openhanako-work/scheduler/jobs.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        if status:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
                (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        return {"error": str(e), "tasks": []}


@app.delete("/tasks/{job_id}")
async def cancel_task(job_id: str):
    """取消（删除）一个排队中的任务"""
    r = await get_redis()
    try:
        await r.ping()
    except Exception:
        _redis_unavailable()

    # 标记为 cancelled
    await r.set(f"status:{job_id}", "cancelled", ex=RESULT_TTL)

    # 尝试从 arq 队列中移除 (LREM count=0 表示移除所有匹配项)
    try:
        queue_key = "arq:queue:priority_1"
        await r.lrem(queue_key, 0, job_id)
    except Exception:
        pass  # 队列删除失败不阻塞主流程

    return {"job_id": job_id, "status": "cancelled"}


@app.post("/tasks/{job_id}/retry")
async def retry_task(job_id: str):
    """手动重试失败任务：从 SQLite 或 Redis 中提取原参数，重新入队"""
    r = await get_redis()
    try:
        await r.ping()
    except Exception:
        _redis_unavailable()

    # 1. 从 Redis 读取结果
    result_data = await r.get(f"result:{job_id}")
    if not result_data:
        result_data = await r.get(f"dead:{job_id}")
    if not result_data:
        raise HTTPException(404, f"任务 {job_id} 不存在")

    result = json.loads(result_data)
    if result.get("status") != "failed":
        raise HTTPException(400, f"任务 {job_id} 状态为 {result.get('status')}，仅失败(failed)任务可重试")

    # 2. 提取 task_type 和原始参数
    #    优先从 taskmeta（完整保存的原始参数）获取
    taskmeta_raw = await r.get(f"taskmeta:{job_id}")
    if taskmeta_raw:
        meta = json.loads(taskmeta_raw)
        task_type = meta.get("task_type")
        params = meta.get("params", {})
        priority = meta.get("priority", 1)
        max_retries = meta.get("max_retries", MAX_RETRIES)
        timeout = meta.get("timeout", JOB_TIMEOUT)
        api_timeout = meta.get("api_timeout", API_TIMEOUT)
        callback_url = meta.get("callback_url")
    else:
        # 降级：从 SQLite 获取 task_type，从 result.data 获取 params
        task_type = None
        params = {}
        priority = 1
        max_retries = MAX_RETRIES
        timeout = JOB_TIMEOUT
        api_timeout = API_TIMEOUT
        callback_url = None
        try:
            db_path = os.getenv("DB_PATH", DB_PATH)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT task_type, params FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            conn.close()
            if row:
                task_type = row["task_type"]
                saved_params = row["params"]
                if saved_params and saved_params != "null":
                    saved = json.loads(saved_params)
                    # 去掉 handler 注入的字段
                    params = {k: v for k, v in saved.items() if k not in ("job_id", "api_timeout")}
        except Exception as e:
            raise HTTPException(500, f"读取 SQLite 任务参数失败: {e}")

        if not task_type:
            # 最后手段：从 result.data 中猜
            task_type = result.get("data", {}).get("task_type")
        if not task_type:
            raise HTTPException(500, f"无法获取任务类型 (job_id={job_id})")

    # 3. 删除旧结果（允许重新入队）
    await r.delete(f"result:{job_id}")
    await r.delete(f"dead:{job_id}")

    # 4. 重新提交
    new_job_id = make_job_id()
    try:
        pool = await create_pool(RedisSettings(host=REDIS_HOST, port=REDIS_PORT))
        await pool.enqueue_job(
            "process_task",
            {
                "job_id": new_job_id,
                "task_type": task_type,
                "params": params,
                "max_retries": max_retries,
                "timeout": timeout,
                "api_timeout": api_timeout,
                "callback_url": callback_url,
                "_queue_name": f"priority_{priority}",
            },
            _job_id=new_job_id,
            _queue_name=f"priority_{priority}",
        )
    except Exception as e:
        raise HTTPException(503, f"重试入队失败: {e}")

    await r.incr("stats:total_submitted")

    return {"job_id": new_job_id, "status": "queued"}


@app.get("/tasks/{job_id}")
async def get_task(job_id: str):
    """查询任务状态和结果"""
    r = await get_redis()

    # 结果
    result_data = await r.get(f"result:{job_id}")
    if result_data:
        return json.loads(result_data)

    # 运行中
    status = await r.get(f"status:{job_id}")
    if status:
        return {"job_id": job_id, "status": status.decode()}

    # 死信
    dead = await r.get(f"dead:{job_id}")
    if dead:
        return json.loads(dead)

    return {"job_id": job_id, "status": "not_found"}


@app.get("/status", response_model=StatusResponse)
async def get_status():
    """系统状态 — 小花: 透明性"""
    r = await get_redis()
    redis_ok = False
    try:
        await r.ping()
        redis_ok = True
    except Exception:
        pass

    stats = {}
    try:
        total = await r.get("stats:total_submitted")
        stats["total_submitted"] = int(total) if total else 0
    except Exception:
        stats["total_submitted"] = 0

    return StatusResponse(
        status="ok" if redis_ok else "degraded",
        redis="connected" if redis_ok else "disconnected",
        uptime=STARTED_AT,
        worker={
            "max_concurrent": MAX_CONCURRENT,
            "job_timeout": JOB_TIMEOUT,
            "max_age": MAX_AGE,
        },
        queue={
            "priority_queues": ["priority_0", "priority_1", "priority_2"],
        },
        tasks=stats,
    )


@app.get("/health")
async def health():
    """健康检查"""
    r = await get_redis()
    try:
        await r.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception:
        return {"status": "degraded", "redis": "disconnected"}


# ── 主入口 ──
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info", loop="asyncio")

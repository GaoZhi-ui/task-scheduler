# 独立任务调度器 ⚡

基于 arq + FastAPI + Redis 的异步任务调度器，支持并发执行 LLM 调用、搜索、文件处理等任务。

## 架构

```
任务提交 API (FastAPI) → Redis 队列 → arq Worker 并发执行 → 结果回写
     ↑                          ↑                        ↓
  submit.py 桥接          WSL Alpine                  SQLite/Redis
```

## 功能

- **并发执行** — 基于 arq，单机数百并发，支持 P0~P3 三级队列
- **任务管理** — 提交/取消/重试/查询，完整 CRUD
- **失败重试** — 指数退避（1s→2s→4s），重试耗尽入死信队列
- **去重** — idempotency_key 防止重复提交
- **持久化** — SQLite 存储任务历史，Redis 结果 TTL 自动过期
- **Cron** — 内置定时清理任务（每日 3:00）
- **LLM 调用** — 内置 DeepSeek API 调用器
- **监控面板** — 浏览器访问根路径即可查看实时看板
- **幂等回调** — callback_url 异步通知

## 快速启动

```bash
# 安装依赖
pip install arq fastapi uvicorn httpx redis

# 确保 Redis 运行中
redis-server

# 配置环境变量（用你自己的 Key）
export DEEPSEEK_API_KEY="sk-your-key-here"

# 启动调度器
python3 main.py

# 提交任务
curl -X POST http://127.0.0.1:8765/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"echo","params":{"msg":"hello"}}'

# 查看面板
open http://127.0.0.1:8765/
```

> 代码中不含任何 API Key，每个人用自己的。
> 调度器默认从环境变量 `DEEPSEEK_API_KEY` 读取，不配置则 LLM 调用不可用，其他功能不受影响。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /tasks | 提交任务 |
| POST | /tasks/batch | 批量提交 |
| GET | /tasks/{id} | 查询任务 |
| GET | /tasks/history | 历史记录 |
| DELETE | /tasks/{id} | 取消任务 |
| POST | /tasks/{id}/retry | 重试失败任务 |
| GET | /status | 系统状态 |
| GET | /health | 健康检查 |
| GET | / | 监控面板 |

## 任务类型

| 类型 | 说明 | params |
|------|------|--------|
| echo | 测试回显 | {"msg":"..."} |
| llm_call | LLM 调用 | {"messages":[...]} |
| mock_sleep | 模拟耗时 | {"sleep":1} |

## 配置

通过环境变量配置：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| DEEPSEEK_API_KEY | (必填) | 你自己的 DeepSeek API Key |
| REDIS_HOST | 127.0.0.1 | Redis 地址 |
| REDIS_PORT | 6379 | Redis 端口 |
| MAX_CONCURRENT | 50 | 最大并发数 |
| MAX_RETRIES | 3 | 最大重试次数 |
| RESULT_TTL | 3600 | 结果保留秒数 |

## 许可证

MIT

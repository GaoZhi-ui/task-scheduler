import httpx, asyncio, sys, time

async def demo():
    base = "http://127.0.0.1:8765"
    
    # Submit 10 tasks
    tasks = []
    for i in range(10):
        task = {
            "task_type": "echo",
            "params": {"msg": f"task_{i}", "sleep": (i % 3) + 1},
            "priority": 0 if i < 3 else 1
        }
        r = await httpx.post(f"{base}/tasks", json=task, timeout=10)
        j = r.json()
        tasks.append(j["job_id"])
        print(f"  Submit task_{i}: {j['job_id'][:20]}... status={j['status']}")

    print(f"\nSubmitted {len(tasks)} tasks. Checking results...\n")
    
    # Poll for results
    done = set()
    for _ in range(15):
        for job_id in tasks:
            if job_id in done: continue
            try:
                r = await httpx.get(f"{base}/tasks/{job_id}", timeout=5)
                j = r.json()
                if j["status"] == "done":
                    meta = j.get("metadata", {})
                    dur = meta.get("duration_ms", 0)
                    print(f"  ✅ {job_id[:20]}... done in {dur}ms")
                    done.add(job_id)
                elif j["status"] == "failed":
                    print(f"  ❌ {job_id[:20]}... failed")
                    done.add(job_id)
            except: pass
        
        if len(done) == len(tasks):
            break
        await asyncio.sleep(1)
    
    print(f"\nCompleted: {len(done)}/{len(tasks)} tasks")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(demo())

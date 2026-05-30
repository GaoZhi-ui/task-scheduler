#!/usr/bin/env python3
"""调度器桥接脚本 — 从 Windows 侧向 WSL 调度器提交任务"""
import subprocess, json, sys, time, base64, tempfile, os

BASE = "http://127.0.0.1:8765"

def api(method, path, data=None):
    """通过 WSL Python 调用调度器 API"""
    body = json.dumps(data) if data else ""
    b64 = base64.b64encode(body.encode()).decode()
    
    py_code = (
        'import urllib.request, json, base64\n'
        f'data = json.loads(base64.b64decode("{b64}").decode()) if "{b64}" else None\n'
        'req = urllib.request.Request(\n'
        f'  "{BASE}{path}",\n'
        '  data=json.dumps(data).encode() if data else None,\n'
        '  headers={"Content-Type": "application/json"},\n'
        f'  method="{method}")\n'
        'try:\n'
        '  resp = urllib.request.urlopen(req, timeout=30)\n'
        '  print(resp.read().decode())\n'
        'except urllib.error.HTTPError as e:\n'
        '  print(e.read().decode())\n'
    )
    py_b64 = base64.b64encode(py_code.encode()).decode()
    
    p = subprocess.run([
        'wsl.exe', '-d', 'Alpine', '--', '/bin/sh', '-c',
        f'echo {py_b64} | base64 -d | python3'
    ], capture_output=True, timeout=30, text=True)
    
    if p.stdout.strip():
        try:
            return json.loads(p.stdout)
        except:
            return {"raw": p.stdout.strip()}
    return {"error": p.stderr.strip() or "empty"}

def submit(task_type, params, **kwargs):
    return api("POST", "/tasks", {"task_type": task_type, "params": params, **kwargs})

def query(job_id):
    return api("GET", f"/tasks/{job_id}")

def wait(job_id, timeout=60):
    for _ in range(timeout):
        r = query(job_id)
        if r.get("status") in ("done", "failed"):
            return r
        time.sleep(1)
    return {"status": "timeout"}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: submit.py <task_type> '<json_params>'")
        print("  submit.py echo '{\"msg\":\"hello\"}'")
        sys.exit(1)
    
    params = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    r = submit(sys.argv[1], params)
    print(json.dumps(r, ensure_ascii=False, indent=2))
    
    if r.get("status") == "queued":
        jid = r["job_id"]
        print(f"\nWaiting for {jid[:25]}...")
        result = wait(jid)
        print(json.dumps(result, ensure_ascii=False, indent=2))

"""端到端集成测试：以真实 uvicorn 子进程启动 API，覆盖全部关键链路。

运行:
  .venv/bin/pytest tests/test_integration.py -v
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
TERMINAL = {"succeeded", "failed", "cancelled", "interrupted"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerHarness:
    __test__ = False  # 非测试类，避免 pytest 误收集

    def __init__(self, data_dir: Path, port: int):
        self.data_dir = data_dir
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.log_file: Path | None = None
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30)

    def start(self) -> None:
        env = {
            **os.environ,
            "PIPELINE_DATA_DIR": str(self.data_dir),
            "PIPELINE_TERM_GRACE_SECONDS": "0.5",
            "PIPELINE_KILL_GRACE_SECONDS": "10",
            "PIPELINE_PROC_POLL_INTERVAL": "0.05",
            "PIPELINE_SCHEDULER_TICK": "0.05",
        }
        # 输出落文件而不是 PIPE：管道缓冲写满会冻结 uvicorn 事件循环
        self.log_file = self.data_dir / "server.log"
        log_fh = open(self.log_file, "ab")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--workers",
                "1",
            ],
            cwd=ROOT,
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                r = httpx.get(f"http://127.0.0.1:{self.port}/health", timeout=1)
                if r.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            if self.proc.poll() is not None:
                out = self.log_file.read_text() if self.log_file else ""
                raise RuntimeError(f"服务器提前退出:\n{out}")
            time.sleep(0.1)
        raise RuntimeError(f"服务器启动超时，日志:\n{self.log_file.read_text()}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc is None:
            return
        try:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        finally:
            self.client.close()
            self.proc = None


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = ServerHarness(tmp_path_factory.mktemp("data"), _free_port())
    srv.start()
    yield srv
    srv.stop()


def step(uid, command, deps=None, timeout=30.0):
    return {"id": uid, "command": command, "depends_on": deps or [],
            "timeout_seconds": timeout}


def submit(client: httpx.Client, name: str, steps: list[dict]) -> int:
    r = client.post("/builds", json={"name": name, "steps": steps})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def wait_build(client: httpx.Client, build_id: int, timeout: float = 40) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        detail = client.get(f"/builds/{build_id}").json()
        if detail["status"] in TERMINAL:
            return detail
        time.sleep(0.1)
    raise AssertionError(f"构建 {build_id} 未在 {timeout}s 内结束")


def step_map(detail: dict) -> dict:
    return {s["id"]: s for s in detail["steps"]}


def assert_pid_dead(pid: int) -> None:
    for _ in range(30):
        if not Path(f"/proc/{pid}").exists():
            return
        time.sleep(0.1)
    raise AssertionError(f"进程 {pid} 在清理后仍然存活")


def command_output(log_payload: dict) -> str:
    """剔除 [runner] 元数据行，只保留命令自身的 stdout/stderr。"""
    return "\n".join(
        line for line in log_payload["content"].splitlines()
        if not line.startswith("[runner]")
    )


# --------------------------------------------------------------- 1. 成功构建

def test_success_build_runs_all_steps(server):
    bid = submit(server.client, "success", [
        step("prepare", "mkdir -p out && echo v1 > out/fw.txt"),
        step("compile", "test -f out/fw.txt && echo compiled", ["prepare"]),
        step("package", "echo packed", ["compile"]),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "succeeded"
    assert all(s["status"] == "succeeded" for s in detail["steps"])
    artifact = server.data_dir / "workdir" / str(bid) / "out" / "fw.txt"
    assert artifact.read_text().strip() == "v1"
    log = server.client.get(f"/builds/{bid}/steps/compile/logs").json()
    assert "compiled" in log["content"]
    assert log["status"] == "succeeded"


# ----------------------------------------------------- 2. 失败跳过 + 独立分支

def test_failure_skips_descendants_but_independent_branch_continues(server):
    bid = submit(server.client, "failure", [
        step("good", "echo ok"),
        step("bad", "echo boom; exit 7"),
        step("child", "echo should-not-run", ["bad"]),
        step("grand", "echo should-not-run", ["child"]),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "failed"
    sm = step_map(detail)
    assert sm["good"]["status"] == "succeeded"
    assert sm["bad"]["status"] == "failed" and sm["bad"]["exit_code"] == 7
    assert sm["child"]["status"] == "skipped"
    assert sm["grand"]["status"] == "skipped"
    log = server.client.get(f"/builds/{bid}/steps/child/logs").json()
    assert "should-not-run" not in command_output(log)  # 跳过的步骤确实没执行


# ------------------------------------------------- 3. 慢子进程：主退出后判失败

def test_lingering_child_is_killed_and_build_fails(server):
    bid = submit(server.client, "slow-child", [
        step(
            "spawn",
            # 不用子 shell 包装，$! 才能拿到真实后台子进程 PID
            "sleep 300 & echo $! > slow.pid && echo main-done && exit 0",
            timeout=10,
        ),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "failed"
    sm = step_map(detail)
    assert sm["spawn"]["status"] == "failed"
    assert "遗留子进程" in (sm["spawn"]["detail"] or "")
    workdir = server.data_dir / "workdir" / str(bid)
    child_pid = int((workdir / "slow.pid").read_text().strip())
    assert_pid_dead(child_pid)  # 进程组清空前不确认终态：此时必须已死
    log = server.client.get(f"/builds/{bid}/steps/spawn/logs").json()
    assert "进程组" in log["content"] and "清空" in log["content"]


# ------------------------------------------------------------------- 4. 超时

def test_timeout_kills_group_and_marks_failed(server):
    bid = submit(server.client, "timeout", [
        step("slow", "echo start; sleep 30; echo end", timeout=1),
    ])
    detail = wait_build(server.client, bid, timeout=20)
    assert detail["status"] == "failed"
    assert step_map(detail)["slow"]["status"] == "failed"
    assert "超时" in (step_map(detail)["slow"]["detail"] or "")
    log = server.client.get(f"/builds/{bid}/steps/slow/logs").json()
    out = command_output(log)
    assert "start" in out
    assert "end" not in out  # 超时点在 sleep，后续输出不会产生


# ------------------------------------------------------ 5. 全局最多两步并行

def test_global_parallelism_capped_at_two(server):
    # 四个各 1.2s 的独立步骤，2 名额 -> 至少两批，总耗时应 >= 2.0s 且 < 4.5s
    steps = [step(f"s{i}", "echo running-$PIPELINE_STEP_ID; sleep 1.2") for i in range(4)]
    t0 = time.time()
    bid = submit(server.client, "parallel-cap", steps)
    detail = wait_build(server.client, bid, timeout=30)
    elapsed = time.time() - t0
    assert detail["status"] == "succeeded"
    assert elapsed >= 2.0, f"并行度疑似超过 2，总耗时仅 {elapsed:.2f}s"
    assert elapsed < 4.5


# ------------------------------------------------------------- 6. 整次取消

def test_cancel_stops_all_running_steps_and_skips_pending(server):
    bid = submit(server.client, "cancel", [
        step("a", "echo a-start; sleep 20; echo a-end"),
        step("b", "echo b-start; sleep 20; echo b-end"),
        step("c", "echo c-should-not-run; sleep 20", ["a"]),
    ])
    # 等两个名额占满
    deadline = time.time() + 10
    running = []
    while time.time() < deadline:
        sm = step_map(server.client.get(f"/builds/{bid}").json())
        running = [k for k, v in sm.items() if v["status"] == "running"]
        if len(running) == 2:
            break
        time.sleep(0.1)
    assert sorted(running) == ["a", "b"]

    r = server.client.post(f"/builds/{bid}/cancel")
    assert r.status_code == 200
    detail = wait_build(server.client, bid, timeout=20)
    sm = step_map(detail)
    assert detail["status"] == "cancelled"
    assert sm["a"]["status"] == "cancelled" and sm["b"]["status"] == "cancelled"
    assert sm["c"]["status"] == "cancelled"
    for uid in ("a", "b"):
        log = server.client.get(f"/builds/{bid}/steps/{uid}/logs").json()
        out = command_output(log)
        assert f"{uid}-start" in out
        assert f"{uid}-end" not in out  # 取消点在 sleep，尾部输出不会产生


def test_repeated_cancel_is_idempotent(server):
    bid = submit(server.client, "cancel-idem", [step("a", "sleep 20")])
    time.sleep(0.5)
    first = server.client.post(f"/builds/{bid}/cancel")
    assert first.status_code == 200
    detail1 = wait_build(server.client, bid, timeout=20)
    second = server.client.post(f"/builds/{bid}/cancel")
    assert second.status_code == 200
    assert "终态" in second.json()["detail"]
    third = server.client.get(f"/builds/{bid}").json()
    assert third["status"] == detail1["status"] == "cancelled"
    assert third["finished_at"] == detail1["finished_at"]  # 终态未被改写


# --------------------------------------- 7. 慢清理不阻塞另一名额（跨构建）

def test_slow_cleanup_does_not_block_other_slot(server):
    # 顽固子进程忽略 SIGTERM（忽略型信号处置会被子进程继承），
    # 主命令退出后清理必须升级到 SIGKILL 流程
    stubborn = (
        "trap '' TERM; sleep 300 & echo $! > stubborn.pid; "
        "echo main-exit; exit 0"
    )
    t0 = time.time()
    bid_busy = submit(server.client, "slow-cleanup", [step("x", stubborn, timeout=10)])
    time.sleep(0.6)  # 确保 busy 已占住一个名额
    bid_fast = submit(server.client, "other-slot", [step("y", "echo fast-ok")])
    detail = wait_build(server.client, bid_fast, timeout=15)
    fast_elapsed = time.time() - t0
    assert detail["status"] == "succeeded"
    # 另一名额可用：快构建在 5s 内完成（被慢清理拖住则要等 KILL 流程更久）
    assert fast_elapsed < 5, f"另一名额疑似被慢清理阻塞: {fast_elapsed:.2f}s"
    busy = wait_build(server.client, bid_busy, timeout=30)
    assert busy["status"] == "failed"  # 顽固子进程最终被 SIGKILL，仍判失败
    workdir = server.data_dir / "workdir" / str(bid_busy)
    pid = int((workdir / "stubborn.pid").read_text().strip())
    assert_pid_dead(pid)


# --------------------------------------------------------- 8. 重启恢复全链路

def test_restart_recovers_and_does_not_kill_new_build(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("restart-data")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = submit(srv.client, "will-be-interrupted", [
            step("a", "sleep 30"),
            step("b", "sleep 30"),
            step("c", "echo never", ["a"]),
        ])
        deadline = time.time() + 10
        while time.time() < deadline:
            sm = step_map(srv.client.get(f"/builds/{bid}").json())
            if sm["a"]["status"] == "running":
                running_pid = _step_pid(data_dir, bid, "a")
                break
            time.sleep(0.1)
        else:
            raise AssertionError("步骤未进入 running")
        assert Path(f"/proc/{running_pid}").exists()
        srv.stop()
        # 进程组随 API 被 tini 之外的场景保留（同会话 SIGTERM 只停 uvicorn）
        assert Path(f"/proc/{running_pid}").exists()
    finally:
        pass

    # 同数据目录重启：先清理残留，再标记 interrupted，然后才对外服务
    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        assert_pid_dead(running_pid)  # 旧构建残留必须被清掉
        detail = srv2.client.get(f"/builds/{bid}").json()
        assert detail["status"] == "interrupted"
        sm = step_map(detail)
        assert sm["a"]["status"] == "interrupted"
        assert sm["b"]["status"] == "interrupted"
        assert sm["c"]["status"] == "interrupted"  # 未跑步骤不再启动

        # 后来的构建必须正常工作，不得被恢复逻辑误杀
        new_id = submit(srv2.client, "after-restart", [
            step("z", "echo alive && test $PIPELINE_BUILD_ID -ge 1"),
        ])
        new_detail = wait_build(srv2.client, new_id)
        assert new_detail["status"] == "succeeded"
    finally:
        srv2.stop()


def _step_pid(data_dir: Path, build_id: int, step_uid: str) -> int:
    conn = sqlite3.connect(data_dir / "pipeline.db")
    try:
        row = conn.execute(
            "SELECT pid FROM steps WHERE build_id=? AND step_uid=?",
            (build_id, step_uid),
        ).fetchone()
        return int(row[0])
    finally:
        conn.close()


# --------------------------------------------------------------- 9. 其他接口

def test_404_and_bad_payloads(server):
    assert server.client.get("/builds/999999").status_code == 404
    assert server.client.post("/builds/999999/cancel").status_code == 404

    bad_cycle = {"name": "cycle", "steps": [
        step("a", "true", ["b"]), step("b", "true", ["a"])
    ]}
    r = server.client.post("/builds", json=bad_cycle)
    assert r.status_code == 400 and "循环" in r.json()["detail"]

    missing_dep = {"name": "ghost", "steps": [step("a", "true", ["x"])]}
    r = server.client.post("/builds", json=missing_dep)
    assert r.status_code == 400 and "不存在" in r.json()["detail"]

    empty = {"name": "empty", "steps": []}
    assert server.client.post("/builds", json=empty).status_code == 422


def test_health_and_docs(server):
    health = server.client.get("/health").json()
    assert health["status"] == "ok" and health["max_parallel"] == 2
    assert server.client.get("/docs").status_code == 200
    assert server.client.get("/openapi.json").status_code == 200


# --------------------------------------------------------------- 10. 回收


def wait_reclaim(client: httpx.Client, build_id: int, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/builds/{build_id}/reclaim")
        assert r.status_code == 200, r.text
        payload = r.json()
        if payload["reclaim_status"] == "reclaimed":
            return payload
        time.sleep(0.1)
    raise AssertionError(f"构建 {build_id} 回收未在 {timeout}s 内完成")


def test_reclaim_terminal_build_deletes_dirs_and_keeps_status(server):
    bid = submit(server.client, "reclaim-ok", [
        step("gen", "mkdir -p out && echo data > out/a.txt && echo log-line"),
    ])
    detail = wait_build(server.client, bid)
    assert detail["status"] == "succeeded"
    assert detail["reclaim_status"] is None
    workdir = server.data_dir / "workdir" / str(bid)
    logdir = server.data_dir / "logs" / str(bid)
    assert (workdir / "out" / "a.txt").exists()
    assert (logdir / "gen.log").exists()

    r = server.client.post(f"/builds/{bid}/reclaim")
    assert r.status_code == 202, r.text
    assert r.json()["reclaim_status"] == "reclaiming"
    assert r.json()["build_status"] == "succeeded"

    final = wait_reclaim(server.client, bid)
    assert final["reclaim_status"] == "reclaimed"
    assert final["build_status"] == "succeeded"
    assert final["finished_at"]
    assert not workdir.exists() and not logdir.exists()

    # 原状态保留供追溯：构建/步骤状态不被回收改写
    detail = server.client.get(f"/builds/{bid}").json()
    assert detail["status"] == "succeeded"
    assert detail["reclaim_status"] == "reclaimed"
    assert detail["steps"][0]["status"] == "succeeded"

    # 日志查询明确显示已回收，不再返回文件内容
    log = server.client.get(f"/builds/{bid}/steps/gen/logs").json()
    assert log["reclaim_status"] == "reclaimed"
    assert log["status"] == "succeeded"
    assert log["content"] == ""


def test_reclaim_rejects_nonterminal_and_unknown(server):
    assert server.client.post("/builds/999999/reclaim").status_code == 404
    assert server.client.get("/builds/999999/reclaim").status_code == 404

    bid = submit(server.client, "reclaim-running", [step("slow", "sleep 8")])
    r = server.client.post(f"/builds/{bid}/reclaim")
    assert r.status_code == 409 and "终态" in r.json()["detail"]
    # 未受理：不产生回收记录，工作目录原样保留
    assert server.client.get(f"/builds/{bid}/reclaim").json()["reclaim_status"] == "none"
    assert (server.data_dir / "workdir" / str(bid)).exists()

    server.client.post(f"/builds/{bid}/cancel")
    wait_build(server.client, bid, timeout=20)
    r = server.client.post(f"/builds/{bid}/reclaim")
    assert r.status_code == 202
    assert wait_reclaim(server.client, bid)["reclaim_status"] == "reclaimed"


def test_reclaim_idempotent_and_concurrent(server):
    bid = submit(server.client, "reclaim-idem", [step("a", "echo hi")])
    wait_build(server.client, bid)

    # 并发重复受理：只有一个请求真正创建记录，响应状态一致
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        rs = list(pool.map(
            lambda _: server.client.post(f"/builds/{bid}/reclaim"), range(4)
        ))
    assert all(r.status_code in (200, 202) for r in rs)
    assert sum(1 for r in rs if r.status_code == 202) == 1
    assert {r.json()["id"] for r in rs} == {bid}
    assert {r.json()["reclaim_status"] for r in rs} <= {"reclaiming", "reclaimed"}

    final = wait_reclaim(server.client, bid)
    # 已完成后再重复：幂等返回当前状态，不重置时间戳、不重新受理
    again = server.client.post(f"/builds/{bid}/reclaim")
    assert again.status_code == 200
    assert again.json()["reclaim_status"] == "reclaimed"
    assert again.json()["finished_at"] == final["finished_at"]
    assert again.json()["requested_at"] == final["requested_at"]


def test_log_endpoint_shows_reclaiming_without_reading_files(server):
    """受理后、删净前的中间态：日志接口明确报 reclaiming 且不读盘。"""
    bid = submit(server.client, "reclaim-inflight", [step("a", "echo marker-line")])
    wait_build(server.client, bid)
    log_file = server.data_dir / "logs" / str(bid) / "a.log"
    assert "marker-line" in log_file.read_text()

    # 模拟清理失败重试中的 reclaiming 记录（无后台任务，状态稳定）
    conn = sqlite3.connect(server.data_dir / "pipeline.db", timeout=10)
    try:
        conn.execute(
            "INSERT INTO reclaims (build_id, status, attempts, detail, requested_at) "
            "VALUES (?, 'reclaiming', 2, '模拟清理失败', '2026-01-01T00:00:00+00:00')",
            (bid,),
        )
        conn.commit()
    finally:
        conn.close()

    log = server.client.get(f"/builds/{bid}/steps/a/logs").json()
    assert log["reclaim_status"] == "reclaiming"
    assert log["status"] == "succeeded"
    assert log["content"] == ""  # 文件仍在盘上，但绝不读到半删内容
    assert log_file.exists()
    shown = server.client.get(f"/builds/{bid}/reclaim").json()
    assert shown["reclaim_status"] == "reclaiming"
    assert shown["attempts"] == 2

    # 对已存在的 reclaiming 记录再受理：幂等返回并拉起清理任务直至删净
    r = server.client.post(f"/builds/{bid}/reclaim")
    assert r.status_code == 200
    assert r.json()["reclaim_status"] == "reclaiming"
    final = wait_reclaim(server.client, bid)
    assert final["reclaim_status"] == "reclaimed"
    assert not (server.data_dir / "workdir" / str(bid)).exists()
    assert not log_file.exists()


def test_reclaim_does_not_touch_other_builds(server):
    bid_a = submit(server.client, "reclaim-a", [step("x", "echo keep-a | tee note.txt")])
    bid_b = submit(server.client, "reclaim-b", [step("y", "echo keep-b | tee note.txt")])
    wait_build(server.client, bid_a)
    wait_build(server.client, bid_b)

    assert server.client.post(f"/builds/{bid_a}/reclaim").status_code == 202
    wait_reclaim(server.client, bid_a)

    assert not (server.data_dir / "workdir" / str(bid_a)).exists()
    # 另一构建的工作目录与日志完整可读
    assert (server.data_dir / "workdir" / str(bid_b) / "note.txt").exists()
    log_b = server.client.get(f"/builds/{bid_b}/steps/y/logs").json()
    assert log_b["reclaim_status"] is None
    assert "keep-b" in log_b["content"]
    detail_b = server.client.get(f"/builds/{bid_b}").json()
    assert detail_b["reclaim_status"] is None


def test_reclaim_resumes_after_restart(tmp_path_factory):
    """受理后进程退出、目录未删净：重启必须继续清理，删净后才报完成。"""
    data_dir = tmp_path_factory.mktemp("reclaim-restart")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        bid = submit(srv.client, "reclaim-pending", [step("a", "echo hello")])
        wait_build(srv.client, bid)
    finally:
        srv.stop()

    # 模拟"受理后进程即退出"：直接落一条 reclaiming 记录，目录仍留在盘上
    conn = sqlite3.connect(data_dir / "pipeline.db")
    try:
        conn.execute(
            "INSERT INTO reclaims (build_id, status, requested_at) "
            "VALUES (?, 'reclaiming', '2026-01-01T00:00:00+00:00')",
            (bid,),
        )
        conn.commit()
    finally:
        conn.close()
    assert (data_dir / "workdir" / str(bid)).exists()
    assert (data_dir / "logs" / str(bid)).exists()

    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        assert srv2.client.get("/health").json()["reclaim_resumed"] == 1
        final = wait_reclaim(srv2.client, bid)
        assert final["reclaim_status"] == "reclaimed"
        assert not (data_dir / "workdir" / str(bid)).exists()
        assert not (data_dir / "logs" / str(bid)).exists()
        log = srv2.client.get(f"/builds/{bid}/steps/a/logs").json()
        assert log["reclaim_status"] == "reclaimed"
        assert log["content"] == ""
    finally:
        srv2.stop()

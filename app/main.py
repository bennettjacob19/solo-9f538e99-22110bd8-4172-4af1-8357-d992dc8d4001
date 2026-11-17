"""FastAPI 应用：路由、序列化与启动恢复。"""
from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response

from . import __version__, config
from .db import Database
from .models import BuildCreate, BuildOut, CancelOut, LogOut, ReclaimOut, StepOut
from .reclaim import Reclaimer
from .scheduler import Scheduler

db = Database()
scheduler = Scheduler(db)
reclaimer = Reclaimer(db)


def _step_out(row) -> StepOut:
    return StepOut(
        id=row["step_uid"],
        command=row["command"],
        depends_on=json.loads(row["depends_on"]),
        timeout_seconds=row["timeout_seconds"],
        status=row["status"],
        exit_code=row["exit_code"],
        detail=row["detail"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


async def _build_out(build_id: int) -> BuildOut:
    row = await db.get_build(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    steps = [_step_out(r) for r in await db.get_steps(build_id)]
    reclaim = await db.get_reclaim(build_id)
    return BuildOut(
        id=row["id"],
        name=row["name"],
        status=row["status"],
        cancel_requested=bool(row["cancel_requested"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        detail=row["detail"],
        steps=steps,
        reclaim_status=reclaim["status"] if reclaim is not None else None,
    )


def _reclaim_out(build_row, reclaim_row) -> ReclaimOut:
    if reclaim_row is None:
        return ReclaimOut(
            id=build_row["id"],
            build_status=build_row["status"],
            reclaim_status="none",
            attempts=0,
            requested_at=None,
            finished_at=None,
            detail=None,
        )
    return ReclaimOut(
        id=build_row["id"],
        build_status=build_row["status"],
        reclaim_status=reclaim_row["status"],
        attempts=reclaim_row["attempts"],
        requested_at=reclaim_row["requested_at"],
        finished_at=reclaim_row["finished_at"],
        detail=reclaim_row["detail"],
    )


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # 顺序不可颠倒：先落库连接，再清空旧构建残留并标记中断，然后接管
    # 上次退出前未删净的回收任务，最后才对外服务
    await db.connect()
    app.state.recovery = await scheduler.recover()
    app.state.reclaim_resumed = await reclaimer.resume_pending()
    try:
        yield
    finally:
        await db.close()


app = FastAPI(
    title="固件构建流水线 API",
    version=__version__,
    description=(
        "接收最多 12 步的 JSON DAG，在挂载工作目录中执行真实命令；"
        "全局最多两步并行，依赖全成功才启动，失败后代跳过，"
        "超时/取消终止整个进程组，支持逐步状态与日志查询；"
        "终态构建可按构建号回收工作目录与日志（幂等，未删净持续重试）。"
    ),
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": __version__,
        "max_steps": config.MAX_STEPS,
        "max_parallel": config.MAX_PARALLEL,
        "recovery": getattr(app.state, "recovery", None),
        "reclaim_resumed": getattr(app.state, "reclaim_resumed", None),
    }


@app.post("/builds", response_model=BuildOut, status_code=201)
async def create_build(payload: BuildCreate) -> BuildOut:
    try:
        build_id = await scheduler.submit(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _build_out(build_id)


@app.get("/builds", response_model=list[BuildOut])
async def list_builds(limit: int = Query(100, ge=1, le=500)) -> list[BuildOut]:
    rows = await db.list_builds(limit)
    return [await _build_out(r["id"]) for r in rows]


@app.get("/builds/{build_id}", response_model=BuildOut)
async def get_build(build_id: int) -> BuildOut:
    return await _build_out(build_id)


@app.post("/builds/{build_id}/cancel", response_model=CancelOut)
async def cancel_build(build_id: int) -> CancelOut:
    row, message = await scheduler.cancel(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    # 重新读取以拿到 cancel_requested 的真实落库值（终态上的重复取消不会置位）
    fresh = await db.get_build(build_id)
    return CancelOut(
        id=row["id"],
        status=fresh["status"],
        cancel_requested=bool(fresh["cancel_requested"]),
        detail=message,
    )


@app.post("/builds/{build_id}/reclaim", response_model=ReclaimOut)
async def reclaim_build(build_id: int, response: Response) -> ReclaimOut:
    """按构建号回收：删除该构建的工作目录与日志，保留数据库原状态供追溯。

    仅受理构建及全部步骤均已终态的记录；重复/并发请求幂等返回同一状态。
    """
    build, reclaim, created, message = await reclaimer.request(build_id)
    if build is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    if reclaim is None:
        raise HTTPException(status_code=409, detail=message)
    response.status_code = 202 if created else 200
    return _reclaim_out(build, reclaim)


@app.get("/builds/{build_id}/reclaim", response_model=ReclaimOut)
async def get_reclaim(build_id: int) -> ReclaimOut:
    build = await db.get_build(build_id)
    if build is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    reclaim = await db.get_reclaim(build_id)
    return _reclaim_out(build, reclaim)


@app.get("/builds/{build_id}/steps/{step_id}/logs", response_model=LogOut)
async def get_step_log(
    build_id: int,
    step_id: str,
    tail: int = Query(0, ge=0, description="只返回最后 N 行，0 表示返回全部（受字节上限约束）"),
) -> LogOut:
    step = await db.get_step(build_id, step_id)
    if step is None:
        raise HTTPException(status_code=404, detail="构建或步骤不存在")
    reclaim = await db.get_reclaim(build_id)
    if reclaim is not None:
        # 回收受理后即不再读盘：文件可能正在删除（半删）或已删除，
        # 明确以 reclaim_status 告知，步骤原状态保留在 status 字段
        return LogOut(
            build_id=build_id,
            step_id=step_id,
            status=step["status"],
            truncated=False,
            content="",
            reclaim_status=reclaim["status"],
        )
    path = Path(step["log_path"])
    if not path.exists():
        return LogOut(build_id=build_id, step_id=step_id, status=step["status"],
                      truncated=False, content="")
    # 在线程池读文件，避免大日志阻塞事件循环
    raw = await asyncio.to_thread(path.read_bytes)
    truncated = False
    if len(raw) > config.LOG_MAX_BYTES:
        raw = raw[-config.LOG_MAX_BYTES:]
        truncated = True
    content = raw.decode("utf-8", errors="replace")
    if tail > 0:
        lines = content.splitlines()[-tail:]
        content = "\n".join(lines)
    return LogOut(
        build_id=build_id,
        step_id=step_id,
        status=step["status"],
        truncated=truncated,
        content=content,
    )

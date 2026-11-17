"""构建回收：终态构建的工作目录与日志清理。

设计要点：
- 回收状态独立落库（reclaims 表），构建/步骤原状态保持不动，供追溯。
- 受理即落库 'reclaiming'，此后日志接口不再读盘（文件可能正在删除），
  只回报回收状态；两个目录都确认不存在后才落 'reclaimed'，未删净不报完成。
- 删除失败按 RECLAIM_RETRY_SECONDS 退避重试；进程重启后由启动恢复继续清理，
  状态机不会出现"已回收但盘上仍有残留"。
- 只删 workdir/<id> 与 logs/<id> 两个精确路径，不持调度锁、不碰名额，
  不影响其他构建。
"""
from __future__ import annotations

import asyncio
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import config
from .db import Database
from .models import BUILD_TERMINAL, STEP_TERMINAL

log = logging.getLogger("pipeline.reclaim")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_paths(build_id: int) -> tuple[Path, Path]:
    """回收涉及的精确路径；build_id 为 int，不存在路径穿越空间。"""
    return config.WORKDIR_ROOT / str(build_id), config.LOG_ROOT / str(build_id)


def _remove_path(path: Path) -> None:
    """删除文件或目录树；不存在视为已删（幂等）。"""
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    except FileNotFoundError:
        pass


class Reclaimer:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._tasks: dict[int, asyncio.Task[None]] = {}

    # ------------------------------------------------------------------ 受理

    async def request(
        self, build_id: int
    ) -> tuple[Optional[Any], Optional[Any], bool, str]:
        """受理回收请求，返回 (构建行, 回收记录, 是否本次新建, 说明)。

        构建行为 None -> 路由层 404；回收记录为 None -> 路由层 409（未终态）。
        重复/并发请求返回已存在的记录，created=False，保证状态一致。
        """
        build = await self.db.get_build(build_id)
        if build is None:
            return None, None, False, "构建不存在"

        existing = await self.db.get_reclaim(build_id)
        if existing is not None:
            self._ensure_task(build_id, existing["status"])
            return build, existing, False, "回收请求已受理过，返回当前回收状态"

        # 仅受理构建及全部步骤均已终态的记录；构建终态落库发生在所有步骤
        # 任务（含进程组清理与日志收尾）完成之后，因此此时执行器已不再写盘
        if build["status"] not in BUILD_TERMINAL:
            return (
                build,
                None,
                False,
                f"构建未处于终态（当前 {build['status']}），暂不能回收",
            )
        steps = await self.db.get_steps(build_id)
        non_terminal = [s["step_uid"] for s in steps if s["status"] not in STEP_TERMINAL]
        if non_terminal:
            return (
                build,
                None,
                False,
                f"存在未终态步骤 {non_terminal}，暂不能回收",
            )

        created = await self.db.create_reclaim(build_id, _now())
        if not created:
            # 并发请求已抢先落库，以其记录为准
            existing = await self.db.get_reclaim(build_id)
            self._ensure_task(build_id, existing["status"])
            return build, existing, False, "回收请求已受理过，返回当前回收状态"

        # 先读出响应快照（必为 reclaiming），再启动清理任务，避免响应竞态
        row = await self.db.get_reclaim(build_id)
        self._ensure_task(build_id, "reclaiming")
        return build, row, True, "回收请求已受理，正在清理工作目录与日志"

    # ------------------------------------------------------------- 启动恢复

    async def resume_pending(self) -> int:
        """进程重启后继续完成未删净的回收；返回接管的待清理数量。"""
        rows = await self.db.list_pending_reclaims()
        for row in rows:
            self._ensure_task(row["build_id"], "reclaiming")
        return len(rows)

    # ------------------------------------------------------------- 清理循环

    def _ensure_task(self, build_id: int, status: str) -> None:
        if status != "reclaiming":
            return
        task = self._tasks.get(build_id)
        if task is not None and not task.done():
            return
        task = asyncio.create_task(
            self._delete_loop(build_id), name=f"reclaim-{build_id}"
        )
        self._tasks[build_id] = task
        task.add_done_callback(lambda t, bid=build_id: self._on_task_done(bid, t))

    def _on_task_done(self, build_id: int, task: asyncio.Task[None]) -> None:
        self._tasks.pop(build_id, None)
        if task.cancelled() or task.exception() is None:
            return
        log.error("回收任务异常: %s", task.exception())

    async def _delete_loop(self, build_id: int) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                await self._delete_once(build_id)
            except Exception as exc:
                # 未删净：保留 reclaiming 状态并留痕，退避后重试
                await self.db.update_reclaim_progress(
                    build_id, f"第 {attempt} 次清理未完成: {exc}"
                )
                log.warning("构建 %s 第 %s 次回收清理未完成: %s", build_id, attempt, exc)
                await asyncio.sleep(config.RECLAIM_RETRY_SECONDS)
                continue
            await self.db.finish_reclaim(build_id, _now())
            log.info("构建 %s 回收完成", build_id)
            return

    async def _delete_once(self, build_id: int) -> None:
        # 防御性复核：只清理终态构建，绝不碰仍在执行的工作目录
        build = await self.db.get_build(build_id)
        if build is None:
            raise RuntimeError("构建记录缺失，拒绝清理")
        if build["status"] not in BUILD_TERMINAL:
            raise RuntimeError(f"构建状态为 {build['status']}，拒绝清理")
        steps = await self.db.get_steps(build_id)
        if any(s["status"] not in STEP_TERMINAL for s in steps):
            raise RuntimeError("存在未终态步骤，拒绝清理")

        await asyncio.to_thread(self._remove_dirs, build_id)

        # 未删净不得报完成：两个目录都必须确认不存在
        leftovers = [str(p) for p in _build_paths(build_id) if p.exists()]
        if leftovers:
            raise RuntimeError(f"删除后路径仍存在: {leftovers}")

    @staticmethod
    def _remove_dirs(build_id: int) -> None:
        for path in _build_paths(build_id):
            _remove_path(path)

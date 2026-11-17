"""回收路径清理助手的纯单元测试（不依赖服务器）。"""
from __future__ import annotations

from pathlib import Path

from app.reclaim import _build_paths, _remove_path


def test_remove_path_deletes_directory_tree(tmp_path: Path):
    target = tmp_path / "7"
    (target / "out").mkdir(parents=True)
    (target / "out" / "fw.bin").write_text("data")
    _remove_path(target)
    assert not target.exists()


def test_remove_path_deletes_plain_file(tmp_path: Path):
    target = tmp_path / "9"
    target.write_text("stray")
    _remove_path(target)
    assert not target.exists()


def test_remove_path_tolerates_missing(tmp_path: Path):
    _remove_path(tmp_path / "never-existed")  # 不抛异常即通过


def test_remove_path_unlinks_symlink_without_touching_target(tmp_path: Path):
    real = tmp_path / "real"
    real.mkdir()
    (real / "keep.txt").write_text("keep")
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    _remove_path(link)
    assert not link.exists() and not link.is_symlink()
    # 符号链接本身被删除，指向的真实目录与内容必须保留
    assert (real / "keep.txt").read_text() == "keep"


def test_build_paths_are_scoped_per_build():
    workdir, logdir = _build_paths(42)
    assert workdir.name == "42" and logdir.name == "42"
    assert workdir.parent.name == "workdir" and logdir.parent.name == "logs"
    # 不同构建号路径互不相同（回收不影响其他构建的前提）
    assert _build_paths(1) != _build_paths(2)

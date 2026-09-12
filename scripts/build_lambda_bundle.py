"""Build the Lambda deployment bundle into `build/lambda`.

No Docker. `uv pip install --python-platform x86_64-manylinux2014` resolves Linux wheels
from any host, which is all we need because the whole dependency tree here is pure
Python. The library and the example app are copied in from source, so
the bundle is exactly the code in this repository and not whatever was last published.

`boto3` and `botocore` are deliberately left out: the Lambda Python 3.12 runtime ships a
recent one, and including them adds about 15 MB to a zip that has a 50 MB limit.

    uv run python scripts/build_lambda_bundle.py
"""

from __future__ import annotations

import argparse
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TARGET = Path(os.environ.get("AGENT_WAIT_BUILD_DIR") or (ROOT / "build" / "lambda"))

# Pinned so a deployed bundle matches what the test suite ran against.
RUNTIME_DEPENDENCIES = [
    "langgraph==1.2.11",
    "langgraph-checkpoint>=4.2,<5",
]

SOURCE_TREES = [
    ROOT / "src" / "agent_wait",
    ROOT / "examples" / "refund_agent",
]

PRUNE = ["__pycache__", "*.dist-info", "*.pyc", "tests", "cdk"]


def _force_remove(path: Path, attempts: int = 6) -> None:
    """Delete a tree that a file watcher may be holding open.

    This repository often lives in a OneDrive-synced folder, and OneDrive takes brief
    locks on directories it is scanning -- so the first rmdir gets `Access is denied` and
    the second, a second later, succeeds.
    """

    def clear_readonly(func: Any, target: str, _exc: Any) -> None:
        os.chmod(target, stat.S_IWRITE)
        func(target)

    for attempt in range(attempts):
        try:
            shutil.rmtree(path, onerror=clear_readonly)
            return
        except (PermissionError, OSError):
            if attempt == attempts - 1:
                raise
            print(f"  {path.name} is locked; retrying ({attempt + 1}/{attempts})")
            time.sleep(1.5)


def build(target: Path) -> Path:
    if target.exists():
        _force_remove(target)
    target.mkdir(parents=True)

    print(f"resolving Linux wheels into {target}")
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--target",
            str(target),
            "--python-platform",
            "x86_64-manylinux2014",
            "--python-version",
            "3.12",
            "--no-installer-metadata",
            # OneDrive-backed directories reject hardlinks (os error 396), and this
            # repository lives in one. Copying is slower and always works.
            "--link-mode=copy",
            *RUNTIME_DEPENDENCIES,
        ],
        check=True,
        cwd=ROOT,
    )

    for tree in SOURCE_TREES:
        destination = target / tree.name
        print(f"copying {tree.relative_to(ROOT)} -> {destination.name}")
        shutil.copytree(
            tree,
            destination,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "tests", "cdk", "cdk.out"),
        )

    # boto3 is provided by the runtime; shipping it again wastes a third of the limit.
    for name in ("boto3", "botocore", "s3transfer", "dateutil", "urllib3", "jmespath"):
        for path in target.glob(f"{name}*"):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

    for pattern in PRUNE:
        for path in target.rglob(pattern):
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.exists():
                path.unlink()

    size = sum(f.stat().st_size for f in target.rglob("*") if f.is_file())
    print(f"bundle ready: {target}  ({size / 1_048_576:.1f} MiB unpacked)")
    if size > 240 * 1_048_576:
        print("WARNING: the unpacked bundle exceeds the 250 MiB Lambda limit", file=sys.stderr)
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path, default=DEFAULT_TARGET)
    arguments = parser.parse_args()
    build(arguments.target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

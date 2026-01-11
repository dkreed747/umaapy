"""Generate UMAA Python types via Cyclone DDS idlc."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate UMAA types using Cyclone DDS idlc.")
    parser.add_argument(
        "--idl-root",
        type=Path,
        default=None,
        help="Path to the root idls/ directory (default: repo_root/idls).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Path to generate types under (default: repo_root/src).",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not remove existing generated UMAA package before generation.",
    )
    return parser.parse_args()


def _run_idlc(idl_root: Path, output_root: Path) -> None:
    idl_files = sorted(idl_root.rglob("*.idl"))
    if not idl_files:
        raise RuntimeError(f"No IDL files found under {idl_root}")

    for idl_file in idl_files:
        subprocess.run(
            ["idlc", "-l", "py", "-I", str(idl_root), str(idl_file)],
            cwd=output_root,
            check=True,
        )


def main() -> int:
    args = _parse_args()
    repo_root = _repo_root()
    idl_root = (args.idl_root or (repo_root / "idls")).resolve()
    output_root = (args.output_root or (repo_root / "src")).resolve()
    output_pkg = output_root / "UMAA"

    if not idl_root.exists():
        raise RuntimeError(f"IDL root does not exist: {idl_root}")
    output_root.mkdir(parents=True, exist_ok=True)

    if output_pkg.exists() and not args.no_clean:
        shutil.rmtree(output_pkg)

    _run_idlc(idl_root, output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

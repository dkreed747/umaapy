#!/usr/bin/env python3
"""Generate Cyclone DDS Python types from UMAA IDL definitions."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from shutil import which


REPO_ROOT = Path(__file__).resolve().parents[1]
IDL_ROOT = REPO_ROOT / "specs" / "idls"
PACKAGE_ROOT = REPO_ROOT / "src" / "umaapy"
GENERATED_ROOT = PACKAGE_ROOT / "UMAA"
MIN_GENERATED_BYTES = 32_768


@dataclass(slots=True)
class GenerationFailure:
    """Container for per-file idlc failures."""

    idl_path: Path
    returncode: int
    stderr: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove src/umaapy/UMAA before generation.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed progress and subprocess output.",
    )
    return parser.parse_args()


def discover_idls() -> list[Path]:
    idl_files = sorted(IDL_ROOT.rglob("*.idl"), key=lambda path: path.as_posix())
    if not idl_files:
        raise RuntimeError(f"No IDL files found under {IDL_ROOT}.")
    return idl_files


def resolve_idlc() -> Path:
    configured = os.environ.get("IDLC_PATH")
    if configured:
        candidate = Path(configured).expanduser()
        if candidate.is_file():
            return candidate.resolve()
        discovered = which(configured)
        if discovered:
            return Path(discovered).resolve()
        raise FileNotFoundError(f"IDLC_PATH is set but does not point to a file: {configured}")

    discovered = which("idlc")
    if discovered:
        return Path(discovered).resolve()

    try:
        import cyclonedds  # type: ignore[import-not-found]
    except Exception:
        cyclonedds = None

    if cyclonedds is not None:
        package_root = Path(cyclonedds.__file__).resolve().parent
        candidates = [package_root / ".libs" / ("idlc.exe" if os.name == "nt" else "idlc")]
        for candidate in candidates:
            if candidate.is_file():
                return candidate

    raise FileNotFoundError(
        "Could not locate 'idlc'. Set IDLC_PATH, add idlc to PATH, or install cyclonedds/cyclonedds-nightly."
    )


def build_idlc_environment(idlc_path: Path) -> dict[str, str] | None:
    """Inject loader paths when using idlc bundled inside Cyclone Python wheels."""
    if ".libs" not in idlc_path.parts:
        return None

    try:
        from cyclonedds.__library__ import library_path  # type: ignore[import-not-found]
    except Exception:
        return None

    env = os.environ.copy()
    library_dir = str(Path(library_path).resolve().parent)
    if os.name == "nt":
        current = env.get("PATH", "")
        env["PATH"] = ";".join([library_dir, current]) if current else library_dir
    elif sys.platform == "darwin":
        current = env.get("DYLD_LIBRARY_PATH", "")
        env["DYLD_LIBRARY_PATH"] = ":".join([library_dir, current]) if current else library_dir
    else:
        current = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = ":".join([library_dir, current]) if current else library_dir
    return env


def clean_generated_tree(verbose: bool) -> None:
    if not GENERATED_ROOT.exists():
        if verbose:
            print(f"[clean] Nothing to remove at {GENERATED_ROOT}")
        return

    if verbose:
        print(f"[clean] Removing {GENERATED_ROOT}")
    shutil.rmtree(GENERATED_ROOT)


def run_idlc(
    idlc_path: Path, idl_file: Path, verbose: bool, env: dict[str, str] | None
) -> subprocess.CompletedProcess[str]:
    command = [
        str(idlc_path),
        "-l",
        "py",
        "-I",
        str(IDL_ROOT),
        str(idl_file),
    ]
    if verbose:
        print(f"[idlc] {' '.join(command)}")
    return subprocess.run(
        command,
        cwd=PACKAGE_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def validate_generated_tree(expected_idl_count: int) -> tuple[int, int]:
    if not GENERATED_ROOT.exists():
        raise RuntimeError(f"Generation did not create {GENERATED_ROOT}.")

    python_files = list(GENERATED_ROOT.rglob("*.py"))
    python_file_count = len(python_files)
    total_bytes = sum(path.stat().st_size for path in python_files)

    minimum_file_count = max(8, expected_idl_count // 2)
    if python_file_count < minimum_file_count:
        raise RuntimeError(
            "Generated output appears incomplete: "
            f"found {python_file_count} Python files, expected at least {minimum_file_count}."
        )
    if total_bytes < MIN_GENERATED_BYTES:
        raise RuntimeError(
            "Generated output appears too small: "
            f"found {total_bytes} bytes under {GENERATED_ROOT}, expected at least {MIN_GENERATED_BYTES}."
        )

    return python_file_count, total_bytes


def patch_generated_root_alias(verbose: bool) -> None:
    """Alias top-level 'UMAA' to 'umaapy.UMAA' for generated absolute imports."""
    init_file = GENERATED_ROOT / "__init__.py"
    if not init_file.exists():
        raise RuntimeError(f"Generated root package is missing expected file: {init_file}")

    source = init_file.read_text(encoding="utf-8")
    marker = '_sys.modules.setdefault("UMAA", _sys.modules[__name__])'
    if marker in source:
        return

    lines = source.splitlines()
    insert_at = next((index for index, line in enumerate(lines) if line.startswith("from . import ")), len(lines))
    alias_lines = [
        "import sys as _sys",
        '_sys.modules.setdefault("UMAA", _sys.modules[__name__])',
        "",
    ]
    lines = lines[:insert_at] + alias_lines + lines[insert_at:]
    init_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if verbose:
        print(f"[patch] Added UMAA module alias to {init_file}")


def main() -> int:
    args = parse_args()

    try:
        idlc_path = resolve_idlc()
    except FileNotFoundError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    idlc_env = build_idlc_environment(idlc_path)

    try:
        idl_files = discover_idls()
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    if args.verbose:
        print(f"[config] Repository root: {REPO_ROOT}")
        print(f"[config] IDL root: {IDL_ROOT}")
        print(f"[config] Output root: {PACKAGE_ROOT}")
        print(f"[config] idlc: {idlc_path}")
        print(f"[config] IDL files discovered: {len(idl_files)}")

    if args.clean:
        clean_generated_tree(verbose=args.verbose)

    failures: list[GenerationFailure] = []
    for index, idl_file in enumerate(idl_files, start=1):
        if args.verbose:
            rel = idl_file.relative_to(REPO_ROOT)
            print(f"[{index}/{len(idl_files)}] Generating from {rel}")

        result = run_idlc(idlc_path=idlc_path, idl_file=idl_file, verbose=args.verbose, env=idlc_env)
        if result.returncode != 0:
            failures.append(
                GenerationFailure(
                    idl_path=idl_file,
                    returncode=result.returncode,
                    stderr=result.stderr.strip(),
                )
            )

    if failures:
        print(
            f"ERROR: idlc failed for {len(failures)} of {len(idl_files)} IDL files. " "Generation is incomplete.",
            file=sys.stderr,
        )
        for failure in failures:
            relative_idl = failure.idl_path.relative_to(REPO_ROOT)
            print(f"- {relative_idl} (exit {failure.returncode})", file=sys.stderr)
            if failure.stderr:
                first_line = failure.stderr.splitlines()[0]
                print(f"  stderr: {first_line}", file=sys.stderr)
        return 1

    try:
        patch_generated_root_alias(verbose=args.verbose)
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    try:
        python_file_count, total_bytes = validate_generated_tree(expected_idl_count=len(idl_files))
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(
        "Generated Cyclone Python types successfully: "
        f"{len(idl_files)} IDL files processed, {python_file_count} Python files written, "
        f"{total_bytes} bytes in src/umaapy/UMAA/."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

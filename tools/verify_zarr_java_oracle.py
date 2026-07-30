#!/usr/bin/env python3
"""Run the opt-in zarr-java 0.1.3 interoperability oracle."""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import tempfile
import urllib.request
from pathlib import Path


ZARR_JAVA_VERSION = "0.1.3"
NETCDF_ALL_VERSION = "5.9.1"
NETCDF_ALL_SHA256 = "3ba80b2b2125028ebcb4d98034b165dfca5a5dddaeaad5fa41ab211aa378fc72"
NETCDF_ALL_URL = (
    "https://github.com/Unidata/netcdf-java/releases/download/"
    f"v{NETCDF_ALL_VERSION}/netcdfAll-{NETCDF_ALL_VERSION}.jar"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def netcdf_all(cache_root: Path) -> Path:
    cache_root.mkdir(parents=True, exist_ok=True)
    target = cache_root / f"netcdfAll-{NETCDF_ALL_VERSION}.jar"
    if target.is_file():
        actual = sha256(target)
        if actual != NETCDF_ALL_SHA256:
            raise RuntimeError(
                f"cached {target} has SHA-256 {actual}; expected {NETCDF_ALL_SHA256}"
            )
        return target

    request = urllib.request.Request(
        NETCDF_ALL_URL,
        headers={"User-Agent": "zarr4s-core-java-oracle/1"},
    )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix="netcdfAll-",
        suffix=".part",
        dir=cache_root,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as destination:
            with urllib.request.urlopen(request, timeout=60) as response:
                shutil.copyfileobj(response, destination)
        actual = sha256(temporary)
        if actual != NETCDF_ALL_SHA256:
            raise RuntimeError(
                f"downloaded netcdfAll has SHA-256 {actual}; expected {NETCDF_ALL_SHA256}"
            )
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=("verify-python-shard", "write-fixture"),
    )
    parser.add_argument("target", type=Path)
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path(tempfile.gettempdir()) / "zarr4s-java-oracle",
    )
    parser.add_argument("--java-home", type=Path)
    parser.add_argument("--gradle", default="gradle")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    gradle = shutil.which(arguments.gradle)
    if gradle is None:
        raise SystemExit(f"Gradle executable not found: {arguments.gradle}")
    if arguments.java_home is not None:
        java = arguments.java_home / "bin" / "java"
        if not java.is_file():
            raise SystemExit(f"Java executable not found: {java}")

    if arguments.mode.startswith("verify-") and not arguments.target.exists():
        raise SystemExit(f"oracle target does not exist: {arguments.target}")
    if arguments.mode == "write-fixture" and arguments.target.exists():
        raise SystemExit(f"create-only fixture target already exists: {arguments.target}")

    cache_root = arguments.cache_root.resolve()
    netcdf_jar = netcdf_all(cache_root)
    project = Path(__file__).resolve().with_name("zarr-java-oracle")
    environment = os.environ.copy()
    environment["GRADLE_USER_HOME"] = str(cache_root / "gradle-home")
    if arguments.java_home is not None:
        environment["JAVA_HOME"] = str(arguments.java_home.resolve())

    command = [
        gradle,
        "--no-daemon",
        "-Dorg.gradle.native=false",
        f"-PnetcdfAllJar={netcdf_jar}",
        f"-PoracleMode={arguments.mode}",
        f"-PoracleTarget={arguments.target.resolve()}",
        "run",
    ]
    print(
        f"zarr-java={ZARR_JAVA_VERSION} "
        f"netcdfAll={NETCDF_ALL_VERSION} sha256={NETCDF_ALL_SHA256}",
        flush=True,
    )
    completed = subprocess.run(command, cwd=project, env=environment, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Bidirectional Zarr-Python 3.2.1 Blosc/Zstd gate for Scalafim."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import zarr
from zarr.codecs import (
    BloscCodec,
    BloscShuffle,
    BytesCodec,
    Crc32cCodec,
    ShardingCodec,
)


DIRECT = np.array([[1.25, -2.5, 300.0], [4.5, 5.75, -6.0]], dtype=np.float32)
INT16 = np.array([[1, -2, 300], [4, 5, -6]], dtype=np.int16)
SHARDED = np.arange(1, 17, dtype=np.float32).reshape(4, 4)


def blosc() -> BloscCodec:
    return BloscCodec(
        cname="zstd",
        clevel=5,
        shuffle=BloscShuffle.shuffle,
        typesize=4,
        blocksize=0,
    )


def write_python(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    direct = zarr.create_array(
        root / "python-blosc-direct.zarr",
        shape=DIRECT.shape,
        dtype="float32",
        chunks=DIRECT.shape,
        serializer=BytesCodec(endian="little"),
        compressors=(blosc(),),
        fill_value=0,
        dimension_names=("y", "x"),
        zarr_format=3,
    )
    direct[:] = DIRECT

    int16 = zarr.create_array(
        root / "python-blosc-int16.zarr",
        shape=INT16.shape,
        dtype="int16",
        chunks=INT16.shape,
        serializer=BytesCodec(endian="little"),
        compressors=(
            BloscCodec(
                cname="zstd",
                clevel=5,
                shuffle=BloscShuffle.shuffle,
                typesize=2,
                blocksize=0,
            ),
        ),
        fill_value=0,
        dimension_names=("y", "x"),
        zarr_format=3,
    )
    int16[:] = INT16

    sharded = zarr.create_array(
        root / "python-blosc-sharded.zarr",
        shape=SHARDED.shape,
        dtype="float32",
        chunks=SHARDED.shape,
        serializer=ShardingCodec(
            chunk_shape=(2, 2),
            codecs=(BytesCodec(endian="little"), blosc()),
            index_codecs=(BytesCodec(endian="little"), Crc32cCodec()),
            index_location="start",
        ),
        compressors=(),
        fill_value=0,
        dimension_names=("y", "x"),
        zarr_format=3,
    )
    sharded[:] = SHARDED


def verify_scala(root: Path) -> None:
    direct = zarr.open_array(root / "scala-blosc-direct.zarr", mode="r")
    np.testing.assert_array_equal(np.asarray(direct[:]), DIRECT)

    sharded = zarr.open_array(root / "scala-blosc-sharded.zarr", mode="r")
    np.testing.assert_array_equal(np.asarray(sharded[:]), SHARDED)


def print_fixtures(root: Path) -> None:
    for name in (
        "python-blosc-direct.zarr",
        "python-blosc-int16.zarr",
        "python-blosc-sharded.zarr",
    ):
        path = root / name
        print(name)
        print((path / "zarr.json").read_text())
        for payload in sorted(path.rglob("*")):
            if payload.is_file() and payload.name != "zarr.json":
                print(payload.relative_to(path), payload.read_bytes().hex())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("write-python", "verify-scala", "print-fixtures"))
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    if zarr.__version__ != "3.2.1":
        parser.error(f"expected zarr 3.2.1, found {zarr.__version__}")
    if arguments.mode == "write-python":
        write_python(arguments.root)
    elif arguments.mode == "verify-scala":
        verify_scala(arguments.root)
    else:
        print_fixtures(arguments.root)


if __name__ == "__main__":
    main()

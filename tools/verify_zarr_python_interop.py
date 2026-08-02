#!/usr/bin/env python3
"""Bidirectional Zarr-Python 3.2.1 interoperability gate for zarr4s-core."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
import zarr
from numcodecs import GZip
from zarr.codecs import BytesCodec, Crc32cCodec, GzipCodec, ShardingCodec, TransposeCodec
from zarr.core.chunk_key_encodings import V2ChunkKeyEncoding


def verify_scala(root: Path) -> None:
    rank_five_path = root / "rank5.zarr"
    sharded_path = root / "sharded.zarr"

    rank_five = zarr.open_array(rank_five_path, mode="r")
    np.testing.assert_array_equal(
        np.asarray(rank_five[:]),
        np.arange(48, dtype=np.int16).reshape(2, 2, 2, 2, 3),
    )

    expected_sharded = np.zeros((4, 4), dtype=np.int16)
    expected_sharded[:2, :2] = np.array([[1, 2], [3, 4]], dtype=np.int16)
    expected_sharded[2:, 2:] = np.array([[13, 14], [15, 16]], dtype=np.int16)
    sharded = zarr.open_array(sharded_path, mode="r")
    np.testing.assert_array_equal(np.asarray(sharded[:]), expected_sharded)

    unsigned = zarr.open_array(root / "uint64.zarr", mode="r")
    np.testing.assert_array_equal(
        np.asarray(unsigned[:]),
        np.array(
            [0, 1, 2**63 - 1, 2**63, 2**64 - 2, 2**64 - 1],
            dtype=np.uint64,
        ).reshape(2, 3),
    )

    transposed = zarr.open_array(root / "transpose-v2.zarr", mode="r")
    np.testing.assert_array_equal(
        np.asarray(transposed[:]),
        np.arange(6, dtype=np.int32).reshape(2, 3),
    )
    assert (root / "transpose-v2.zarr" / "0.0").is_file()

    v2_gzip = zarr.open_array(root / "v2-gzip.zarr", mode="r")
    np.testing.assert_array_equal(
        np.asarray(v2_gzip[:]),
        np.array([[1, -2, 300], [4, 5, -6]], dtype=np.int16),
    )
    assert v2_gzip.attrs["_ARRAY_DIMENSIONS"] == ["y", "x"]
    assert (root / "v2-gzip.zarr" / ".zarray").is_file()
    assert (root / "v2-gzip.zarr" / ".zattrs").is_file()
    assert (root / "v2-gzip.zarr" / "0" / "0").is_file()

    scalar_values = {
        "bool": [False, True, False, True, True, False],
        "int8": [-128, -1, 0, 1, 42, 127],
        "uint8": [0, 1, 127, 128, 254, 255],
        "int16": [-32768, -1, 0, 1, 42, 32767],
        "uint16": [0, 1, 32767, 32768, 65534, 65535],
        "int32": [-(2**31), -1, 0, 1, 42, 2**31 - 1],
        "uint32": [0, 1, 2**31 - 1, 2**31, 2**32 - 2, 2**32 - 1],
        "int64": [-(2**63), -1, 0, 1, 42, 2**63 - 1],
        "uint64": [0, 1, 2**63 - 1, 2**63, 2**64 - 2, 2**64 - 1],
        "float32": [0.0, -0.0, 1.5, -2.25, np.inf, -np.inf],
        "float64": [0.0, -0.0, 1.5, -2.25, np.inf, -np.inf],
    }
    for dtype, values in scalar_values.items():
        written = zarr.open_array(root / f"scala-{dtype}.zarr", mode="r")
        expected = np.asarray(values, dtype=np.dtype(dtype)).reshape(2, 3)
        np.testing.assert_array_equal(np.asarray(written[:]), expected)

    with tempfile.TemporaryDirectory(prefix="zarr4s-core-corrupt-") as directory:
        corrupt = Path(directory) / "rank5.zarr"
        shutil.copytree(rank_five_path, corrupt)
        payload = next(path for path in sorted(corrupt.rglob("*")) if path.is_file() and path.name != "zarr.json")
        encoded = bytearray(payload.read_bytes())
        encoded[-1] ^= 0x01
        payload.write_bytes(encoded)
        try:
            np.asarray(zarr.open_array(corrupt, mode="r")[:])
        except Exception:
            pass
        else:
            raise AssertionError("Zarr-Python accepted a corrupted CRC32C chunk")

    with tempfile.TemporaryDirectory(prefix="zarr4s-core-missing-codec-") as directory:
        missing = Path(directory) / "array.zarr"
        shutil.copytree(rank_five_path, missing)
        metadata_path = missing / "zarr.json"
        metadata = json.loads(metadata_path.read_text())
        metadata["codecs"][1]["name"] = "io.github.canardlapin.zarr4s.missing"
        metadata_path.write_text(json.dumps(metadata, separators=(",", ":")))
        try:
            zarr.open_array(missing, mode="r")
        except Exception:
            pass
        else:
            raise AssertionError("Zarr-Python accepted metadata for an unavailable codec")


def write_python(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    direct_path = root / "python-direct.zarr"
    sharded_path = root / "python-sharded.zarr"

    direct = zarr.create_array(
        direct_path,
        shape=(2, 3),
        dtype="int16",
        chunks=(2, 3),
        serializer=BytesCodec(endian="little"),
        compressors=(GzipCodec(level=1), Crc32cCodec()),
        fill_value=0,
        dimension_names=("y", "x"),
        zarr_format=3,
    )
    direct[:] = np.array([[1, -2, 300], [4, 5, -6]], dtype=np.int16)

    sharded = zarr.create_array(
        sharded_path,
        shape=(4, 4),
        dtype="int16",
        chunks=(4, 4),
        serializer=ShardingCodec(
            chunk_shape=(2, 2),
            codecs=(BytesCodec(endian="little"),),
            index_codecs=(BytesCodec(endian="little"), Crc32cCodec()),
            index_location="start",
        ),
        compressors=(),
        fill_value=0,
        dimension_names=("y", "x"),
        zarr_format=3,
    )
    sharded[:] = np.arange(1, 17, dtype=np.int16).reshape(4, 4)

    scalar_values = {
        "bool": [False, True, False, True, True, False],
        "int8": [-128, -1, 0, 1, 42, 127],
        "uint8": [0, 1, 127, 128, 254, 255],
        "int16": [-32768, -1, 0, 1, 42, 32767],
        "uint16": [0, 1, 32767, 32768, 65534, 65535],
        "int32": [-(2**31), -1, 0, 1, 42, 2**31 - 1],
        "uint32": [0, 1, 2**31 - 1, 2**31, 2**32 - 2, 2**32 - 1],
        "int64": [-(2**63), -1, 0, 1, 42, 2**63 - 1],
        "uint64": [0, 1, 2**63 - 1, 2**63, 2**64 - 2, 2**64 - 1],
        "float32": [0.0, -0.0, 1.5, -2.25, np.inf, -np.inf],
        "float64": [0.0, -0.0, 1.5, -2.25, np.inf, -np.inf],
    }
    for dtype, values in scalar_values.items():
        numpy_dtype = np.dtype(dtype)
        serializer = BytesCodec() if numpy_dtype.itemsize == 1 else BytesCodec(endian="big")
        array = zarr.create_array(
            root / f"python-{dtype}.zarr",
            shape=(2, 3),
            dtype=dtype,
            chunks=(2, 3),
            serializer=serializer,
            compressors=(),
            fill_value=False if dtype == "bool" else 0,
            zarr_format=3,
        )
        array[:] = np.asarray(values, dtype=numpy_dtype).reshape(2, 3)

    transpose = zarr.create_array(
        root / "python-transpose-v2.zarr",
        shape=(2, 3),
        dtype="int32",
        chunks=(2, 3),
        filters=(TransposeCodec(order=(1, 0)),),
        serializer=BytesCodec(endian="big"),
        compressors=(),
        fill_value=0,
        chunk_key_encoding=V2ChunkKeyEncoding(separator="."),
        zarr_format=3,
    )
    transpose[:] = np.arange(6, dtype=np.int32).reshape(2, 3)

    v2_c = zarr.create_array(
        root / "python-v2-c.zarr",
        shape=(2, 3),
        dtype="int16",
        chunks=(2, 3),
        filters=(),
        compressors=(),
        fill_value=-1,
        order="C",
        zarr_format=2,
    )
    v2_c[:] = np.arange(6, dtype=np.int16).reshape(2, 3)

    v2_f = zarr.create_array(
        root / "python-v2-f.zarr",
        shape=(2, 3),
        dtype=">i2",
        chunks=(2, 3),
        filters=(),
        compressors=(),
        fill_value=-1,
        order="F",
        zarr_format=2,
    )
    v2_f[:] = np.arange(6, dtype=">i2").reshape(2, 3)

    v2_gzip = zarr.create_array(
        root / "python-v2-gzip.zarr",
        shape=(2, 3),
        dtype="int16",
        chunks=(2, 3),
        filters=(),
        compressors=(GZip(level=1),),
        fill_value=0,
        order="C",
        zarr_format=2,
    )
    v2_gzip[:] = np.array([[1, -2, 300], [4, 5, -6]], dtype=np.int16)

    v2_group = zarr.open_group(root / "python-v2-hierarchy.zarr", mode="w", zarr_format=2)
    v2_bold = v2_group.create_array(
        "bold",
        shape=(2, 3),
        dtype="int16",
        chunks=(2, 3),
        filters=(),
        compressors=(),
        attributes={"_ARRAY_DIMENSIONS": ["y", "x"]},
    )
    v2_bold[:] = np.arange(6, dtype=np.int16).reshape(2, 3)
    v2_derived = v2_group.create_group("derived")
    v2_mask = v2_derived.create_array(
        "mask",
        shape=(2, 3),
        dtype="uint8",
        chunks=(2, 3),
        filters=(),
        compressors=(),
    )
    v2_mask[:] = np.array([[1, 1, 0], [0, 1, 1]], dtype=np.uint8)
    zarr.consolidate_metadata(root / "python-v2-hierarchy.zarr", zarr_format=2)

    v3_group = zarr.open_group(root / "python-v3-hierarchy.zarr", mode="w", zarr_format=3)
    v3_bold = v3_group.create_array(
        "bold",
        shape=(2, 3),
        dtype="int16",
        chunks=(2, 3),
        serializer=BytesCodec(endian="little"),
        compressors=(),
        fill_value=0,
        dimension_names=("y", "x"),
    )
    v3_bold[:] = np.arange(6, dtype=np.int16).reshape(2, 3)
    v3_derived = v3_group.create_group("derived")
    v3_mask = v3_derived.create_array(
        "mask",
        shape=(2, 3),
        dtype="uint8",
        chunks=(2, 3),
        serializer=BytesCodec(),
        compressors=(),
        fill_value=0,
    )
    v3_mask[:] = np.array([[1, 1, 0], [0, 1, 1]], dtype=np.uint8)
    zarr.consolidate_metadata(root / "python-v3-hierarchy.zarr", zarr_format=3)

    factored_values = np.fromfunction(
        lambda y, x, z: 100 * y + 10 * x + z,
        (7, 8, 9),
        dtype=np.int32,
    ).astype(np.int32)
    factored = zarr.create_array(
        root / "python-factored.zarr",
        shape=factored_values.shape,
        dtype="int32",
        chunks=(3, 4, 5),
        serializer=BytesCodec(endian="little"),
        compressors=(),
        fill_value=-1,
        dimension_names=("y", "x", "z"),
        zarr_format=3,
    )
    factored[:] = factored_values
    y_indices = np.array([6, 1, 6, 0])
    z_indices = np.array([8, 2, 8, 0])
    zarr_selected = np.asarray(factored.oindex[y_indices, slice(1, 8, 3), z_indices])
    numpy_selected = factored_values[np.ix_(y_indices, np.arange(1, 8, 3), z_indices)]
    np.testing.assert_array_equal(zarr_selected, numpy_selected)
    (root / "python-factored-expected.json").write_text(
        json.dumps(zarr_selected.reshape(-1).tolist(), separators=(",", ":"))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("verify-scala", "write-python"))
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    if zarr.__version__ != "3.2.1":
        parser.error(f"expected zarr 3.2.1, found {zarr.__version__}")
    if arguments.mode == "verify-scala":
        verify_scala(arguments.root)
    else:
        write_python(arguments.root)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Opt-in zarrs 0.23.13 readback gate for zarr4s and Ravel v2 writer fixtures."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path


CARGO_TOML = """[package]
name = "zarr4s-zarrs-v2-oracle"
version = "0.1.0"
edition = "2024"

[dependencies]
zarrs = "0.23.13"
"""

RUST_MAIN = r'''use std::env;
use std::path::Path;
use std::sync::Arc;

use zarrs::array::{Array, ArraySubset};
use zarrs::filesystem::FilesystemStore;

fn read(path: &Path, expected: &[i16]) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(FilesystemStore::new(Path::new(&path))?);
    let array = Array::open(store, "/")?;
    let values: Vec<i16> = array.retrieve_array_subset::<Vec<i16>>(
        &ArraySubset::new_with_shape(array.shape().to_vec()),
    )?;
    assert_eq!(values, expected);
    println!("zarrs 0.23.13 read {:?}: {:?}", array.shape(), values);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = env::args().nth(1).ok_or("expected a fixture directory")?;
    let root = Path::new(&root);
    read(
        &root.join("v2-gzip.zarr"),
        &[1, -2, 300, 4, 5, -6],
    )?;
    read(&root.join("facade-v2.zarr"), &[7, 8, 9, 10, 11, 12])?;
    read(&root.join("ravel-v2-int16.zarr"), &[1, -2, 300, 4, 5, -6])?;
    Ok(())
}
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    for name in ("v2-gzip.zarr", "facade-v2.zarr", "ravel-v2-int16.zarr"):
        fixture = arguments.root / name
        if not fixture.is_dir():
            parser.error(f"missing writer fixture: {fixture}")

    with tempfile.TemporaryDirectory(prefix="zarr4s-zarrs-oracle-") as directory:
        project = Path(directory)
        (project / "src").mkdir()
        (project / "Cargo.toml").write_text(CARGO_TOML)
        (project / "src" / "main.rs").write_text(RUST_MAIN)
        subprocess.run(
            [
                "cargo",
                "run",
                "--quiet",
                "--manifest-path",
                str(project / "Cargo.toml"),
                "--",
                str(arguments.root),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()

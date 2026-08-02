#!/usr/bin/env python3
"""Opt-in zarrs 0.23.13 readback gate for a zarr4s v2 writer fixture."""

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).ok_or("expected a Zarr array path")?;
    let store = Arc::new(FilesystemStore::new(Path::new(&path))?);
    let array = Array::open(store, "/")?;
    let values: Vec<i16> = array.retrieve_array_subset::<Vec<i16>>(
        &ArraySubset::new_with_shape(array.shape().to_vec()),
    )?;
    assert_eq!(values, vec![1, -2, 300, 4, 5, -6]);
    println!("zarrs 0.23.13 read {:?}: {:?}", array.shape(), values);
    Ok(())
}
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    fixture = arguments.root / "v2-gzip.zarr"
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
                str(fixture),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()

# scalafim-zarr-codec-blosc-zstd

Optional Zarr v3 `blosc` provider restricted to the Zstandard compressor. The
shared layer owns normative metadata validation and bounded Blosc-frame checks;
the JVM and Scala.js layers supply platform algorithms.

The module is deliberately separate from `scalafim-zarr`: applications that do
not need Blosc acquire no JNI or WebAssembly dependency. The JVM provider uses
`blosc-java`. The Scala.js provider uses the embedded-WASM `numcodecs` package
for reads. That package's encoder does not expose Blosc's `typesize` parameter;
its observed contract is `typesize == 4`. The provider validates the produced
frame and rejects every other requested stride before writing.

Install the pinned browser dependency before compiling or testing the optional
Scala.js provider:

```text
npm ci --prefix modules/zarr-codec-blosc-zstd/js
sbt zarrCodecBloscZstdJS/test
```

This explicit opt-in keeps npm and the embedded WebAssembly payload out of the
dependency-free `scalafim-zarr` core and out of builds that do not select this
provider.

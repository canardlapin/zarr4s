# zarr-java interoperability oracle

This is an opt-in, JVM-only differential oracle. It is deliberately outside
the zarr4s module graph and is not a runtime backend for `zarr4s-core`.

The Gradle project pins `dev.zarr:zarr-java` 0.1.3. That artifact's published
POM obtains `edu.ucar:cdm-core` from the Unidata repository, so the oracle
excludes that edge and instead uses Unidata's official `netcdfAll` 5.9.1
GitHub release asset. `tools/verify_zarr_java_oracle.py` downloads the asset
over verified HTTPS and requires its published SHA-256:

```text
3ba80b2b2125028ebcb4d98034b165dfca5a5dddaeaad5fa41ab211aa378fc72
```

The rest of zarr-java's substantial Java dependency graph remains confined to
this tool. This is useful independent evidence for compatibility and concrete
evidence against putting zarr-java in the portable core.

Requirements are a Gradle 8 executable and a Gradle-compatible JDK. If the
shell default JDK is newer than the installed Gradle supports, pass a supported
JDK explicitly (17 through 24 for Gradle 8.14.3):

```sh
python tools/verify_zarr_java_oracle.py \
  verify-python-shard /tmp/python-sharded.zarr \
  --java-home /path/to/jdk-22
```

The writer mode produces a small direct Zarr v3 fixture for the reciprocal
Scala-reader gate:

```sh
python tools/verify_zarr_java_oracle.py \
  write-fixture /tmp/zarr-java-direct.zarr \
  --java-home /path/to/jdk-22
```

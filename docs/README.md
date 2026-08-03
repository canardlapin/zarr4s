# Design and verification records

The plans and benchmark records in this directory document the work that made
the Zarr kernel independently consumable. Some measurements use NeuroArchive
or fMRI-shaped arrays as concrete scientific workloads. Those workloads do not
form part of the zarr4s API.

Current user-facing guarantees are stated in the repository README and tested
by the JVM, Scala.js, and interoperability suites. Historical benchmark
figures describe their recorded environment; they are not general performance
guarantees.

Editorial records:

- [`reviews/guide-editorial-review-2026-08-02.md`](reviews/guide-editorial-review-2026-08-02.md)
  evaluates the public guide against the source, tests, examples, design
  records, and current release boundary. It also records API issues exposed by
  the documentation rewrite.

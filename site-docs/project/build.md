# Build and verify the project

This page is for contributors and documentation authors. Library users do not
need the repository build to understand the API, except while the project still
requires local publication.

## Build the guide

The site is a separate sbt project in `site/`. Public sources live in
`site-docs/`; internal plans, benchmarks, and evidence records under `docs/`
are excluded from rendering.

```text
sbt -batch docsBundle
```

The task runs mdoc against `coreJVM`, renders the processed Markdown with
Laika, generates JVM and Scala.js Scaladoc for both public modules, and copies
those four API trees into the site artifact. Generated output under
`site/target/` is ignored.

For an interactive authoring loop:

```text
sbt docs/tlSitePreview
```

The preview server is not the CI gate. Close it when authoring finishes.

## Run the affected platform gates

Every shared change must pass both platform suites:

```text
sbt coreJVM/test coreJS/test
```

For the optional codec provider, install its pinned npm dependencies first:

```text
npm ci --prefix codec-blosc-zstd/js
```

Then run the repository gate:

```text
sbt -J-Xmx4G checkAll
```

`checkAll` checks formatting, compiles every module, runs JVM and Scala.js
tests for core and the optional provider, and builds the complete documentation
bundle.

Generate one API tree during focused source work:

```text
sbt coreJVM/doc coreJS/doc
```

A green documentation bundle proves that JVM mdoc examples compile and
evaluate, Laika renders the guide, and all four Scaladoc trees compile. It does
not by itself prove Scala.js runtime behavior, independent Zarr
interoperability, a published Maven artifact, or a successful Pages deployment.

## Publication states

The GitHub Pages workflow builds `docsBundle` and uploads
`site/target/docs/site`. It is the only site publisher; no `gh-pages` branch is
configured in the sbt build.

Keep these states separate:

- **generated:** the local or CI site task completed;
- **configured:** the workflow and Pages settings exist;
- **published:** the deployment job passed and the served URL contains the
  expected revision.

The public guide is <https://canardlapin.github.io/zarr4s/>. Internal records
under `docs/` must not appear in its paths or search index.

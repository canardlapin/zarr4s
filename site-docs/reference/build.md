# Build and verify the guide

The site is a separate sbt project in `site/`. Its source directory is
`site-docs/`, not the repository's existing `docs/` tree. The latter contains
design, benchmark, and development-evidence records and is intentionally not
rendered as public user documentation.

## Generate the site

From the repository root:

```text
sbt -batch docs/tlSite
```

`docs/tlSite` runs mdoc against the JVM `coreJVM` dependency and then renders
the processed Markdown with Laika. The generated files under `site/target/`
are disposable build output and are ignored by Git.

For an interactive authoring loop, use:

```text
sbt docs/tlSitePreview
```

The preview server is useful while writing but is not the non-interactive
verification gate.

## Run the repository gates

The focused documentation gate is:

```text
sbt -batch docsCheck
```

The repository's `checkAll` alias also includes `docs/tlSite`, so the CI job
checks the executable guide together with formatting, compilation, and tests:

```text
sbt -J-Xmx4G checkAll
```

The site examples exercise `coreJVM`. They do not replace the cross-platform
library checks:

```text
sbt coreJVM/test coreJS/test
```

The shared API and the standalone consumer provide the evidence for the
Scala.js lane. A green JVM mdoc build is deliberately reported as JVM guide
coverage, not as Scala.js parity.

## Publish the guide

The repository's `.github/workflows/docs.yml` workflow builds `docs/tlSite` on
pushes to `main` and on manual dispatch, then deploys the generated
`site/target/docs/site` directory through the `github-pages` environment. It is
the only site publisher; the repository does not use a `gh-pages` branch.

The deployed guide is available at
<https://canardlapin.github.io/zarr4s/> after the workflow completes.

## Documentation boundaries

- Keep public onboarding, concepts, and task guides under `site-docs/`.
- Keep symbol contracts in Scaladoc and link to published API docs when an API
  site exists.
- Keep design decisions, benchmark receipts, tracker handoffs, and release
  evidence under `docs/` unless they are rewritten as user-facing advanced
  material.
- Do not add a release coordinate to a guide until that artifact is actually
  published. This checkout currently uses source/local-consumer paths.

The site workflow publishes only `site-docs/`. The repository's `docs/` tree
contains design, benchmark, and development-evidence records and is not
rendered as public user documentation.

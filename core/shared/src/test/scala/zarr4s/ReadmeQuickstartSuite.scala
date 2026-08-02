package zarr4s

import munit.FunSuite

/** Keeps the README's front-door example executable on both supported platforms. */
class ReadmeQuickstartSuite extends FunSuite:
  test("README typed quickstart compiles and executes"):
    quickstart()

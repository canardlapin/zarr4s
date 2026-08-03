import org.scalajs.linker.interface.ModuleKind
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport.*
import org.typelevel.sbt.gha.JavaSpec
import sbtcrossproject.CrossPlugin.autoImport.*
import scalajscrossproject.ScalaJSCrossPlugin.autoImport.*

val Scala3 = "3.7.4"
val munitV = "1.2.1"

lazy val docsBundle = taskKey[File]("Build the guide and bundled per-platform API reference")

ThisBuild / tlBaseVersion := "0.1"
ThisBuild / organization := "io.github.canardlapin"
ThisBuild / organizationName := "Bradley Buchsbaum"
ThisBuild / startYear := Some(2026)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  tlGitHubDev("canardlapin", "Bradley Buchsbaum")
)

ThisBuild / scalaVersion := Scala3
ThisBuild / crossScalaVersions := Seq(Scala3)
ThisBuild / tlJdkRelease := Some(11)
ThisBuild / githubWorkflowJavaVersions := Seq(
  JavaSpec.temurin("17"),
  JavaSpec.temurin("21")
)

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-Xmax-inlines:64",
    "-Wconf:msg=package scala contains object and package with same name.*caps:silent"
  ),
  Test / fork := false,
  Test / parallelExecution := false,
  libraryDependencies += "org.scalameta" %%% "munit" % munitV % Test
)

lazy val jsSettingsBase = Seq(
  scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule)),
  Test / jsEnv := new org.scalajs.jsenv.nodejs.NodeJSEnv()
)

lazy val root = tlCrossRootProject
  .aggregate(core, codecBloscZstd, benchmarks)

lazy val core =
  crossProject(JSPlatform, JVMPlatform)
    .crossType(CrossType.Full)
    .in(file("core"))
    .settings(commonSettings)
    .settings(
      name := "zarr4s-core"
    )
    .jsSettings(jsSettingsBase)

lazy val codecBloscZstd =
  crossProject(JSPlatform, JVMPlatform)
    .crossType(CrossType.Full)
    .in(file("codec-blosc-zstd"))
    .dependsOn(core)
    .settings(commonSettings)
    .settings(
      name := "zarr4s-codec-blosc-zstd"
    )
    .jvmSettings(
      libraryDependencies ++= Seq(
        "com.scalableminds" % "blosc-java" % "0.3-1.21.6",
        "com.github.luben" % "zstd-jni" % "1.5.7-11"
      )
    )
    .jsSettings(jsSettingsBase)
    .jsSettings(
      scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.ESModule))
    )

lazy val coreJS = core.js
lazy val coreJVM = core.jvm
lazy val codecBloscZstdJS = codecBloscZstd.js
lazy val codecBloscZstdJVM = codecBloscZstd.jvm

lazy val docs =
  project
    .in(file("site"))
    .dependsOn(coreJVM)
    .enablePlugins(org.typelevel.sbt.TypelevelSitePlugin)
    .settings(
      name := "zarr4s-site",
      publish / skip := true,
      mdocIn := (ThisBuild / baseDirectory).value / "site-docs",
      mdocExtraArguments += "--clean-target",
      tlSiteApiUrl := Some(
        url("https://canardlapin.github.io/zarr4s/reference/api-map.html")
      ),
      tlSitePublishBranch := None,
      tlSitePublishTags := false
    )

lazy val benchmarks =
  project
    .in(file("benchmarks"))
    .dependsOn(coreJVM)
    .settings(commonSettings)
    .settings(
      name := "zarr4s-benchmarks",
      publish / skip := true,
      Test / unmanagedResourceDirectories +=
        (ThisBuild / baseDirectory).value / "site-docs"
    )

docsBundle := {
  (docs / tlSite).value
  val apiDocs = Vector(
    "core/jvm" -> (coreJVM / Compile / doc).value,
    "core/js" -> (coreJS / Compile / doc).value,
    "codec-blosc-zstd/jvm" -> (codecBloscZstdJVM / Compile / doc).value,
    "codec-blosc-zstd/js" -> (codecBloscZstdJS / Compile / doc).value
  )
  val siteRoot = (docs / target).value / "docs" / "site"
  apiDocs.foreach { case (relative, source) =>
    val sourceIndex = source / "index.html"
    if (!sourceIndex.isFile) {
      throw new MessageOnlyException(s"missing Scaladoc index: $sourceIndex")
    }
    val destination = siteRoot / "api" / relative
    IO.delete(destination)
    IO.copyDirectory(source, destination)
  }
  siteRoot
}

addCommandAlias(
  "compileAll",
  ";coreJVM/compile;coreJS/compile;codecBloscZstdJVM/compile;codecBloscZstdJS/compile;benchmarks/compile"
)
addCommandAlias(
  "testAll",
  ";coreJVM/test;coreJS/test;codecBloscZstdJVM/test;codecBloscZstdJS/test;benchmarks/test"
)
addCommandAlias(
  "checkAll",
  ";scalafmtCheckAll;scalafmtSbtCheck;compileAll;testAll;docsBundle"
)

addCommandAlias("docsCheck", ";docsBundle")
addCommandAlias("performanceCheck", ";benchmarks/test")

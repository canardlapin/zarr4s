import org.scalajs.linker.interface.ModuleKind
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport.*
import org.typelevel.sbt.gha.JavaSpec
import sbtcrossproject.CrossPlugin.autoImport.*
import scalajscrossproject.ScalaJSCrossPlugin.autoImport.*

val Scala3 = "3.7.4"
val munitV = "1.2.1"

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
  .aggregate(core, codecBloscZstd)

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
      tlSitePublishBranch := None,
      tlSitePublishTags := false
    )

addCommandAlias(
  "compileAll",
  ";coreJVM/compile;coreJS/compile;codecBloscZstdJVM/compile;codecBloscZstdJS/compile"
)
addCommandAlias(
  "testAll",
  ";coreJVM/test;coreJS/test;codecBloscZstdJVM/test;codecBloscZstdJS/test"
)
addCommandAlias(
  "checkAll",
  ";scalafmtCheckAll;scalafmtSbtCheck;compileAll;testAll;docs/tlSite"
)

addCommandAlias("docsCheck", ";docs/tlSite")

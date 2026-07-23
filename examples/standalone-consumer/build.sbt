import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport.*
import sbtcrossproject.CrossPlugin.autoImport.*
import scalajscrossproject.ScalaJSCrossPlugin.autoImport.*

ThisBuild / scalaVersion := "3.7.4"

lazy val consumer =
  crossProject(JSPlatform, JVMPlatform)
    .crossType(CrossType.Full)
    .in(file("."))
    .settings(
      name := "scala-zarr-standalone-consumer",
      scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
      libraryDependencies += "scalafim" %%% "scalafim-zarr" % "0.1.0-SNAPSHOT"
    )
    .jsSettings(
      scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule))
    )

lazy val consumerJVM = consumer.jvm
lazy val consumerJS = consumer.js

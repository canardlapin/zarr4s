import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport.*
import sbtcrossproject.CrossPlugin.autoImport.*
import scalajscrossproject.ScalaJSCrossPlugin.autoImport.*
import scala.sys.process.*

ThisBuild / scalaVersion := "3.7.4"

lazy val zarr4sVersion =
  sys.props.getOrElse(
    "zarr4s.version",
    s"0.1-${Process(Seq("git", "rev-parse", "--short=7", "HEAD"), file("../..")).!!.trim}-SNAPSHOT"
  )

lazy val consumer =
  crossProject(JSPlatform, JVMPlatform)
    .crossType(CrossType.Full)
    .in(file("."))
    .settings(
      name := "zarr4s-standalone-consumer",
      scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
      libraryDependencies += "io.github.canardlapin" %%% "zarr4s-core" % zarr4sVersion
    )
    .jsSettings(
      scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule))
    )

lazy val consumerJVM = consumer.jvm
lazy val consumerJS = consumer.js

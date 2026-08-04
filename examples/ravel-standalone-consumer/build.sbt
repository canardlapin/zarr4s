import org.scalajs.linker.interface.ModuleKind
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport.*
import sbtcrossproject.CrossPlugin.autoImport.*
import scalajscrossproject.ScalaJSCrossPlugin.autoImport.*

ThisBuild / scalaVersion := "3.7.4"

lazy val zarr4sVersion =
  sys.props.getOrElse("zarr4s.version", "0.1.0-z11.dcffada")

lazy val ravelVersion =
  sys.props.getOrElse("ravel.version", "0.0.0-d0f7bac")

lazy val consumer =
  crossProject(JSPlatform, JVMPlatform)
    .crossType(CrossType.Full)
    .in(file("."))
    .settings(
      name := "zarr4s-ravel-standalone-consumer",
      resolvers += Resolver.mavenLocal,
      scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
      libraryDependencies ++= Seq(
        "io.github.canardlapin" %%% "zarr4s-interop-ravel" % zarr4sVersion,
        "io.github.canardlapin" %%% "ravel-core" % ravelVersion
      ),
      dependencyOverrides +=
        "io.github.canardlapin" %%% "ravel-core" % ravelVersion
    )
    .jsSettings(
      scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule)),
      scalaJSUseMainModuleInitializer := true
    )

lazy val consumerJVM = consumer.jvm
lazy val consumerJS = consumer.js

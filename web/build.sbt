import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._

val ScalatraVersion = "2.5.0"

ScalatraPlugin.scalatraSettings

scalateSettings

organization := "com.example"

name := "My Scalatra Web App"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
  "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.5" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "container",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "org.apache.derby" % "derbyclient" % "10.12.1.1",
  "com.typesafe.slick" %% "slick" % "3.1.1",
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.slick" % "slick-hikaricp_2.11" % "3.1.1",
  "org.scalatra" %% "scalatra-json" % ScalatraVersion,
  "org.json4s" %% "json4s-jackson" % "3.3.0"
)

scalateTemplateConfig in Compile := {
  val base = (sourceDirectory in Compile).value
  Seq(
    TemplateConfig(
      base / "webapp" / "WEB-INF" / "templates",
      Seq.empty,  /* default imports should be added here */
      Seq(
        Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
      ),  /* add extra bindings here */
      Some("templates")
    )
  )
}

enablePlugins(JettyPlugin)

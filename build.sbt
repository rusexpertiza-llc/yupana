lazy val yupana = (project in file("."))
  .aggregate(api)
  .settings(noPublishSettings, commonSettings)

lazy val api = (project in file("yupana-api"))
  .settings(
    commonSettings,
    publishSettings
  )

val commonSettings = Seq(
  organization := "org.yupana",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq("2.11.12", "2.12.8")
)

val noPublishSettings = Seq(
  publish / skip := true
)

val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    if (isSnapshot.value)
      Some("nexus ru snapshots" at "https://nexus.esc-hq.ru/nexus/content/repositories/ru-snapshots/")
    else
      Some("nexus ru releases" at "https://nexus.esc-hq.ru/nexus/content/repositories/ru-release/")
  },
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },
  licenses += ("Apache 2.0 License", url("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://www.yupana.org")),
  developers := List(
    Developer("rusexpertiza", "Rusexpertiza LLC", url("https://www.1-ofd.ru"))
  )
)

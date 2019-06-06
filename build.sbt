import scalapb.compiler.Version.scalapbVersion

lazy val yupana = (project in file("."))
  .aggregate(api, proto, jdbc)
  .settings(noPublishSettings, commonSettings)

lazy val api = (project in file("yupana-api"))
  .settings(
    name := "yupana-api",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "joda-time"              %  "joda-time"            % versions.joda,
      "org.scalatest"          %% "scalatest"            % versions.scalaTest         % Test,
      "org.scalacheck"         %% "scalacheck"           % versions.scalaCheck        % Test
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val proto = (project in file("yupana-proto"))
  .settings(
    name := "yupana-proto",
    commonSettings,
    publishSettings,
    PB.protocVersion := "-v261",
    Compile / PB.targets := Seq (
      scalapb.gen(grpc = false) -> (Compile / sourceManaged).value
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb"   %% "scalapb-runtime"      % scalapbVersion             % "protobuf"  exclude("com.google.protobuf", "protobuf-java"),
      "com.google.protobuf"    %  "protobuf-java"        % versions.protobufJava force()
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val jdbc = (project in file("yupana-jdbc"))
  .settings(
    name := "yupana-jdbc",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "org.scalatest"          %% "scalatest"            % versions.scalaTest         % Test,
      "org.scalamock"          %% "scalamock"            % versions.scalaMock         % Test
    ),
    buildInfoKeys := {
      val vn = VersionNumber(version.value)
      Seq[BuildInfoKey](
        version,
        "majorVersion" -> vn.numbers(0).toInt,
        "minorVersion" -> vn.numbers(1).toInt
      )
    },
    buildInfoPackage := "org.yupana.jdbc.build",
    Compile / assembly / artifact := {
      val art = (Compile / assembly / artifact).value
      art.withClassifier(Some("driver"))
    },
    addArtifact(Compile / assembly / artifact, assembly)
  )
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(AssemblyPlugin)
  .dependsOn(api, proto)

lazy val versions = new {
  val joda = "2.10.1"

  val protobufJava = "2.6.1"

  val scalaTest = "3.0.7"
  val scalaCheck = "1.14.0"
  val scalaMock = "4.1.0"
}

val commonSettings = Seq(
  organization := "org.yupana",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.12", "2.12.8")
)

val noPublishSettings = Seq(
  publish / skip := true
)

val publishSettings = Seq(
  publishMavenStyle := true,
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials_nexus"),
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
    Developer("rusexpertiza", "Rusexpertiza LLC", "info@1-ofd.ru", url("https://www.1-ofd.ru"))
  )
)

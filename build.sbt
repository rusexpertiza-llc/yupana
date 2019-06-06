import scalapb.compiler.Version.scalapbVersion

lazy val yupana = (project in file("."))
  .aggregate(api, proto, jdbc, utils, core, hbase)
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
    pbSettings,
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

lazy val utils = (project in file ("yupana-utils"))
  .settings(
    name := "yupana-utils",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "org.apache.lucene"           %  "lucene-analyzers-common"       % versions.lucene,
      "org.scalatest"               %% "scalatest"                     % versions.scalaTest
    )
  )

lazy val core = (project in file ("yupana-core"))
  .settings(
    name := "yupana-core",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging"  %% "scala-logging"                % versions.scalaLogging,
      "com.lihaoyi"                 %% "fastparse"                    % versions.fastparse,
      "org.apache.ignite"           %  "ignite-core"                  % versions.ignite,
      "org.apache.ignite"           %  "ignite-slf4j"                 % versions.ignite,
      "org.ehcache"                 %  "ehcache"                      % versions.ehcache,

      "org.scalatest"               %% "scalatest"                    % versions.scalaTest          % Test,
      "org.scalamock"               %% "scalamock"                    % versions.scalaMock          % Test
    )
  )
  .dependsOn(api, utils)

lazy val hbase = (project in file("yupana-hbase"))
  .settings(
    name := "yupana-hbase",
    commonSettings,
    publishSettings,
    pbSettings,
    libraryDependencies ++= Seq(
      "org.apache.hbase"            %  "hbase-common"                 % versions.hbase,
      "org.apache.hbase"            %  "hbase-client"                 % versions.hbase,
      "org.apache.hadoop"           %  "hadoop-common"                % versions.hadoop,
      "org.apache.hadoop"           %  "hadoop-hdfs-client"           % versions.hadoop,
      "com.thesamet.scalapb"        %% "scalapb-runtime"              % scalapbVersion                    % "protobuf"  exclude("com.google.protobuf", "protobuf-java"),
      "com.google.protobuf"         %  "protobuf-java"                % versions.protobufJava force(),
      "org.scalatest"               %% "scalatest"                    % versions.scalaTest                % Test,
      "org.scalamock"               %% "scalamock"                    % versions.scalaMock                % Test,
      "org.scalacheck"              %% "scalacheck"                   % versions.scalaCheck               % Test
    )
  )
  .dependsOn(core % "compile->compile ; test->test")

lazy val versions = new {
  val joda = "2.10.2"

  val protobufJava = "2.6.1"

  val scalaLogging = "3.9.2"
  val fastparse = "1.0.0"

  val hbase = "1.3.1"
  val hadoop = "2.8.3"
  val spark = "2.4.3"

  val lucene = "6.6.0"
  val ignite = "2.7.0"
  val ehcache = "3.3.2"

  val scalaTest = "3.0.7"
  val scalaCheck = "1.14.0"
  val scalaMock = "4.1.0"
}

val commonSettings = Seq(
  organization := "org.yupana",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.12", "2.12.8"),
  parallelExecution in Test := false
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

val pbSettings = Seq(
  PB.protocVersion := "-v261",
  Compile / PB.targets := Seq (
    scalapb.gen(grpc = false) -> (Compile / sourceManaged).value
  )
)

import scalapb.compiler.Version.scalapbVersion

lazy val yupana = (project in file("."))
  .aggregate(api, proto, jdbc, utils, core, hbase, akka, spark, schema, externalLinks, examples)
  .settings(noPublishSettings, commonSettings, crossScalaVersions := Nil)

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
  .disablePlugins(AssemblyPlugin)

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
  .disablePlugins(AssemblyPlugin)

lazy val akka = (project in file("yupana-akka"))
  .settings(
    name := "yupana-akka",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.akka"             %% "akka-actor"                 % versions.akka,
      "com.typesafe.akka"             %% "akka-stream"                % versions.akka,
      "com.typesafe.scala-logging"    %% "scala-logging"              % versions.scalaLogging,
      "com.google.protobuf"           %  "protobuf-java"              % versions.protobufJava force(),
      "org.scalatest"                 %% "scalatest"                  % versions.scalaTest                % Test,
      "com.typesafe.akka"             %% "akka-stream-testkit"        % versions.akka                     % Test
    )
  )
  .dependsOn(proto, core)
  .disablePlugins(AssemblyPlugin)

lazy val spark = (project in file("yupana-spark"))
  .settings(
    name := "yupana-spark",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark"            %% "spark-core"                     % versions.spark          % Provided,
      "org.apache.spark"            %% "spark-sql"                      % versions.spark          % Provided,
      "org.apache.spark"            %% "spark-streaming"                % versions.spark          % Provided,
      "org.apache.hbase"            % "hbase-server"                    % versions.hbase,
      "org.apache.hbase"            % "hbase-hadoop-compat"             % versions.hbase
    )
  )
  .dependsOn(core, hbase, externalLinks)
  .disablePlugins(AssemblyPlugin)

lazy val schema = (project in file("yupana-schema"))
  .settings(
    name := "yupana-schema",
    commonSettings,
    publishSettings
  )
  .dependsOn(api, utils)
  .disablePlugins(AssemblyPlugin)

lazy val externalLinks = (project in file("yupana-external-links"))
  .settings(
    name := "yupana-external-links",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"             % versions.json4s,
      "org.springframework"         %  "spring-jdbc"                % versions.spring,
      "org.scalatest"               %% "scalatest"                  % versions.scalaTest        % Test,
      "com.h2database"              %  "h2"                         % versions.h2Jdbc           % Test,
      "org.flywaydb"                %  "flyway-core"                % versions.flyway           % Test,
      "ch.qos.logback"              %  "logback-classic"            % versions.logback          % Test
    )
  )
  .dependsOn(schema, core)
  .disablePlugins(AssemblyPlugin)

lazy val examples = (project in file("yupana-examples"))
  .settings(
    name := "yupana-examples",
    commonSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark"            %% "spark-core"                     % versions.spark          % Provided,
      "org.apache.spark"            %% "spark-sql"                      % versions.spark          % Provided,
      "org.apache.spark"            %% "spark-streaming"                % versions.spark          % Provided,
      "com.zaxxer"                  %  "HikariCP"                       % versions.hikariCP,
      "org.postgresql"              %  "postgresql"                     % versions.postgresqlJdbc % Runtime,
      "ch.qos.logback"              %  "logback-classic"                % versions.logback        % Runtime
    )
  )
  .dependsOn(spark, akka, hbase, schema, externalLinks)
  .enablePlugins(FlywayPlugin)

lazy val versions = new {
  val joda = "2.10.2"

  val protobufJava = "2.6.1"

  val scalaLogging = "3.9.2"
  val fastparse = "1.0.0"

  val hbase = "1.3.1"
  val hadoop = "2.8.3"
  val spark = "2.4.3"
  val akka = "2.5.23"

  val lucene = "6.6.0"
  val ignite = "2.7.0"
  val ehcache = "3.3.2"

  val json4s = "3.5.3"
  val spring = "5.0.8.RELEASE"

  val flyway = "5.2.4"
  val hikariCP = "3.3.1"
  val logback = "1.2.3"
  val h2Jdbc = "1.4.199"
  val postgresqlJdbc = "42.2.6"

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

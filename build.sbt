import scalapb.compiler.Version.scalapbVersion
import ReleaseTransformations._
import sbt.Keys.excludeDependencies

ThisBuild / useCoursier := false

lazy val yupana = (project in file("."))
  .aggregate(api, proto, jdbc, utils, core, hbase, akka, spark, schema, externalLinks, examples, ehcache, ignite, caffeine)
  .settings(
    allSettings,
    noPublishSettings,
    crossScalaVersions := Nil
  )

lazy val api = (project in file("yupana-api"))
  .settings(
    name := "yupana-api",
    allSettings,
    libraryDependencies ++= Seq(
      "joda-time"              %  "joda-time"            % versions.joda,
      "org.scalatest"          %% "scalatest"            % versions.scalaTest         % Test,
      "org.scalacheck"         %% "scalacheck"           % versions.scalaCheck        % Test,
      "org.scalatestplus"      %% "scalacheck-1-15"      % versions.scalaTestCheck    % Test
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val proto = (project in file("yupana-proto"))
  .settings(
    name := "yupana-proto",
    allSettings,
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
    allSettings,
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

lazy val utils = (project in file("yupana-utils"))
  .settings(
    name := "yupana-utils",
    allSettings,
    libraryDependencies ++= Seq(
      "org.apache.lucene"           %  "lucene-analyzers-common"       % versions.lucene,
      "org.scalatest"               %% "scalatest"                     % versions.scalaTest % Test
    )
  )
  .dependsOn(api)

lazy val core = (project in file("yupana-core"))
  .settings(
    name := "yupana-core",
    allSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging"    %% "scala-logging"                % versions.scalaLogging,
      "com.lihaoyi"                   %% "fastparse"                    % versions.fastparse,
      "javax.cache"                   %  "cache-api"                    % "1.1.1",
      "org.scalatest"                 %% "scalatest"                    % versions.scalaTest          % Test,
      "org.scalamock"                 %% "scalamock"                    % versions.scalaMock          % Test
    )
  )
  .dependsOn(api, utils % Test)
  .disablePlugins(AssemblyPlugin)

lazy val hbase = (project in file("yupana-hbase"))
  .settings(
    name := "yupana-hbase",
    allSettings,
    pbSettings,
    libraryDependencies ++= Seq(
      "org.apache.hbase"            %  "hbase-common"                 % versions.hbase,
      "org.apache.hbase"            %  "hbase-client"                 % versions.hbase,
      "org.apache.hadoop"           %  "hadoop-common"                % versions.hadoop, // excludeAll(ExclusionRule(organization = "org.eclipse.jetty")),
      "org.apache.hadoop"           %  "hadoop-hdfs-client"           % versions.hadoop,
      "com.thesamet.scalapb"        %% "scalapb-runtime"              % scalapbVersion                    % "protobuf"  exclude("com.google.protobuf", "protobuf-java"),
      "com.google.protobuf"         %  "protobuf-java"                % versions.protobufJava force(),
      "org.scalatest"               %% "scalatest"                    % versions.scalaTest                % Test,
      "org.scalamock"               %% "scalamock"                    % versions.scalaMock                % Test,
      "org.scalacheck"              %% "scalacheck"                   % versions.scalaCheck               % Test,
      "org.apache.hbase"            %  "hbase-server"                 % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-server"                 % versions.hbase                    % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-common"                 % versions.hbase                    % Test classifier "tests",
      "org.apache.hadoop"           %  "hadoop-hdfs"                  % versions.hadoop                   % Test,
      "org.apache.hadoop"           %  "hadoop-hdfs"                  % versions.hadoop                   % Test classifier "tests",
      "org.apache.hadoop"           %  "hadoop-common"                % versions.hadoop                   % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-hadoop-compat"          % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-hadoop-compat"          % versions.hbase                    % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-zookeeper"              % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-zookeeper"              % versions.hbase                    % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-http"                   % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-metrics-api"            % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-metrics"                % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-asyncfs"                % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-logging"                % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-hadoop2-compat"         % versions.hbase                    % Test,
      "org.apache.hbase"            %  "hbase-hadoop2-compat"         % versions.hbase                    % Test classifier "tests",
      "org.apache.hadoop"           %  "hadoop-mapreduce-client-core" % versions.hadoop                   % Test,
      "junit"                       %  "junit"                        % "4.13"                            % Test,
      "jakarta.ws.rs"               %  "jakarta.ws.rs-api"            % "2.1.5"                           % Test,
      "ch.qos.logback"              %  "logback-classic"              % versions.logback                  % Test,
      "org.slf4j"                   %  "log4j-over-slf4j"             % "1.7.30"                          % Test,
      "javax.activation"            %  "javax.activation-api"         % "1.2.0"                           % Test
    ),
    excludeDependencies ++= Seq(
      // workaround for https://github.com/sbt/sbt/issues/3618
      // include "jakarta.ws.rs" % "jakarta.ws.rs-api" instead
      "javax.ws.rs" % "javax.ws.rs-api",
      "org.slf4j" % "slf4j-log4j12"
    )
  )
  .dependsOn(core % "compile->compile ; test->test", caffeine % Test)
  .disablePlugins(AssemblyPlugin)

lazy val akka = (project in file("yupana-akka"))
  .settings(
    name := "yupana-akka",
    allSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.akka"             %% "akka-actor"                 % versions.akka,
      "com.typesafe.akka"             %% "akka-stream"                % versions.akka,
      "com.typesafe.scala-logging"    %% "scala-logging"              % versions.scalaLogging,
      "com.google.protobuf"           %  "protobuf-java"              % versions.protobufJava force(),
      "org.scalatest"                 %% "scalatest"                  % versions.scalaTest                % Test,
      "org.scalamock"                 %% "scalamock"                  % versions.scalaMock                % Test,
      "com.typesafe.akka"             %% "akka-stream-testkit"        % versions.akka                     % Test
    )
  )
  .dependsOn(proto, core, schema % Test)
  .disablePlugins(AssemblyPlugin)

lazy val spark = (project in file("yupana-spark"))
  .settings(
    name := "yupana-spark",
    allSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark"            %% "spark-core"                     % versions.spark          % Provided,
      "org.apache.spark"            %% "spark-sql"                      % versions.spark          % Provided,
      "org.apache.spark"            %% "spark-streaming"                % versions.spark          % Provided,
      "org.apache.hbase"            %  "hbase-mapreduce"                % versions.hbase,
      "org.scalatest"               %% "scalatest"                      % versions.scalaTest      % Test,
      "com.holdenkarau"             %% "spark-testing-base"             % versions.sparkTesting   % Test,
      "org.apache.hbase"            %  "hbase-server"                   % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-server"                   % versions.hbase          % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-common"                   % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-common"                   % versions.hbase          % Test classifier "tests",
      "org.apache.hadoop"           %  "hadoop-hdfs"                    % versions.hadoop         % Test,
      "org.apache.hadoop"           %  "hadoop-hdfs"                    % versions.hadoop         % Test classifier "tests",
      "org.apache.hadoop"           %  "hadoop-common"                  % versions.hadoop         % Test,
      "org.apache.hadoop"           %  "hadoop-common"                  % versions.hadoop         % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-hadoop-compat"            % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-hadoop-compat"            % versions.hbase          % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-hadoop2-compat"           % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-hadoop2-compat"           % versions.hbase          % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-zookeeper"                % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-zookeeper"                % versions.hbase          % Test classifier "tests",
      "org.apache.hbase"            %  "hbase-http"                     % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-metrics-api"              % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-metrics"                  % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-asyncfs"                  % versions.hbase          % Test,
      "org.apache.hbase"            %  "hbase-logging"                  % versions.hbase          % Test,
      "ch.qos.logback"              %  "logback-classic"              % versions.logback                  % Test,
      "jakarta.ws.rs"               %  "jakarta.ws.rs-api"              % "2.1.5"                 % Test,
      "javax.activation"            %  "javax.activation-api"           % "1.2.0"                 % Test
    ),
    excludeDependencies ++= Seq(
      // workaround for https://github.com/sbt/sbt/issues/3618
      // include "jakarta.ws.rs" % "jakarta.ws.rs-api" instead
      "javax.ws.rs" % "javax.ws.rs-api",
      "org.slf4j" % "slf4j-log4j12"
      )
  )
  .dependsOn(core, hbase, externalLinks)
  .disablePlugins(AssemblyPlugin)

lazy val schema = (project in file("yupana-schema"))
  .settings(
    name := "yupana-schema",
    allSettings,
    libraryDependencies ++= Seq(
      "org.scalatest"               %% "scalatest"                  % versions.scalaTest        % Test
    )
  )
  .dependsOn(api, utils)
  .disablePlugins(AssemblyPlugin)

lazy val externalLinks = (project in file("yupana-external-links"))
  .settings(
    name := "yupana-external-links",
    allSettings,
    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"             % versions.json4s,
      "org.scalatest"               %% "scalatest"                  % versions.scalaTest        % Test,
      "org.scalamock"               %% "scalamock"                  % versions.scalaMock        % Test,
      "com.h2database"              %  "h2"                         % versions.h2Jdbc           % Test,
      "org.flywaydb"                %  "flyway-core"                % versions.flyway           % Test,
      "ch.qos.logback"              %  "logback-classic"            % versions.logback          % Test
    )
  )
  .dependsOn(schema, core, ehcache % Test)
  .disablePlugins(AssemblyPlugin)

lazy val ehcache = (project in file("yupana-ehcache"))
  .settings(
    name := "yupana-ehcache",
    allSettings,
    libraryDependencies ++= Seq(
      "org.ehcache"                   %  "ehcache"                      % versions.ehcache
    )
  )
  .dependsOn(core)
  .disablePlugins(AssemblyPlugin)

lazy val caffeine = (project in file("yupana-caffeine"))
  .settings(
    name := "yupana-caffeine",
    allSettings,
    libraryDependencies ++= Seq(
      "com.github.ben-manes.caffeine" %  "caffeine"                     % versions.caffeine,
      "com.github.ben-manes.caffeine" %  "jcache"                       % versions.caffeine
    )
  )
  .dependsOn(core)
  .disablePlugins(AssemblyPlugin)

lazy val ignite = (project in file("yupana-ignite"))
  .settings(
    name := "yupana-ignite",
    allSettings,
    libraryDependencies ++= Seq(
      "org.apache.ignite"             %  "ignite-core"                  % versions.ignite,
      "org.apache.ignite"             %  "ignite-slf4j"                 % versions.ignite
    )
  )
  .dependsOn(core)
  .disablePlugins(AssemblyPlugin)

lazy val writeAssemblyName = taskKey[Unit]("Writes assembly filename into file")

lazy val examples = (project in file("yupana-examples"))
  .settings(
    name := "yupana-examples",
    allSettings,
    noPublishSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark"            %% "spark-core"                     % versions.spark                % Provided,
      "org.apache.spark"            %% "spark-sql"                      % versions.spark                % Provided,
      "org.apache.spark"            %% "spark-streaming"                % versions.spark                % Provided,
      "com.zaxxer"                  %  "HikariCP"                       % versions.hikariCP,
      "org.postgresql"              %  "postgresql"                     % versions.postgresqlJdbc       % Runtime,
      "ch.qos.logback"              %  "logback-classic"                % versions.logback              % Runtime
    ),
    excludeDependencies ++= Seq(
      "asm" % "asm"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("org", "apache", "jasper", _*)  => MergeStrategy.last
      case PathList("org", "apache", "commons", _*) => MergeStrategy.last
      case PathList("javax", "servlet", _*)         => MergeStrategy.last
      case PathList("javax", "el", _*)              => MergeStrategy.last
      case PathList(ps @ _*) if ps.last.endsWith(".proto") => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case PathList("META-INF", "native-image", "io.netty", "common", "native-image.properties") => MergeStrategy.first
      case PathList("org", "slf4j", "impl", _*)     => MergeStrategy.first
      case "module-info.class"                      => MergeStrategy.first
      case x                                        => (assembly / assemblyMergeStrategy).value(x)
    },
    writeAssemblyName := {
      val outputFile = target.value / "assemblyname.sh"
      val assemblyName = ((assembly / target).value / (assembly / assemblyJarName).value).getCanonicalPath
      streams.value.log.info("Assembly into: " + assemblyName)
      IO.write(outputFile, s"JARFILE=$assemblyName\n")
    },
    assembly := assembly.dependsOn(writeAssemblyName).value
  )
  .dependsOn(spark, akka, hbase, schema, externalLinks, ehcache % Runtime)
  .enablePlugins(FlywayPlugin)

lazy val versions = new {
  val spark =  "3.0.1"
  val sparkTesting = spark + "_1.0.0"

  val joda = "2.10.10"

  val protobufJava = "2.6.1"

  val scalaLogging = "3.9.2"
  val fastparse = "2.1.3"

  val hbase = "2.4.1"
  val hadoop = "3.0.3"
  val akka = "2.6.12"

  val lucene = "6.6.0"
  val ignite = "2.8.1"
  val ehcache = "3.3.2"
  val caffeine = "2.8.6"

  val json4s = "3.5.3"

  val flyway = "7.4.0"
  val hikariCP = "3.4.5"
  val logback = "1.2.3"
  val h2Jdbc = "1.4.199"
  val postgresqlJdbc = "42.2.18"

  val scalaTest = "3.2.6"
  val scalaCheck = "1.15.1"
  val scalaTestCheck = "3.2.4.0"
  val scalaMock = "5.1.0"
}

val commonSettings = Seq(
  organization := "org.yupana",
  scalaVersion := "2.12.12",
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-Xsource:2.12",
    "-deprecation",
    "-unchecked",
    "-feature",
    "-Xlint",
    "-Xfatal-warnings",
    "-Ywarn-dead-code",
    "-Ywarn-unused-import"
  ),
  Compile / console / scalacOptions --= Seq("-Ywarn-unused-import", "-Xfatal-warnings"),
  testOptions in Test += Tests.Argument("-l", "org.scalatest.tags.Slow"),
  parallelExecution in Test := false,
  coverageExcludedPackages := "<empty>;org\\.yupana\\.examples\\..*;org\\.yupana\\.proto\\..*;org\\.yupana\\.hbase\\.proto\\..*",
  headerLicense := Some(HeaderLicense.ALv2("2019", "Rusexpertiza LLC"))
)

val noPublishSettings = Seq(
  publish / skip := true
)

val publishSettings = Seq(
  publishMavenStyle := true,
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials_nexus"),
  publishTo := {
    if (isSnapshot.value)
      Some("nexus common snapshots" at "https://nexus.esc-hq.ru/nexus/content/repositories/common-snapshots/")
    else
      sonatypePublishToBundle.value
  },
  Test / publishArtifact := false,
  pomIncludeRepository := { _ =>
    false
  },
  licenses += ("Apache 2.0 License", url("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://github.com/rusexpertiza-llc/yupana")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/rusexpertiza-llc/yupana"),
      "scm:git:git@github.com/rusexpertiza-llc/yupana.git"
    )
  ),
  developers := List(
    Developer("rusexpertiza", "Rusexpertiza LLC", "info@1-ofd.ru", url("https://www.1-ofd.ru"))
  )
)

val pbSettings = Seq(
  PB.protocVersion := "-v261",
  Compile / PB.targets := Seq(
    scalapb.gen(grpc = false) -> (Compile / sourceManaged).value
  )
)

val releaseSettings = Seq(
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
//    releaseStepCommandAndRemaining("+publishSigned"),
//    releaseStepCommand("sonatypeBundleRelease"),
//    setNextVersion,
//    commitNextVersion,
    pushChanges
  )
)

val allSettings = commonSettings ++ publishSettings ++ releaseSettings

credentials in ThisBuild ++= (for {
  username <- Option(System.getenv().get("SONATYPE_USERNAME"))
  password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq

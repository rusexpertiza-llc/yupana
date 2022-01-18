import scalapb.compiler.Version.scalapbVersion
import ReleaseTransformations._
import sbt.Keys.excludeDependencies

ThisBuild / useCoursier := false

lazy val yupana = (project in file("."))
  .aggregate(
    api,
    proto,
    jdbc,
    utils,
    core,
    hbase,
    akka,
    spark,
    schema,
    externalLinks,
    examples,
    ehcache,
    ignite,
    caffeine,
    benchmarks
  )
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
      "org.threeten"           %  "threeten-extra"       % versions.threeTenExtra,
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
      "org.scalatest"          %% "scalatest"               % versions.scalaTest         % Test,
      "org.scalamock"          %% "scalamock"               % versions.scalaMock         % Test
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
      "org.scala-lang"                %  "scala-reflect"                % scalaVersion.value,
      "org.scala-lang"                %  "scala-compiler"               % scalaVersion.value,
      "org.scala-lang.modules"        %% "scala-collection-compat"      % versions.colCompat,
      "com.typesafe.scala-logging"    %% "scala-logging"                % versions.scalaLogging,
      "com.lihaoyi"                   %% "fastparse"                    % versions.fastparse,
      "javax.cache"                   %  "cache-api"                    % "1.1.1",
      "ch.qos.logback"                %  "logback-classic"              % versions.logback            % Test,
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
      "org.scala-lang.modules"      %% "scala-collection-compat"      % versions.colCompat,
      "org.apache.hbase"            %  "hbase-common"                 % versions.hbase,
      "org.apache.hbase"            %  "hbase-client"                 % versions.hbase,
      "org.apache.hadoop"           %  "hadoop-common"                % versions.hadoop,
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
      "javax.activation"            % "javax.activation-api"          % "1.2.0"                           % Test
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
      "org.apache.spark"            %% "spark-core"                     % versions.spark                % Provided,
      "org.apache.spark"            %% "spark-sql"                      % versions.spark                % Provided,
      "org.apache.spark"            %% "spark-streaming"                % versions.spark                % Provided,
      "org.apache.hbase"            %  "hbase-mapreduce"                % versions.hbase,
      "org.scalatest"               %% "scalatest"                      % versions.scalaTest            % Test
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
      "com.github.ben-manes.caffeine" %  "caffeine"                     % versions.caffeine
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

lazy val benchmarks = (project in file("yupana-benchmarks"))
  .enablePlugins(JmhPlugin)
  .settings(commonSettings, noPublishSettings)
  .dependsOn(core % "compile->test", api, schema, externalLinks, hbase, hbase % "compile->test")
  .settings(
    name := "yupana-benchmarks",
    libraryDependencies ++= Seq(
      "com.github.scopt"              %% "scopt"                      % versions.scopt,
      "io.prometheus"                 %  "simpleclient"               % versions.prometheus,
      "io.prometheus"                 %  "simpleclient_pushgateway"   % versions.prometheus,
      "jakarta.ws.rs"                 %  "jakarta.ws.rs-api"          % "2.1.5",
      "org.scalatest"                 %% "scalatest"                  % versions.scalaTest    % Test
    ),
    excludeDependencies ++= Seq(
      // workaround for https://github.com/sbt/sbt/issues/3618
      // include "jakarta.ws.rs" % "jakarta.ws.rs-api" instead
      "javax.ws.rs" % "javax.ws.rs-api",
      "org.slf4j" % "slf4j-log4j12"
    )
  )

lazy val docs = project
  .in(file("yupana-docs"))
  .dependsOn(api, core)
  .enablePlugins(MdocPlugin, ScalaUnidocPlugin, DocusaurusPlugin)
  .settings(
    scalaVersion := "2.12.15",
    moduleName := "yupana-docs",
    noPublishSettings,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(api, core),
    ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    mdocIn := (LocalRootProject / baseDirectory).value / "docs" / "mdoc",
    mdocOut := (LocalRootProject / baseDirectory).value / "website" / "target" / "docs",
    Compile / resourceGenerators += Def.task {
      val imagesPath =  (LocalRootProject / baseDirectory).value / "docs" / "assets" / "images"
      val images = (imagesPath * "*").get()

      val targetPath = (LocalRootProject / baseDirectory).value / "website" / "static" / "assets" / "images"

      val pairs = images pair Path.rebase(imagesPath, targetPath)
      IO.copy(pairs, overwrite = true, preserveLastModified = true, preserveExecutable = false)

      pairs.map(_._2)
    }.taskValue,
    mdocVariables := Map(
      "SCALA_VERSION" -> minMaj(scalaVersion.value, "2.12"),
      "HBASE_VERSION" -> minMaj(versions.hbase, "1.3"),
      "HADOOP_VERSION" -> minMaj(versions.hadoop, "3.0"),
      "SPARK_VERSION" -> minMaj(versions.spark, "2.4"),
      "IGNITE_VERSION" -> versions.ignite
    )
  )

def minMaj(v: String, default: String): String = {
 val n = VersionNumber(v)
 val r = for {
   f <- n._1
   s <- n._2
 } yield s"$f.$s"
 r getOrElse default
}

lazy val versions = new {
  val colCompat = "2.1.1" // Same version with Spark
  val spark = "3.2.0"

  val threeTenExtra = "1.7.0"

  val protobufJava = "2.6.1"

  val scalaLogging = "3.9.4"
  val fastparse = "2.1.3"
  val scopt = "4.0.1"
  val prometheus = "0.9.0"

  val hbase = "2.4.1"
  val hadoop = "3.0.3"

  val akka = "2.6.17"

  val lucene = "6.6.0"
  val ignite = "2.8.1"
  val ehcache = "3.9.7"
  val caffeine = "2.9.3"

  val json4s = "3.7.0-M11" // Same version with Spark

  val flyway = "7.4.0"
  val hikariCP = "3.4.5"
  val logback = "1.2.6"
  val h2Jdbc = "1.4.200"
  val postgresqlJdbc = "42.2.24"

  val scalaTest = "3.2.10"
  val scalaCheck = "1.15.4"
  val scalaTestCheck = "3.2.10.0"
  val scalaMock = "5.1.0"
}

val commonSettings = Seq(
  organization := "org.yupana",
  scalaVersion := "2.13.6",
  crossScalaVersions := Seq("2.12.15", "2.13.6"),
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-Xsource:2.13",
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:higherKinds",
    "-Xlint",
    "-Xfatal-warnings",
    "-Ywarn-dead-code"
  ) ++ {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2,13)) => Seq("-Wconf:cat=unused:info")
      case _ => Seq.empty
    }
  },
  Compile / console / scalacOptions --= Seq("-Ywarn-unused-import", "-Xfatal-warnings"),
  Test / testOptions += Tests.Argument("-l", "org.scalatest.tags.Slow"),
  Test / parallelExecution := false,
  coverageExcludedPackages := "<empty>;org\\.yupana\\.examples\\..*;org\\.yupana\\.proto\\..*;org\\.yupana\\.hbase\\.proto\\..*;org\\.yupana\\.benchmarks\\..*",
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

ThisBuild / credentials ++= (for {
  username <- Option(System.getenv().get("SONATYPE_USERNAME"))
  password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq

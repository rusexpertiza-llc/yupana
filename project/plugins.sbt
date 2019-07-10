addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.23")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.8.4"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("io.github.davidmweber" % "flyway-sbt" % "5.2.0")

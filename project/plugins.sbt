addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.10")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.10.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.34")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.9.8"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")

addSbtPlugin("io.github.davidmweber" % "flyway-sbt" % "7.4.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")

addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.2.23")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.1")

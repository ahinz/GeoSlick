import sbt._
import sbt.Keys._

object MyBuild extends Build {
  lazy val project = Project("root", file(".")) settings(
    //organization := "org.sample.demo",

    name := "slickstuff",

    scalaVersion := "2.10.0-RC5",

    scalacOptions ++= Seq("-deprecation", "-unchecked", "-optimize"),

    parallelExecution := false,

    libraryDependencies ++= Seq(
        "com.typesafe" % "slick_2.10.0-RC5" % "0.11.2",
        "org.slf4j" % "slf4j-nop" % "1.6.4",
        "postgresql" % "postgresql" % "8.4-702.jdbc4",
        "com.vividsolutions" % "jts" % "1.12"
    ),

    resolvers ++= Seq(
      "NL4J Repository" at "http://nativelibs4java.sourceforge.net/maven/",
      "maven2 dev repository" at "http://download.java.net/maven/2",
      "Scala Test" at "http://www.scala-tools.org/repo-reloases/",
      "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
      "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      "sonatypeSnapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "JAI" at "http://download.osgeo.org/webdav/geotools/"
    ),

    resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns),

    javaOptions in run += "-Xmx6G",
    // enable forking in run
    fork in run := true
  )
}

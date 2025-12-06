
/***** DEFINE THESE PARAMS *****/
val projectName = "hello-flink-kafka"
val mainClassName = "example.Hello"
val outputJarName = "myProject.jar"
/*******************************/

val scalaVer = "2.13.16"
val flinkVer = "2.0.0"
val kafkaVer = "4.0.0"
val log4JVer = "2.24.1"

name := projectName
version := "0.1-SNAPSHOT"
organization := "org.example"

ThisBuild / scalaVersion := scalaVer

val flinkDependencies = Seq(
      "org.apache.flink" % "flink-core" % flinkVer % "provided",
      "org.apache.flink" % "flink-streaming-java" % flinkVer % "provided",
      "org.apache.flink" % "flink-runtime" % flinkVer % "provided",
      "org.apache.flink" % "flink-clients" % flinkVer % "provided",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4JVer % "provided"
      )

val kafkaDependencies = Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVer, // not provided, must be on the compile classpath AND in assembled jar
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4JVer % "provided"
      )


lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies ++ kafkaDependencies,
    libraryDependencies += "org.apache.flink" % "flink-connector-base" % flinkVer % "provided",
    libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % "4.0.0-2.0" // not provided, must be on the compile classpath AND in assembled jar
  )

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

assembly / mainClass := Some(mainClassName)
assembly / assemblyJarName := outputJarName

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)

// discard useless stuff that may give conflicts
assembly / assemblyMergeStrategy := {
  case PathList("module-info.class") =>
    MergeStrategy.discard
  case PathList("META-INF", "versions", _ @ _*) =>
    MergeStrategy.discard
  case PathList("META-INF", xs @ _*) =>
    xs.map(_.toLowerCase) match {
      case ("manifest.mf" :: Nil) => MergeStrategy.discard
      case ("index.list" :: Nil) => MergeStrategy.discard
      case ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

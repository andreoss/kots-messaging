import Dependencies.*

lazy val commonSettings = Seq(
  organization := "kots.messaging",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala213, scala3),
  libraryDependencies ++= common,
  Compile / compile / scalacOptions ++= ScalacOptions.forVersion(scalaVersion.value),
  Test / publishArtifact := false,
  coverageMinimumStmtTotal := 85,
  coverageFailOnMinimum := true,
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(name := "kots-messaging", publish / skip := true)
  .aggregate(core, mem, stream, interop, bench)

lazy val core = project
  .in(file("modules/core"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-core")

lazy val mem = project
  .in(file("modules/mem"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-mem")
  .dependsOn(core % "compile->compile;test->test")

lazy val kafka = project
  .in(file("modules/kafka"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-kafka")
  .settings(libraryDependencies += kafkaClients)
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val amqp = project
  .in(file("modules/amqp"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-amqp")
  .settings(libraryDependencies += amqpClient)
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val sqs = project
  .in(file("modules/sqs"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-sqs")
  .settings(libraryDependencies += awsSqs)
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val jms = project
  .in(file("modules/jms"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-jms")
  .settings(libraryDependencies ++= Seq(jmsApi, activemqClient, artemisClient))
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val stream = project
  .in(file("modules/stream"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-stream")
  .settings(libraryDependencies += fs2Core)
  .dependsOn(core % "compile->compile;test->test", mem % "test->compile")

lazy val interop = project
  .in(file("modules/interop"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-interop")
  .settings(libraryDependencies ++= Seq(zio, zioInteropCats))
  .dependsOn(core % "compile->compile;test->test", mem % "test->compile")

lazy val prometheus = project
  .in(file("modules/prometheus"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-prometheus")
  .settings(libraryDependencies ++= Seq(prometheusCore, prometheusHttpServer))
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test", mem % "test->compile")

lazy val bench = project
  .in(file("modules/bench"))
  .settings(commonSettings)
  .settings(name := "kots-messaging-bench")
  .dependsOn(core, mem)

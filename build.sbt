import Dependencies.*

lazy val commonSettings = Seq(
  organization := "kots.mq",
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
  .settings(name := "kots-mq", publish / skip := true)
  .aggregate(core, mem)

lazy val core = project
  .in(file("modules/core"))
  .settings(commonSettings)
  .settings(name := "kots-mq-core")

lazy val mem = project
  .in(file("modules/mem"))
  .settings(commonSettings)
  .settings(name := "kots-mq-mem")
  .dependsOn(core % "compile->compile;test->test")

lazy val kafka = project
  .in(file("modules/kafka"))
  .settings(commonSettings)
  .settings(name := "kots-mq-kafka")
  .settings(libraryDependencies += kafkaClients)
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val amqp = project
  .in(file("modules/amqp"))
  .settings(commonSettings)
  .settings(name := "kots-mq-amqp")
  .settings(libraryDependencies += amqpClient)
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val sqs = project
  .in(file("modules/sqs"))
  .settings(commonSettings)
  .settings(name := "kots-mq-sqs")
  .settings(libraryDependencies += awsSqs)
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

lazy val jms = project
  .in(file("modules/jms"))
  .settings(commonSettings)
  .settings(name := "kots-mq-jms")
  .settings(libraryDependencies ++= Seq(jmsApi, activemqClient, artemisClient))
  .settings(Test / fork := true)
  .dependsOn(core % "compile->compile;test->test")

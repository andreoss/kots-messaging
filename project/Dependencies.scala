import sbt.*

object Dependencies {

  val catsCore = "org.typelevel" %% "cats-core" % "2.13.0"
  val catsEffect = "org.typelevel" %% "cats-effect" % "3.7.1"
  val munitCatsEffect = "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit" % "3.7.1" % Test

  val kafkaClients = "org.apache.kafka" % "kafka-clients" % "3.9.1"

  val scala213 = "2.13.18"
  val scala3 = "3.3.8"

  val common = Seq(catsCore, catsEffect, munitCatsEffect, catsEffectTestkit)
}

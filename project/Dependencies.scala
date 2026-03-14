import sbt.*

object Dependencies {

  val catsCore = "org.typelevel" %% "cats-core" % "2.13.0"
  val catsEffect = "org.typelevel" %% "cats-effect" % "3.7.1"
  val munitCatsEffect = "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit" % "3.7.1" % Test

  val kafkaClients = "org.apache.kafka" % "kafka-clients" % "3.9.1"

  val amqpClient = "com.rabbitmq" % "amqp-client" % "5.25.0"

  val awsSqs = "software.amazon.awssdk" % "sqs" % "2.55.0"
  val jmsApi = "jakarta.jms" % "jakarta.jms-api" % "3.1.0"
  val activemqClient = "org.apache.activemq" % "activemq-client-jakarta" % "6.1.0" % Test
  val artemisClient = "org.apache.activemq" % "artemis-jakarta-client" % "2.57.0" % Test

  val fs2Core = "co.fs2" %% "fs2-core" % "3.14.0"
  val zio = "dev.zio" %% "zio" % "2.1.26" % Test
  val zioInteropCats = "dev.zio" %% "zio-interop-cats" % "23.1.0.13" % Test

  val prometheusCore = "io.prometheus" % "prometheus-metrics-core" % "1.9.0"
  val prometheusHttpServer =
    "io.prometheus" % "prometheus-metrics-exporter-httpserver" % "1.9.0" % Test

  val scala213 = "2.13.18"
  val scala3 = "3.3.8"

  val common = Seq(catsCore, catsEffect, munitCatsEffect, catsEffectTestkit)
}

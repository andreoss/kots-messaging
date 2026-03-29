package kots.messaging.mem

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MemLateSettleSuite extends CatsEffectSuite {

  private val destination = Destination("late")

  private val settings =
    ConsumerSettings.default.withLease(5.seconds).withBackoff(Backoff.none).withMaxAttempts(9)

  test("an acknowledgement after the lease expired does not settle the redelivered copy") {
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (
          broker.producer(destination),
          broker.consumer(destination, settings),
          broker.consumer(destination, settings),
        ).tupled.use { case (producer, first, second) =>
          for {
            _ <- producer.send(Message.of("body"))
            held <- first.receive
            _ <- IO.sleep(6.seconds)
            taken <- second.receive
            _ <- held.traverse_(_.ack)
            depth <- broker.admin.depth(destination)
            stillHeld <- first.receive
          } yield {
            assertEquals(held.map(_.envelope.attempt), Some(1))
            assertEquals(taken.map(_.envelope.attempt), Some(2))
            assertEquals(depth, Some(0L))
            assertEquals(stillHeld.map(_.envelope.attempt), None)
          }
        }
      }
    )
  }

  test("a rejection after the lease expired does not requeue the message twice") {
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (broker.producer(destination), broker.consumer(destination, settings)).tupled.use {
          case (producer, consumer) =>
            for {
              _ <- producer.send(Message.of("body"))
              held <- consumer.receive
              _ <- IO.sleep(6.seconds)
              again <- consumer.receive
              _ <- held.traverse_(_.reject)
              depth <- broker.admin.depth(destination)
              _ <- again.traverse_(_.ack)
              afterAck <- broker.admin.depth(destination)
            } yield {
              assertEquals(depth, Some(0L))
              assertEquals(afterAck, Some(0L))
            }
        }
      }
    )
  }
}

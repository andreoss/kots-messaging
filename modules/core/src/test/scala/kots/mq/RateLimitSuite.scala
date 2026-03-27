package kots.mq

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class RateLimitSuite extends CatsEffectSuite {

  private val destination = Destination("paced")

  private def endpoints[A](
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    StubBroker.create[IO, String](Capabilities.none).flatMap { broker =>
      (broker.producer(destination), broker.consumer(destination, ConsumerSettings.default)).tupled
        .use { case (producer, consumer) => f(producer, consumer) }
    }

  test("a paced consumer hands out no faster than its bound") {
    TestControl.executeEmbed(
      endpoints { (producer, consumer) =>
        for {
          _ <- List.range(0, 5).traverse_(index => producer.send(Message.of(s"body-$index")))
          paced <- RateLimit.tokenBucket(consumer, perSecond = 2.0, burst = 1)
          start <- IO.monotonic
          _ <- List.range(0, 5).traverse_(_ => paced.receive)
          elapsed <- IO.monotonic.map(_ - start)
        } yield assert(elapsed >= 2.seconds, s"five at two a second took $elapsed")
      }
    )
  }

  test("a burst is allowed before the pace applies") {
    TestControl.executeEmbed(
      endpoints { (producer, consumer) =>
        for {
          _ <- List.range(0, 3).traverse_(index => producer.send(Message.of(s"body-$index")))
          paced <- RateLimit.tokenBucket(consumer, perSecond = 1.0, burst = 3)
          start <- IO.monotonic
          received <- List.range(0, 3).traverse(_ => paced.receive)
          elapsed <- IO.monotonic.map(_ - start)
        } yield {
          assertEquals(received.flatten.size, 3)
          assertEquals(elapsed, Duration.Zero)
        }
      }
    )
  }

  test("pacing does not touch settlement") {
    TestControl.executeEmbed(
      endpoints { (producer, consumer) =>
        for {
          _ <- producer.send(Message.of("body"))
          paced <- RateLimit.tokenBucket(consumer, perSecond = 100.0, burst = 10)
          received <- paced.receive
          _ <- received.traverse_(delivery => paced.ackAll(List(delivery)))
          again <- paced.receive
        } yield assertEquals(again.map(_.envelope.message.payload), None)
      }
    )
  }
}

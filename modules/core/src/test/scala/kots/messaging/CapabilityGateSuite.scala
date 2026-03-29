package kots.messaging

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class CapabilityGateSuite extends CatsEffectSuite {

  private val destination = Destination("gated")

  private def endpoints(declared: Capabilities) =
    StubBroker.create[IO, String](declared).map { broker =>
      (broker, broker.producer(destination), broker.consumer(destination, ConsumerSettings.default))
    }

  test("an adapter that declares a capability it lacks fails the check") {
    endpoints(Capabilities.of(Capability.Delay)).flatMap { case (_, producer, consumer) =>
      (producer, consumer).tupled
        .use { case (p, c) => CapabilityChecks.delay(p, c) }
        .attempt
        .map(result => assert(result.isLeft, "a lying adapter passed the delay check"))
    }
  }

  test("an undeclared capability is refused rather than emulated") {
    endpoints(Capabilities.none).flatMap { case (_, producer, _) =>
      producer
        .use(_.sendAfter(Message.of("body"), 1.second))
        .attempt
        .map(result => assert(result.left.exists(_.isInstanceOf[CapabilityUnsupported])))
    }
  }

  test("a declared capability is visible on the broker") {
    StubBroker.create[IO, String](Capabilities.of(Capability.Batch)).map { broker =>
      assert(broker.capabilities.has(Capability.Batch))
      assert(!broker.capabilities.has(Capability.Delay))
      assertEquals(
        broker.capabilities.and(Capability.Delay).has(Capability.Delay),
        true,
      )
    }
  }
}

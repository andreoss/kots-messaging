package kots.messaging

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class BackoffSuite extends CatsEffectSuite {

  private val policy = Backoff(1.second, 2.0, 10.seconds, 0.0)

  test("the first attempt waits the initial delay") {
    assertEquals(policy.delay(1, 1.0), 1.second)
  }

  test("each further attempt grows by the factor") {
    assertEquals(policy.delay(2, 1.0), 2.seconds)
    assertEquals(policy.delay(3, 1.0), 4.seconds)
  }

  test("growth stops at the cap") {
    assertEquals(policy.delay(9, 1.0), 10.seconds)
  }

  test("an attempt below one is treated as the first") {
    assertEquals(policy.delay(0, 1.0), 1.second)
    assertEquals(policy.delay(-3, 1.0), 1.second)
  }

  test("jitter shortens the delay and never lengthens it") {
    val jittered = Backoff(1.second, 2.0, 10.seconds, 0.5)
    assertEquals(jittered.delay(1, 1.0), 1.second)
    assertEquals(jittered.delay(1, 0.0), 500.millis)
    assertEquals(jittered.delay(1, 0.5), 750.millis)
  }

  test("a jitter sample outside the unit interval is clamped") {
    val jittered = Backoff(1.second, 2.0, 10.seconds, 1.0)
    assertEquals(jittered.delay(1, 5.0), 1.second)
    assertEquals(jittered.delay(1, -5.0), Duration.Zero)
  }

  test("the entropy port supplies the sample") {
    implicit val entropy: Entropy[IO] = Entropy.const[IO](0.0)
    val jittered = Backoff(1.second, 2.0, 10.seconds, 0.5)
    jittered.next[IO](1).assertEquals(500.millis)
  }
}

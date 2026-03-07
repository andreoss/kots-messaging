package kots.mq

import cats.effect.IO
import munit.CatsEffectSuite

final class EntropySuite extends CatsEffectSuite {

  test("a constant source returns its value") {
    Entropy.const[IO](0.25).nextDouble.assertEquals(0.25)
  }

  test("the system source stays within the unit interval") {
    Entropy
      .system[IO]
      .nextDouble
      .replicateA(64)
      .map(samples => assert(samples.forall(s => s >= 0.0 && s < 1.0)))
  }
}

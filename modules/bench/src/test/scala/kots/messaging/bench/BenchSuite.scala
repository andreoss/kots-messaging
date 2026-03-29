package kots.messaging.bench

import cats.effect.IO
import munit.CatsEffectSuite

final class BenchSuite extends CatsEffectSuite {

  test("the benchmark reports a timing for each path") {
    Bench.run[IO](200).map { timings =>
      assertEquals(timings.map(_.label), List("queue", "mem"))
      assert(timings.forall(_.elapsed.toNanos > 0L))
    }
  }
}

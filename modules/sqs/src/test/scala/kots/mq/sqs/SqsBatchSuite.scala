package kots.mq.sqs

import cats.effect.IO
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class SqsBatchSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  test("a batch send reports an outcome per entry") {
    val destination = SqsTestSupport.queue("batch")
    val bodies = List.range(0, 12).map(index => Message.of(s"body-$index"))
    SqsTestSupport.broker.use { broker =>
      (broker.producer(destination), broker.consumer(destination, SqsTestSupport.settingsWithoutBackoff)).tupled
        .use { case (producer, consumer) =>
          for {
            outcomes <- producer.sendBatch(bodies)
            received <- CapabilityChecks.batchWithin(consumer, bodies.size, 60.seconds)
            _ <- received.traverse_(_.ack)
          } yield {
            assertEquals(outcomes.size, bodies.size)
            assert(outcomes.forall(_.isRight), outcomes.toString)
            assertEquals(received.size, bodies.size)
          }
        }
    }
  }

  test("a batch the service refuses never reports a whole-batch success") {
    val destination = SqsTestSupport.queue("batch-mixed")
    val oversized = Message.of("x" * (300 * 1024))
    val messages = List(Message.of("ok"), oversized)
    SqsTestSupport.broker.use { broker =>
      broker.producer(destination).use { producer =>
        producer.sendBatch(messages).attempt.map {
          case Right(outcomes) =>
            assertEquals(outcomes.size, messages.size)
            assert(outcomes.lastOption.exists(_.isLeft), outcomes.toString)
          case Left(_) => ()
        }
      }
    }
  }
}

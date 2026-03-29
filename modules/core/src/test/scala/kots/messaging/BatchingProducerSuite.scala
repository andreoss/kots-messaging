package kots.messaging

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._
import munit.CatsEffectSuite

import scala.concurrent.duration._

/** Producer that records the batches it was asked to publish. */
final class RecordingProducer(
  batches: Ref[IO, List[Int]],
  failures: Ref[IO, Map[String, SendFailure]],
) extends Producer[IO, String] {

  def send(message: Message[String]): IO[MessageId] =
    sendBatch(List(message)).flatMap(_.head.fold(failure => IO.raiseError(SendRefused(failure)), IO.pure))

  def sendAfter(message: Message[String], delay: FiniteDuration): IO[MessageId] = send(message)

  def sendBatch(messages: List[Message[String]]): IO[List[Either[SendFailure, MessageId]]] =
    batches.update(_ :+ messages.size) *>
      messages.traverse { message =>
        failures.modify { current =>
          current.get(message.payload) match {
            case Some(failure) => (current - message.payload, Left(failure))
            case None => (current, Right(MessageId(message.payload)))
          }
        }
      }

  val recorded: IO[List[Int]] = batches.get
}

object RecordingProducer {
  def create(failures: Map[String, SendFailure] = Map.empty): IO[RecordingProducer] =
    for {
      batches <- Ref.of[IO, List[Int]](Nil)
      pending <- Ref.of[IO, Map[String, SendFailure]](failures)
    } yield new RecordingProducer(batches, pending)
}

final class BatchingProducerSuite extends CatsEffectSuite {

  private val settings =
    ProducerSettings.default.withBatchSize(5).withLinger(50.millis).withParallelism(1)

  test("callers publishing one at a time still send batches") {
    for {
      recording <- RecordingProducer.create()
      ids <- BatchingProducer.resource(recording, settings, Entropy.const[IO](1.0)).use { producer =>
        List.range(0, 5).parTraverse(index => producer.send(Message.of(s"body-$index")))
      }
      batches <- recording.recorded
    } yield {
      assertEquals(ids.map(_.value).sorted, List.range(0, 5).map(index => s"body-$index"))
      assert(batches.sum == 5, batches.toString)
      assert(batches.exists(_ > 1), s"nothing was batched: $batches")
    }
  }

  test("every caller learns its own outcome") {
    val refused = SendFailure("InvalidMessage", "refused", recoverable = false)
    for {
      recording <- RecordingProducer.create(Map("bad" -> refused))
      outcomes <- BatchingProducer.resource(recording, settings, Entropy.const[IO](1.0)).use {
        producer => producer.sendBatch(List(Message.of("good"), Message.of("bad")))
      }
    } yield {
      assertEquals(outcomes.head.map(_.value), Right("good"))
      assertEquals(outcomes.last, Left(refused))
    }
  }

  test("a single send that is refused fails its own effect") {
    val refused = SendFailure("InvalidMessage", "refused", recoverable = false)
    for {
      recording <- RecordingProducer.create(Map("bad" -> refused))
      outcome <- BatchingProducer
        .resource(recording, settings, Entropy.const[IO](1.0))
        .use(_.send(Message.of("bad")))
        .attempt
    } yield assertEquals(outcome.left.map(_.isInstanceOf[SendRefused]), Left(true))
  }

  test("a recoverable failure is retried within the budget") {
    val throttled = SendFailure("ThrottlingException", "slow down", recoverable = true)
    for {
      recording <- RecordingProducer.create(Map("body" -> throttled))
      id <- BatchingProducer
        .resource(
          recording,
          settings.withBackoff(Backoff.none).withMaxAttempts(3),
          Entropy.const[IO](1.0),
        )
        .use(_.send(Message.of("body")))
      batches <- recording.recorded
    } yield {
      assertEquals(id, MessageId("body"))
      assertEquals(batches.size, 2)
    }
  }

  test("a recoverable failure past the budget is reported") {
    val throttled = SendFailure("ThrottlingException", "slow down", recoverable = true)
    for {
      recording <- RecordingProducer.create(
        Map("body" -> throttled)
      )
      outcomes <- BatchingProducer
        .resource(
          recording,
          settings.withBackoff(Backoff.none).withMaxAttempts(1),
          Entropy.const[IO](1.0),
        )
        .use(_.sendBatch(List(Message.of("body"))))
    } yield assertEquals(outcomes, List(Left(throttled)))
  }
}

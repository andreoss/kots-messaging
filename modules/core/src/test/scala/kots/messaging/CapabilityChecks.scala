package kots.messaging

import cats.effect.IO
import cats.syntax.all._

import scala.concurrent.duration._

/** Assertions the contract runs for a capability an adapter declares. */
object CapabilityChecks {

  def receiveWithin(
    consumer: Consumer[IO, String],
    timeout: FiniteDuration,
  ): IO[Option[Delivery[IO, String]]] = {
    lazy val poll: IO[Option[Delivery[IO, String]]] =
      consumer.receive.flatMap {
        case Some(delivery) => IO.pure(Some(delivery))
        case None => IO.sleep(50.millis) *> poll
      }
    poll.timeoutTo(timeout, IO.pure(None))
  }

  def batchWithin(
    consumer: Consumer[IO, String],
    size: Int,
    timeout: FiniteDuration,
  ): IO[List[Delivery[IO, String]]] = {
    def poll(held: List[Delivery[IO, String]]): IO[List[Delivery[IO, String]]] =
      if (held.size >= size) IO.pure(held)
      else
        consumer.receiveBatch(size - held.size).flatMap {
          case Nil => IO.sleep(50.millis) *> poll(held)
          case more => poll(held ::: more)
        }
    poll(Nil).timeoutTo(timeout, IO.pure(Nil))
  }

  private def parkedWithin(
    source: Consumer[IO, String],
    parked: Consumer[IO, String],
    timeout: FiniteDuration,
  ): IO[Option[Delivery[IO, String]]] = {
    lazy val poll: IO[Option[Delivery[IO, String]]] =
      parked.receive.flatMap {
        case Some(delivery) => IO.pure(Some(delivery))
        case None =>
          source.receive.flatMap(_.traverse_(_.reject)) *> IO.sleep(200.millis) *> poll
      }
    poll.timeoutTo(timeout, IO.pure(None))
  }

  private def require(condition: Boolean, failure: String): IO[Unit] =
    IO.raiseError(new AssertionError(failure)).unlessA(condition)

  def delay(producer: Producer[IO, String], consumer: Consumer[IO, String]): IO[Unit] =
    for {
      _ <- producer.sendAfter(Message.of("delayed"), 1.second)
      early <- consumer.receive
      _ <- require(early.isEmpty, "a delayed message was visible at once")
      late <- receiveWithin(consumer, 10.seconds)
      _ <- require(late.nonEmpty, "a delayed message never became visible")
      _ <- late.traverse_(_.ack)
    } yield ()

  def leaseExtension(producer: Producer[IO, String], consumer: Consumer[IO, String]): IO[Unit] =
    for {
      _ <- producer.send(Message.of("held"))
      first <- receiveWithin(consumer, 10.seconds)
      _ <- require(first.nonEmpty, "nothing to hold")
      _ <- first.traverse_(_.extend(30.seconds))
      _ <- IO.sleep(3.seconds)
      again <- consumer.receive
      _ <- require(again.isEmpty, "an extended lease was redelivered anyway")
      _ <- first.traverse_(_.ack)
    } yield ()

  def deadLetter(
    producer: Producer[IO, String],
    consumer: Consumer[IO, String],
    parked: Consumer[IO, String],
  ): IO[Unit] =
    for {
      _ <- producer.send(Message.of("poison"))
      first <- receiveWithin(consumer, 10.seconds)
      _ <- require(first.nonEmpty, "nothing to poison")
      _ <- first.traverse_(_.reject)
      dead <- parkedWithin(consumer, parked, 30.seconds)
      _ <- require(dead.nonEmpty, "a spent message never reached the dead-letter destination")
      _ <- require(
        dead.exists(_.envelope.message.payload == "poison"),
        "the dead-letter destination held another message",
      )
      _ <- dead.traverse_(_.ack)
      stale <- consumer.receive
      _ <- require(stale.isEmpty, "a dead-lettered message was redelivered to its source")
    } yield ()
}

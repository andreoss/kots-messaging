package kots.mq.bench

import cats.effect.kernel.Temporal
import cats.effect.std.Queue
import cats.syntax.all._
import kots.mq._
import kots.mq.mem.MemBroker

import scala.concurrent.duration.FiniteDuration

/** Times the in-memory adapter against a bare effect queue. */
object Bench {

  final case class Timing(label: String, elapsed: FiniteDuration)

  private val destination = Destination("bench")

  def run[F[_]](messages: Int)(implicit F: Temporal[F]): F[List[Timing]] =
    for {
      viaQueue <- timed("queue", bareQueue[F](messages))
      viaBroker <- timed("mem", broker[F](messages))
    } yield List(viaQueue, viaBroker)

  private def timed[F[_]](label: String, work: F[Unit])(implicit F: Temporal[F]): F[Timing] =
    F.timed(work).map { case (elapsed, _) => Timing(label, elapsed) }

  private def bareQueue[F[_]](messages: Int)(implicit F: Temporal[F]): F[Unit] =
    for {
      queue <- Queue.unbounded[F, String]
      _ <- List.range(0, messages).traverse_(index => queue.offer(s"body-$index"))
      _ <- List.range(0, messages).traverse_(_ => queue.take)
    } yield ()

  private def broker[F[_]](messages: Int)(implicit F: Temporal[F]): F[Unit] =
    MemBroker.create[F, String](Entropy.const[F](1.0)).flatMap { made =>
      (
        made.producer(destination),
        made.consumer(destination, ConsumerSettings.default),
      ).tupled.use { case (producer, consumer) =>
        List.range(0, messages).traverse_(index => producer.send(Message.of(s"body-$index"))) *>
          List
            .range(0, messages)
            .traverse_(_ => consumer.receive.flatMap(_.traverse_(_.ack)))
      }
    }
}

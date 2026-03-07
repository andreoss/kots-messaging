package kots.mq.mem

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

final class MemTranscodedSuite extends QueueContract {

  def broker: Resource[IO, Broker[IO, String]] =
    Resource
      .eval(MemBroker.create[IO, Array[Byte]](Entropy.const[IO](1.0)))
      .map(Transcode.broker(_, Codec.utf8))
}

final class MemTranscodeFailureSuite extends CatsEffectSuite {

  private val destination = Destination("undecodable")

  private val ints: Codec[Int] =
    new Codec[Int] {
      def encode(value: Int): Array[Byte] = Codec.utf8.encode(value.toString)
      def decode(bytes: Array[Byte]): Either[CodecError, Int] =
        Codec.utf8.decode(bytes).flatMap(_.toIntOption.toRight(CodecError("not a base-10 int")))
    }

  test("an undecodable payload fails the receive with a parse error") {
    for {
      raw <- MemBroker.create[IO, Array[Byte]](Entropy.const[IO](1.0))
      typed = Transcode.broker(raw, ints)
      result <- (
        raw.producer(destination),
        typed.consumer(destination, ConsumerSettings.default),
      ).tupled.use { case (bytes, consumer) =>
        bytes.send(Message.of(Codec.utf8.encode("seven"))) *> consumer.receive.attempt
      }
    } yield assert(result.left.exists(_.isInstanceOf[CodecError]))
  }
}

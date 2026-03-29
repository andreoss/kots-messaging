package kots.messaging

import cats.Invariant

import java.nio.charset.StandardCharsets.UTF_8

/** Codec between a typed payload and the bytes on the wire; decode parses. */
trait Codec[A] {
  def encode(value: A): Array[Byte]
  def decode(bytes: Array[Byte]): Either[CodecError, A]
}

/** Decode failure; the description never carries payload contents. */
final case class CodecError(description: String) extends RuntimeException(description)

object Codec {

  implicit val invariant: Invariant[Codec] =
    new Invariant[Codec] {
      def imap[A, B](fa: Codec[A])(f: A => B)(g: B => A): Codec[B] =
        new Codec[B] {
          def encode(value: B): Array[Byte] = fa.encode(g(value))
          def decode(bytes: Array[Byte]): Either[CodecError, B] = fa.decode(bytes).map(f)
        }
    }

  val bytes: Codec[Array[Byte]] =
    new Codec[Array[Byte]] {
      def encode(value: Array[Byte]): Array[Byte] = value
      def decode(bytes: Array[Byte]): Either[CodecError, Array[Byte]] = Right(bytes)
    }

  val utf8: Codec[String] =
    new Codec[String] {
      def encode(value: String): Array[Byte] = value.getBytes(UTF_8)
      def decode(bytes: Array[Byte]): Either[CodecError, String] = Right(new String(bytes, UTF_8))
    }
}

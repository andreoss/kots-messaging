package kots.mq

import cats.Invariant
import cats.syntax.all._
import munit.FunSuite

final class CodecSuite extends FunSuite {

  private def roundTrip[A](codec: Codec[A], value: A): Either[CodecError, A] =
    codec.decode(codec.encode(value))

  test("the utf8 codec round-trips text") {
    assertEquals(roundTrip(Codec.utf8, "body"), Right("body"))
    assertEquals(roundTrip(Codec.utf8, ""), Right(""))
    assertEquals(roundTrip(Codec.utf8, "ключ"), Right("ключ"))
  }

  test("the byte codec round-trips its input") {
    assertEquals(roundTrip(Codec.bytes, Array[Byte](1, 2, 3)).map(_.toList), Right(List[Byte](1, 2, 3)))
  }

  test("a derived codec round-trips when its base does") {
    val ints: Codec[Int] = Invariant[Codec].imap(Codec.utf8)(_.toInt)(_.toString)
    assertEquals(roundTrip(ints, 42), Right(42))
  }

  test("a derived codec reports the base failure") {
    val ints: Codec[Int] =
      new Codec[Int] {
        def encode(value: Int): Array[Byte] = Codec.utf8.encode(value.toString)
        def decode(bytes: Array[Byte]): Either[CodecError, Int] =
          Codec.utf8.decode(bytes).flatMap(_.toIntOption.toRight(CodecError("not a base-10 int")))
      }
    assertEquals(roundTrip(ints, 7), Right(7))
    assert(ints.decode(Codec.utf8.encode("seven")).isLeft)
  }

  test("a failure description carries no input contents") {
    val error = CodecError("not a base-10 int")
    assert(!error.description.contains("seven"))
  }
}

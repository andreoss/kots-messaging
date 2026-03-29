package kots.messaging.interop

import cats.arrow.FunctionK
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.~>
import kots.messaging.mem.MemBroker
import kots.messaging.{Broker, Entropy, QueueContract}
import zio.interop.catz._
import zio.{Runtime, Task, Unsafe}

final class InteropContractSuite extends QueueContract {

  private val runtime = Runtime.default

  private val taskToIo: Task ~> IO =
    new FunctionK[Task, IO] {
      def apply[A](task: Task[A]): IO[A] =
        IO.fromFuture(IO(Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task))))
    }

  def broker: Resource[IO, Broker[IO, String]] =
    Resource.eval(
      taskToIo(
        MemBroker
          .create[Task, String](Entropy.const[Task](1.0))
          .map(MqInterop.mapK(_)(taskToIo))
      )
    )
}

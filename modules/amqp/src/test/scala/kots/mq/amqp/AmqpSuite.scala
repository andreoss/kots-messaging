package kots.mq.amqp

import cats.syntax.all._
import kots.mq.Destination
import munit.CatsEffectSuite

import scala.concurrent.duration._

/** A suite that owns the destinations it declares and removes them. */
abstract class AmqpSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  private val created = new java.util.concurrent.ConcurrentLinkedQueue[Destination]()

  protected def queue(name: String): Destination = {
    val destination = AmqpTestSupport.queue(name)
    created.add(destination)
    created.add(Destination(s"${destination.name}.delay"))
    destination
  }

  override def afterAll(): Unit = {
    val destinations = Iterator
      .continually(Option(created.poll()))
      .takeWhile(_.isDefined)
      .flatten
      .toList
    AmqpTestSupport
      .broker()
      .use(broker => destinations.traverse_(broker.admin.delete(_).attempt.void))
      .attempt
      .unsafeRunTimed(20.seconds)
    super.afterAll()
  }
}

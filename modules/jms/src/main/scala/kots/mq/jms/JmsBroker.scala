package kots.mq.jms

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Dispatcher, Mutex, Queue}
import cats.syntax.all._
import jakarta.jms.{
  BytesMessage,
  Connection,
  ConnectionFactory,
  Message => JmsMessage,
  MessageListener,
  MessageProducer,
  Session,
}
import kots.mq._

import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration.FiniteDuration

/** Jakarta Messaging adapter; one session per producer and per consumer. */
object JmsBroker {

  private[jms] val attemptProperty = "x_mq_attempt"
  private[jms] val keyProperty = "x_mq_key"
  private[jms] val headerPrefix = "x_mq_h_"
  private[jms] val priorityHeader = "x-mq-priority"

  /** Property name for a header, so any header name is a valid identifier. */
  private[jms] def propertyOf(header: String): String =
    headerPrefix + header.getBytes(UTF_8).map(byte => f"${byte & 0xff}%02x").mkString

  private[jms] def headerOf(property: String): Option[String] =
    if (!property.startsWith(headerPrefix)) None
    else {
      val hex = property.drop(headerPrefix.length)
      if (hex.length % 2 != 0) None
      else
        Some(
          new String(
            hex.grouped(2).map(pair => Integer.parseInt(pair, 16).toByte).toArray,
            UTF_8,
          )
        )
    }

  def bytes[F[_]](
    factory: ConnectionFactory,
    settings: JmsSettings,
    entropy: Entropy[F],
  )(implicit F: Async[F]): Resource[F, Broker[F, Array[Byte]]] =
    Resource
      .make(F.blocking {
        val connection = factory.createConnection()
        connection.start()
        connection
      })(connection => F.blocking(connection.close()))
      .map(new JmsBroker[F](_, settings, entropy))
}

private final class JmsBroker[F[_]](
  connection: Connection,
  settings: JmsSettings,
  entropy: Entropy[F],
)(implicit F: Async[F])
  extends Broker[F, Array[Byte]] {

  import JmsBroker._

  val capabilities: Capabilities = {
    val base = Capabilities.of(Capability.Batch, Capability.DeadLetter)
    val withDelay = if (settings.delaySupported) base.and(Capability.Delay) else base
    if (settings.prioritySupported) withDelay.and(Capability.Priority) else withDelay
  }

  val admin: Admin[F] = Admin.unknown[F]

  private def sessionResource(mode: Int): Resource[F, Session] =
    Resource.make(F.blocking(connection.createSession(false, mode)))(session =>
      F.blocking(session.close())
    )

  def producer(destination: Destination): Resource[F, Producer[F, Array[Byte]]] =
    for {
      session <- sessionResource(Session.AUTO_ACKNOWLEDGE)
      sender <- Resource.make(
        F.blocking(session.createProducer(session.createQueue(destination.name)))
      )(client => F.blocking(client.close()))
      guard <- Resource.eval(Mutex[F])
    } yield new Producer[F, Array[Byte]] {

      def send(message: Message[Array[Byte]]): F[MessageId] =
        publish(session, sender, guard, message, 1, None)

      def sendAfter(message: Message[Array[Byte]], delay: FiniteDuration): F[MessageId] =
        if (settings.delaySupported) publish(session, sender, guard, message, 1, Some(delay))
        else F.raiseError(CapabilityUnsupported(Capability.Delay))

      def sendBatch(
        messages: List[Message[Array[Byte]]]
      ): F[List[Either[SendFailure, MessageId]]] =
        messages.traverse(message => send(message).attempt.map(_.leftMap(SendFailure.of)))
    }

  def consumer(
    destination: Destination,
    consumerSettings: ConsumerSettings,
  ): Resource[F, Consumer[F, Array[Byte]]] =
    for {
      session <- sessionResource(settings.acknowledgeMode)
      reader <- Resource.make(
        F.blocking(session.createConsumer(session.createQueue(destination.name)))
      )(client => F.blocking(client.close()))
      retrySession <- sessionResource(Session.AUTO_ACKNOWLEDGE)
      guard <- Resource.eval(Mutex[F])
      retryGuard <- Resource.eval(Mutex[F])
      inflight <- Resource.eval(F.ref(Set.empty[String]))
      arrivals <- Resource.eval(Queue.bounded[F, JmsMessage](consumerSettings.prefetch max 1))
      dispatcher <- Dispatcher.parallel[F](await = false)
      open <- Resource.make(F.ref(true))(_.set(false))
      _ <- Resource.eval(
        F.blocking(
          reader.setMessageListener(new MessageListener {
            def onMessage(message: JmsMessage): Unit =
              dispatcher.unsafeRunAndForget(arrivals.offer(message))
          })
        )
      )
    } yield new Consumer[F, Array[Byte]] {

      def receive: F[Option[Delivery[F, Array[Byte]]]] = receiveBatch(1).map(_.headOption)

      def receiveBatch(max: Int): F[List[Delivery[F, Array[Byte]]]] =
        for {
          held <- inflight.get
          room = math.max(0, math.min(max, consumerSettings.prefetch - held.size))
          messages <- poll(room, Nil)
          _ <- inflight.update(_ ++ messages.map(_.getJMSMessageID))
        } yield messages.map(delivered(_, destination, consumerSettings))

      def ackAll(deliveries: List[Delivery[F, Array[Byte]]]): F[Unit] =
        deliveries.traverse_(_.ack)

      def extendAll(deliveries: List[Delivery[F, Array[Byte]]], by: FiniteDuration): F[Unit] =
        F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))

      private def poll(room: Int, taken: List[JmsMessage]): F[List[JmsMessage]] =
        if (taken.size >= room) F.pure(taken)
        else if (taken.isEmpty)
          F.timeoutTo(
            arrivals.take.map(Option(_)),
            settings.receiveTimeout,
            F.pure(Option.empty[JmsMessage]),
          ).flatMap {
            case Some(message) => poll(room, taken :+ message)
            case None =>
              open.get.ifM(F.pure(taken), F.raiseError(JmsConsumerClosed(destination.name)))
          }
        else
          arrivals.tryTake.flatMap {
            case Some(message) => poll(room, taken :+ message)
            case None => F.pure(taken)
          }

      private def delivered(
        message: JmsMessage,
        destination: Destination,
        consumerSettings: ConsumerSettings,
      ): Delivery[F, Array[Byte]] =
        new Delivery[F, Array[Byte]] {

          val envelope: Envelope[Array[Byte]] = envelopeOf(message)

          val ack: F[Unit] =
            F.blocking(message.acknowledge()) *> inflight.update(_ - message.getJMSMessageID)

          val reject: F[Unit] =
            for {
              sample <- entropy.nextDouble
              _ <-
                if (envelope.attempt >= consumerSettings.maxAttempts)
                  consumerSettings.deadLetter.traverse_(parked =>
                    republish(parked, envelope.message, envelope.attempt, None)
                  )
                else
                  republish(
                    destination,
                    envelope.message,
                    envelope.attempt + 1,
                    Some(consumerSettings.backoff.delay(envelope.attempt, sample)),
                  )
              _ <- ack
            } yield ()

          val release: F[Unit] =
            republish(destination, envelope.message, envelope.attempt, None) *> ack

          val deadLetter: F[Unit] =
            consumerSettings.deadLetter.traverse_(parked =>
              republish(parked, envelope.message, envelope.attempt, None)
            ) *> ack

          def extend(by: FiniteDuration): F[Unit] =
            F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))
        }

      private def republish(
        destination: Destination,
        message: Message[Array[Byte]],
        attempt: Int,
        delay: Option[FiniteDuration],
      ): F[Unit] =
        Resource
          .make(
            F.blocking(retrySession.createProducer(retrySession.createQueue(destination.name)))
          )(sender => F.blocking(sender.close()))
          .use(sender => publish(retrySession, sender, retryGuard, message, attempt, delay).void)
    }

  private def envelopeOf(message: JmsMessage): Envelope[Array[Byte]] = {
    val names = message.getPropertyNames
    val properties = Iterator
      .continually(if (names.hasMoreElements) Some(names.nextElement().toString) else None)
      .takeWhile(_.isDefined)
      .flatten
      .map(name => name -> message.getStringProperty(name))
      .filter { case (_, value) => value != null }
      .toMap
    val payload = message match {
      case bytes: BytesMessage =>
        val body = new Array[Byte](bytes.getBodyLength.toInt)
        bytes.readBytes(body)
        body
      case _ => Array.emptyByteArray
    }
    val attempt = properties.get(attemptProperty).flatMap(_.toIntOption).getOrElse(1)
    val headers = properties.toList.flatMap { case (property, value) =>
      headerOf(property).map(_ -> value)
    }.toMap
    Envelope(
      MessageId(message.getJMSMessageID),
      Message(payload, headers, properties.get(keyProperty).map(MessageKey.apply)),
      attempt,
    )
  }

  private def publish(
    session: Session,
    sender: MessageProducer,
    guard: Mutex[F],
    message: Message[Array[Byte]],
    attempt: Int,
    delay: Option[FiniteDuration],
  ): F[MessageId] =
    guard.lock.surround(
      F.blocking {
        val body = session.createBytesMessage()
        body.writeBytes(message.payload)
        message.headers.foreach { case (name, value) =>
          body.setStringProperty(propertyOf(name), value)
        }
        message.key.foreach(key => body.setStringProperty(keyProperty, key.value))
        body.setStringProperty(attemptProperty, attempt.toString)
        message.headers.get(priorityHeader).flatMap(_.toIntOption).foreach(sender.setPriority)
        if (settings.delaySupported)
          sender.setDeliveryDelay(delay.fold(0L)(_.toMillis.max(0L)))
        sender.send(body)
        MessageId(body.getJMSMessageID)
      }
    )

}

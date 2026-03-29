package kots.messaging.amqp

import cats.effect.kernel.{Async, Deferred, Ref, Resource}
import cats.effect.std.{Dispatcher, Mutex, Queue}
import cats.syntax.all._
import com.rabbitmq.client.{
  AMQP,
  Channel,
  BlockedListener,
  ConfirmListener,
  Connection,
  ConnectionFactory,
  DefaultConsumer,
  ReturnListener,
}
import kots.messaging._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** AMQP 0-9-1 adapter over the broker's own acknowledgement and routing. */
object AmqpBroker {

  private[amqp] val attemptHeader = "x-mq-attempt"
  private[amqp] val keyHeader = "x-mq-key"
  private[amqp] val deadLetterExchange = "x-dead-letter-exchange"
  private[amqp] val deadLetterRoutingKey = "x-dead-letter-routing-key"

  private[amqp] final case class Pushed(
    tag: Long,
    redelivered: Boolean,
    properties: AMQP.BasicProperties,
    body: Array[Byte],
  )

  def bytes[F[_]](settings: AmqpSettings, entropy: Entropy[F])(implicit
    F: Async[F]
  ): Resource[F, Broker[F, Array[Byte]]] =
    for {
      blocked <- Resource.eval(F.delay(new AtomicReference[Option[String]](None)))
      connection <- Resource.make(connect[F](settings))(open => F.blocking(open.close()))
      _ <- Resource.eval(
        F.blocking(
          connection.addBlockedListener(new BlockedListener {
            def handleBlocked(reason: String): Unit = blocked.set(Some(reason))
            def handleUnblocked(): Unit = blocked.set(None)
          })
        )
      )
    } yield new AmqpBroker[F](connection, settings, entropy, blocked)

  /** Tries each endpoint in order; the first that answers is the one used. */
  private def connect[F[_]](settings: AmqpSettings)(implicit F: Async[F]): F[Connection] = {
    def attempt(remaining: List[String], last: Option[Throwable]): F[Connection] =
      remaining match {
        case Nil =>
          F.raiseError(
            last.getOrElse(new IllegalArgumentException("no endpoint was given to connect to"))
          )
        case uri :: rest =>
          F.blocking {
            val factory = new ConnectionFactory()
            factory.setUri(uri)
            settings.connectionName.fold(factory.newConnection())(factory.newConnection)
          }.handleErrorWith(error => attempt(rest, Some(error)))
      }
    attempt(settings.uris, None)
  }

  private[amqp] def queueArguments(deadLetter: Option[Destination]): Map[String, AnyRef] =
    deadLetter.fold(Map.empty[String, AnyRef])(parked =>
      Map(deadLetterExchange -> "", deadLetterRoutingKey -> parked.name)
    )
}

private final class AmqpBroker[F[_]](
  connection: Connection,
  settings: AmqpSettings,
  entropy: Entropy[F],
  blockedState: AtomicReference[Option[String]],
)(implicit F: Async[F])
  extends Broker[F, Array[Byte]] {

  import AmqpBroker._

  val capabilities: Capabilities =
    Capabilities.of(
      Capability.Batch,
      Capability.Delay,
      Capability.DeadLetter,
      Capability.Topology,
      Capability.Expiry,
    )

  val events: BrokerEvents[F] = new BrokerEvents[F] {
    val blocked: F[Option[String]] = F.delay(blockedState.get)
  }

  val admin: Admin[F] = new Admin[F] {

    def depth(destination: Destination): F[Option[Long]] =
      channelResource
        .use(channel =>
          F.blocking(Option(channel.queueDeclarePassive(destination.name).getMessageCount.toLong))
        )
        .recover { case _: Throwable => None }

    def declare(destination: Destination): F[Unit] =
      channelResource.use(channel =>
        F.blocking(
          channel.queueDeclare(destination.name, true, false, false, Map.empty[String, AnyRef].asJava)
        ).void
      )

    def purge(destination: Destination): F[Option[Long]] =
      channelResource.use(channel =>
        F.blocking(Option(channel.queuePurge(destination.name).getMessageCount.toLong))
      )

    def delete(destination: Destination): F[Unit] =
      channelResource.use(channel => F.blocking(channel.queueDelete(destination.name)).void)
  }

  private def channelResource: Resource[F, Channel] =
    Resource.make(F.blocking(connection.createChannel()))(channel => F.blocking(channel.close()))

  private final class Confirming(
    val channel: Channel,
    val guard: Mutex[F],
    val returned: AtomicReference[Option[AmqpPublishFailed]],
    val pending: Ref[F, Map[Long, Deferred[F, Either[String, Unit]]]],
  ) {

    def settle(tag: Long, multiple: Boolean, outcome: Either[String, Unit]): F[Unit] =
      pending
        .modify { current =>
          val (settled, rest) = current.partition { case (sequence, _) =>
            if (multiple) sequence <= tag else sequence == tag
          }
          (rest, settled.values.toList)
        }
        .flatMap(_.traverse_(_.complete(outcome).void))
  }

  /** A channel whose publishes are confirmed by the broker as they land. */
  private def confirming(channel: Channel): Resource[F, Confirming] =
    for {
      _ <- Resource.eval(F.blocking(channel.confirmSelect()))
      dispatcher <- Dispatcher.parallel[F](await = false)
      guard <- Resource.eval(Mutex[F])
      returned <- Resource.eval(F.delay(new AtomicReference[Option[AmqpPublishFailed]](None)))
      pending <- Resource.eval(F.ref(Map.empty[Long, Deferred[F, Either[String, Unit]]]))
      publisher = new Confirming(channel, guard, returned, pending)
      _ <- Resource.eval(
        F.blocking {
          channel.addConfirmListener(new ConfirmListener {
            def handleAck(tag: Long, multiple: Boolean): Unit =
              dispatcher.unsafeRunAndForget(publisher.settle(tag, multiple, Right(())))

            def handleNack(tag: Long, multiple: Boolean): Unit =
              dispatcher.unsafeRunAndForget(
                publisher.settle(tag, multiple, Left("the broker refused the publish"))
              )
          })
          channel.addReturnListener(new ReturnListener {
            def handleReturn(
              replyCode: Int,
              replyText: String,
              exchange: String,
              routingKey: String,
              properties: AMQP.BasicProperties,
              body: Array[Byte],
            ): Unit =
              returned.set(Some(AmqpPublishFailed.returned(replyCode, replyText, routingKey)))
          })
        }
      )
    } yield publisher

  def producer(destination: Destination): Resource[F, Producer[F, Array[Byte]]] =
    for {
      channel <- channelResource
      publisher <- confirming(channel)
      declared <- Resource.eval(F.ref(Set.empty[String]))
    } yield new Producer[F, Array[Byte]] {

      def send(message: Message[Array[Byte]]): F[MessageId] =
        publish(publisher, destination.name, message, 1, None)

      def sendBatch(
        messages: List[Message[Array[Byte]]]
      ): F[List[Either[SendFailure, MessageId]]] =
        messages.traverse(message => send(message).attempt.map(_.leftMap(SendFailure.of)))

      def sendAfter(message: Message[Array[Byte]], delay: FiniteDuration): F[MessageId] = {
        val holding = s"${destination.name}.delay"
        for {
          known <- declared.get
          _ <- F
            .blocking(
              channel.queueDeclare(
                holding,
                true,
                false,
                false,
                Map[String, AnyRef](
                  deadLetterExchange -> "",
                  deadLetterRoutingKey -> destination.name,
                ).asJava,
              )
            )
            .void
            .unlessA(known.contains(holding))
          _ <- declared.update(_ + holding)
          id <- publish(publisher, holding, message, 1, Some(delay.toMillis.max(0L).toString))
        } yield id
      }
    }

  def consumer(
    destination: Destination,
    consumerSettings: ConsumerSettings,
  ): Resource[F, Consumer[F, Array[Byte]]] =
    for {
      channel <- channelResource
      publisher <- confirming(channel)
      _ <- Resource.eval(F.blocking(channel.basicQos(consumerSettings.prefetch)))
      _ <- Resource.eval(
        consumerSettings.deadLetter.traverse_(parked =>
          F.blocking(
            channel.queueDeclare(
              parked.name,
              true,
              false,
              false,
              Map.empty[String, AnyRef].asJava,
            )
          ).void
        )
      )
      _ <- Resource.eval(
        F.blocking(
          channel.queueDeclare(
            destination.name,
            true,
            false,
            false,
            queueArguments(consumerSettings.deadLetter).asJava,
          )
        ).void
      )
      inflight <- Resource.eval(F.ref(Set.empty[Long]))
      arrivals <- Resource.eval(Queue.bounded[F, Pushed](consumerSettings.prefetch max 1))
      dispatcher <- Dispatcher.sequential[F](await = false)
      _ <- Resource.make(
        F.blocking(
          channel.basicConsume(
            destination.name,
            false,
            new DefaultConsumer(channel) {
              override def handleDelivery(
                consumerTag: String,
                envelope: com.rabbitmq.client.Envelope,
                properties: AMQP.BasicProperties,
                body: Array[Byte],
              ): Unit =
                dispatcher.unsafeRunAndForget(
                  arrivals.offer(
                    Pushed(envelope.getDeliveryTag, envelope.isRedeliver, properties, body)
                  )
                )
            },
          )
        )
      )(tag => F.blocking(channel.basicCancel(tag)).attempt.void)
    } yield new Consumer[F, Array[Byte]] {

      private val guard: Mutex[F] = publisher.guard

      def receive: F[Option[Delivery[F, Array[Byte]]]] = receiveBatch(1).map(_.headOption)

      def receiveBatch(max: Int): F[List[Delivery[F, Array[Byte]]]] =
        for {
          held <- inflight.get
          room = math.max(0, math.min(max, consumerSettings.prefetch - held.size))
          taken <- fetch(room, Nil)
          _ <- inflight.update(_ ++ taken.map(_.tag))
        } yield taken.map(delivered)

      def ackAll(deliveries: List[Delivery[F, Array[Byte]]]): F[Unit] =
        deliveries.traverse_(_.ack)

      def extendAll(deliveries: List[Delivery[F, Array[Byte]]], by: FiniteDuration): F[Unit] =
        F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))

      private def fetch(room: Int, taken: List[Pushed]): F[List[Pushed]] =
        if (taken.size >= room) F.pure(taken)
        else if (taken.isEmpty)
          F.timeoutTo(
            arrivals.take.map(Option(_)),
            settings.receiveTimeout,
            F.pure(Option.empty[Pushed]),
          ).flatMap {
            case Some(pushed) => fetch(room, taken :+ pushed)
            case None =>
              F.blocking(channel.isOpen)
                .ifM(F.pure(taken), F.raiseError(AmqpChannelClosed(destination.name)))
          }
        else
          arrivals.tryTake.flatMap {
            case Some(pushed) => fetch(room, taken :+ pushed)
            case None => F.pure(taken)
          }

      private def delivered(pushed: Pushed): Delivery[F, Array[Byte]] =
        new Delivery[F, Array[Byte]] {

          private val tag: Long = pushed.tag

          val envelope: Envelope[Array[Byte]] = envelopeOf(pushed)

          val ack: F[Unit] =
            guard.lock.surround(
              F.blocking(channel.basicAck(tag, false)) *> inflight.update(_ - tag)
            )

          val reject: F[Unit] =
            if (envelope.attempt >= consumerSettings.maxAttempts)
              guard.lock.surround(
                F.blocking(channel.basicReject(tag, false)) *> inflight.update(_ - tag)
              )
            else
              for {
                sample <- entropy.nextDouble
                _ <- F.sleep(consumerSettings.backoff.delay(envelope.attempt, sample))
                _ <- publish(publisher, destination.name, envelope.message, envelope.attempt + 1, None)
                _ <- guard.lock.surround(
                  F.blocking(channel.basicAck(tag, false)) *> inflight.update(_ - tag)
                )
              } yield ()

          val release: F[Unit] =
            guard.lock.surround(
              F.blocking(channel.basicNack(tag, false, true)) *> inflight.update(_ - tag)
            )

          val deadLetter: F[Unit] =
            guard.lock.surround(
              F.blocking(channel.basicReject(tag, false)) *> inflight.update(_ - tag)
            )

          def extend(by: FiniteDuration): F[Unit] =
            F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))
        }
    }
  private def envelopeOf(pushed: Pushed): Envelope[Array[Byte]] = {
    val headers = Option(pushed.properties.getHeaders)
      .fold(Map.empty[String, String])(
        _.asScala.view.map { case (name, value) => name -> value.toString }.toMap
      )
    val attempt = headers.get(attemptHeader).flatMap(_.toIntOption).getOrElse(1)
    val id = Option(pushed.properties.getMessageId).getOrElse(pushed.tag.toString)
    val carried = MessageProperties(
      contentType = Option(pushed.properties.getContentType),
      correlationId = Option(pushed.properties.getCorrelationId),
      replyTo = Option(pushed.properties.getReplyTo).map(Destination.apply),
      priority = Option(pushed.properties.getPriority).map(_.intValue),
      persistent = Option(pushed.properties.getDeliveryMode).forall(_.intValue == 2),
      expiry = Option(pushed.properties.getExpiration).flatMap(_.toLongOption).map(_.millis),
    )
    Envelope(
      MessageId(id),
      Message(
        pushed.body,
        headers - attemptHeader - keyHeader,
        headers.get(keyHeader).map(MessageKey.apply),
        carried,
      ),
      attempt,
      pushed.redelivered,
    )
  }

  private def unroutable(
    returned: AtomicReference[Option[AmqpPublishFailed]]
  ): F[Option[AmqpPublishFailed]] =
    if (!settings.mandatory) F.pure(None)
    else {
      def poll(left: Int): F[Option[AmqpPublishFailed]] =
        F.delay(returned.get).flatMap {
          case found @ Some(_) => F.pure(found)
          case None if left > 0 => F.sleep(100.millis) *> poll(left - 1)
          case None => F.pure(None)
        }
      poll(5)
    }

  private def publish(
    publisher: Confirming,
    routingKey: String,
    message: Message[Array[Byte]],
    attempt: Int,
    expiration: Option[String],
  ): F[MessageId] =
    F.delay(java.util.UUID.randomUUID().toString).flatMap { id =>
      val headers: Map[String, AnyRef] =
        message.headers.map { case (name, value) => name -> (value: AnyRef) } ++
          message.key.map(key => keyHeader -> (key.value: AnyRef)).toMap +
          (attemptHeader -> (attempt.toString: AnyRef))
      val carried = message.properties
      val builder = new AMQP.BasicProperties.Builder()
        .messageId(id)
        .headers(headers.asJava)
        .deliveryMode(if (carried.persistent) 2 else 1)
      val described = carried.contentType.fold(builder)(builder.contentType)
      val correlated = carried.correlationId.fold(described)(described.correlationId)
      val replied = carried.replyTo.fold(correlated)(destination => correlated.replyTo(destination.name))
      val prioritised =
        carried.priority.fold(replied)(value => replied.priority(Integer.valueOf(value)))
      val lifetime = expiration.orElse(carried.expiry.map(_.toMillis.max(0L).toString))
      val properties = lifetime.fold(prioritised)(prioritised.expiration).build()

      val send: F[Deferred[F, Either[String, Unit]]] =
        F.deferred[Either[String, Unit]].flatTap { confirmation =>
          for {
            _ <- F.delay(publisher.returned.set(None))
            sequence <- F.blocking(publisher.channel.getNextPublishSeqNo)
            _ <- publisher.pending.update(_ + (sequence -> confirmation))
            _ <- F.blocking(
              publisher.channel
                .basicPublish("", routingKey, settings.mandatory, properties, message.payload)
            )
          } yield ()
        }

      val awaited: F[MessageId] =
        for {
          refusal <- F.delay(blockedState.get)
          _ <- refusal.traverse_(reason =>
            F.raiseError[Unit](
              AmqpPublishFailed.Refused(s"the broker is blocking publishes: $reason")
            )
          )
          confirmation <- if (settings.mandatory) send else publisher.guard.lock.surround(send)
          outcome <- F.timeoutTo(
            confirmation.get,
            settings.confirmTimeout,
            F.pure(Left("the broker did not confirm the publish"): Either[String, Unit]),
          )
          _ <- outcome.fold(
            reason => F.raiseError[Unit](AmqpPublishFailed.NotConfirmed(reason)),
            _ => F.unit,
          )
          failure <- unroutable(publisher.returned)
          _ <- failure.traverse_(F.raiseError[Unit](_))
        } yield MessageId(id)

      val attempted =
        if (settings.mandatory) publisher.guard.lock.surround(awaited) else awaited

      F.timeoutTo(
        attempted,
        settings.confirmTimeout * 2,
        F.raiseError[MessageId](
          AmqpPublishFailed.NotConfirmed("the broker did not take the publish in time")
        ),
      )
    }
}

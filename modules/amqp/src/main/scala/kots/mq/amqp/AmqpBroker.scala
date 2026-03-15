package kots.mq.amqp

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Mutex
import cats.syntax.all._
import com.rabbitmq.client.{AMQP, Channel, Connection, ConnectionFactory, GetResponse, ReturnListener}
import kots.mq._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** AMQP 0-9-1 adapter over the broker's own acknowledgement and routing. */
object AmqpBroker {

  private[amqp] val attemptHeader = "x-mq-attempt"
  private[amqp] val keyHeader = "x-mq-key"
  private[amqp] val deadLetterExchange = "x-dead-letter-exchange"
  private[amqp] val deadLetterRoutingKey = "x-dead-letter-routing-key"

  def bytes[F[_]](settings: AmqpSettings, entropy: Entropy[F])(implicit
    F: Async[F]
  ): Resource[F, Broker[F, Array[Byte]]] =
    Resource
      .make(F.blocking {
        val factory = new ConnectionFactory()
        factory.setUri(settings.uri)
        factory.newConnection()
      })(connection => F.blocking(connection.close()))
      .map(new AmqpBroker[F](_, settings, entropy))

  private[amqp] def queueArguments(deadLetter: Option[Destination]): Map[String, AnyRef] =
    deadLetter.fold(Map.empty[String, AnyRef])(parked =>
      Map(deadLetterExchange -> "", deadLetterRoutingKey -> parked.name)
    )
}

private final class AmqpBroker[F[_]](
  connection: Connection,
  settings: AmqpSettings,
  entropy: Entropy[F],
)(implicit F: Async[F])
  extends Broker[F, Array[Byte]] {

  import AmqpBroker._

  val capabilities: Capabilities =
    Capabilities.of(Capability.Batch, Capability.Delay, Capability.DeadLetter)

  val admin: Admin[F] = new Admin[F] {
    def depth(destination: Destination): F[Option[Long]] =
      channelResource
        .use(channel =>
          F.blocking(Option(channel.queueDeclarePassive(destination.name).getMessageCount.toLong))
        )
        .recover { case _: Throwable => None }
  }

  private def channelResource: Resource[F, Channel] =
    Resource.make(F.blocking(connection.createChannel()))(channel => F.blocking(channel.close()))

  def producer(destination: Destination): Resource[F, Producer[F, Array[Byte]]] =
    for {
      channel <- channelResource
      _ <- Resource.eval(F.blocking(channel.confirmSelect()))
      returned <- Resource.eval(F.delay(new AtomicReference[Option[String]](None)))
      _ <- Resource.eval(
        F.blocking(
          channel.addReturnListener(new ReturnListener {
            def handleReturn(
              replyCode: Int,
              replyText: String,
              exchange: String,
              routingKey: String,
              properties: AMQP.BasicProperties,
              body: Array[Byte],
            ): Unit = returned.set(Some(s"unroutable: $replyText"))
          })
        )
      )
      guard <- Resource.eval(Mutex[F])
      declared <- Resource.eval(F.ref(Set.empty[String]))
    } yield new Producer[F, Array[Byte]] {

      def send(message: Message[Array[Byte]]): F[MessageId] =
        publish(channel, guard, returned, destination.name, message, 1, None)

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
          id <- publish(
            channel,
            guard,
            returned,
            holding,
            message,
            1,
            Some(delay.toMillis.max(0L).toString),
          )
        } yield id
      }
    }

  def consumer(
    destination: Destination,
    consumerSettings: ConsumerSettings,
  ): Resource[F, Consumer[F, Array[Byte]]] =
    for {
      channel <- channelResource
      _ <- Resource.eval(F.blocking(channel.confirmSelect()))
      _ <- Resource.eval(
        F.blocking(channel.basicQos(consumerSettings.prefetch))
      )
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
      guard <- Resource.eval(Mutex[F])
      returned <- Resource.eval(F.delay(new AtomicReference[Option[String]](None)))
      inflight <- Resource.eval(F.ref(Set.empty[Long]))
    } yield new Consumer[F, Array[Byte]] {

      def receive: F[Option[Delivery[F, Array[Byte]]]] = receiveBatch(1).map(_.headOption)

      def receiveBatch(max: Int): F[List[Delivery[F, Array[Byte]]]] =
        guard.lock.surround {
          for {
            held <- inflight.get
            room = math.max(0, math.min(max, consumerSettings.prefetch - held.size))
            responses <- fetch(room, Nil)
            _ <- inflight.update(_ ++ responses.map(_.getEnvelope.getDeliveryTag))
          } yield responses.map(delivered)
        }

      private def fetch(room: Int, taken: List[GetResponse]): F[List[GetResponse]] =
        if (taken.size >= room) F.pure(taken)
        else
          F.blocking(Option(channel.basicGet(destination.name, false))).flatMap {
            case Some(response) => fetch(room, taken :+ response)
            case None => F.pure(taken)
          }

      private def delivered(response: GetResponse): Delivery[F, Array[Byte]] =
        new Delivery[F, Array[Byte]] {

          private val tag: Long = response.getEnvelope.getDeliveryTag

          val envelope: Envelope[Array[Byte]] = envelopeOf(response)

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
                _ <- publish(
                  channel,
                  guard,
                  returned,
                  destination.name,
                  envelope.message,
                  envelope.attempt + 1,
                  None,
                )
                _ <- guard.lock.surround(
                  F.blocking(channel.basicAck(tag, false)) *> inflight.update(_ - tag)
                )
              } yield ()

          def extend(by: FiniteDuration): F[Unit] =
            F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))
        }
    }

  private def envelopeOf(response: GetResponse): Envelope[Array[Byte]] = {
    val properties = response.getProps
    val headers = Option(properties.getHeaders)
      .fold(Map.empty[String, String])(
        _.asScala.view.map { case (name, value) => name -> value.toString }.toMap
      )
    val attempt = headers.get(attemptHeader).flatMap(_.toIntOption).getOrElse(1)
    val id = Option(properties.getMessageId)
      .getOrElse(response.getEnvelope.getDeliveryTag.toString)
    Envelope(
      MessageId(id),
      Message(
        response.getBody,
        headers - attemptHeader - keyHeader,
        headers.get(keyHeader).map(MessageKey.apply),
      ),
      attempt,
    )
  }

  private def unroutable(returned: AtomicReference[Option[String]]): F[Option[String]] =
    if (!settings.mandatory) F.pure(None)
    else {
      def poll(left: Int): F[Option[String]] =
        F.delay(returned.get).flatMap {
          case found @ Some(_) => F.pure(found)
          case None if left > 0 => F.sleep(100.millis) *> poll(left - 1)
          case None => F.pure(None)
        }
      poll(5)
    }

  private def publish(
    channel: Channel,
    guard: Mutex[F],
    returned: AtomicReference[Option[String]],
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
      val builder = new AMQP.BasicProperties.Builder().messageId(id).headers(headers.asJava)
      val properties = expiration.fold(builder)(builder.expiration).build()
      guard.lock.surround(
        for {
          _ <- F.delay(returned.set(None))
          _ <- F.blocking(
            channel.basicPublish("", routingKey, settings.mandatory, properties, message.payload)
          )
          confirmed <- F.blocking(channel.waitForConfirms(settings.confirmTimeout.toMillis))
          _ <- F
            .raiseError[Unit](AmqpPublishFailed("the broker did not confirm the publish"))
            .unlessA(confirmed)
          failure <- unroutable(returned)
          _ <- failure.traverse_(reason => F.raiseError[Unit](AmqpPublishFailed(reason)))
        } yield MessageId(id)
      )
    }
}

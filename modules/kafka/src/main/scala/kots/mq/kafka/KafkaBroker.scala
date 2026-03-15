package kots.mq.kafka

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.all._
import kots.mq._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration => JavaDuration}
import java.util.{Collections, Properties}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** Kafka adapter: acknowledgement is an offset commit, retry a republish. */
object KafkaBroker {

  private[kafka] val attemptHeader = "x-mq-attempt"

  def bytes[F[_]](settings: KafkaSettings, entropy: Entropy[F])(implicit
    F: Async[F]
  ): Resource[F, Broker[F, Array[Byte]]] =
    Resource
      .make(F.blocking(new KafkaProducer[Array[Byte], Array[Byte]](producerProperties(settings))))(
        client => F.blocking(client.close())
      )
      .map(new KafkaBroker[F](_, settings, entropy))

  private def producerProperties(settings: KafkaSettings): Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[ByteArraySerializer].getName,
    )
    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[ByteArraySerializer].getName,
    )
    properties
  }

  private[kafka] def consumerProperties(
    settings: KafkaSettings,
    destination: Destination,
    consumer: ConsumerSettings,
  ): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, s"${settings.groupPrefix}-${destination.name}")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.valueOf(consumer.prefetch))
    properties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[ByteArrayDeserializer].getName,
    )
    properties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[ByteArrayDeserializer].getName,
    )
    properties
  }

  private[kafka] final case class Offsets(inflight: Set[Long], settled: Option[Long]) {

    def taken(offset: Long): Offsets = copy(inflight = inflight + offset)

    def settle(offset: Long): Offsets =
      Offsets(inflight - offset, Some(settled.fold(offset)(_ max offset)))

    def position: Option[Long] =
      if (inflight.nonEmpty) Some(inflight.min) else settled.map(_ + 1L)
  }

  private[kafka] object Offsets {
    val empty: Offsets = Offsets(Set.empty, None)
  }
}

private final class KafkaBroker[F[_]](
  client: KafkaProducer[Array[Byte], Array[Byte]],
  settings: KafkaSettings,
  entropy: Entropy[F],
)(implicit F: Async[F])
  extends Broker[F, Array[Byte]] {

  import KafkaBroker._

  val capabilities: Capabilities =
    Capabilities.of(Capability.Batch, Capability.DeadLetter, Capability.OrderingGroup)

  val admin: Admin[F] = new Admin[F] {
    def depth(destination: Destination): F[Option[Long]] =
      Resource
        .make(
          F.blocking(
            new KafkaConsumer[Array[Byte], Array[Byte]](
              consumerProperties(settings, destination, ConsumerSettings.default)
            )
          )
        )(reader => F.blocking(reader.close()))
        .use { reader =>
          F.blocking {
            val partitions = Option(reader.partitionsFor(destination.name))
              .map(_.asScala.toList)
              .getOrElse(Nil)
              .map(info => new TopicPartition(info.topic, info.partition))
            if (partitions.isEmpty) None
            else {
              val ends = reader.endOffsets(partitions.asJava).asScala
              val committed = reader.committed(partitions.toSet.asJava).asScala
              Some(partitions.map { partition =>
                val end = ends.get(partition).map(_.longValue).getOrElse(0L)
                val position =
                  committed.get(partition).flatMap(Option(_)).map(_.offset).getOrElse(0L)
                math.max(0L, end - position)
              }.sum)
            }
          }
        }
  }

  def producer(destination: Destination): Resource[F, Producer[F, Array[Byte]]] =
    Resource.pure(new Producer[F, Array[Byte]] {

      def send(message: Message[Array[Byte]]): F[MessageId] = publish(destination, message, 1)

      def sendAfter(message: Message[Array[Byte]], delay: FiniteDuration): F[MessageId] =
        F.raiseError(CapabilityUnsupported(Capability.Delay))

      def sendBatch(
        messages: List[Message[Array[Byte]]]
      ): F[List[Either[SendFailure, MessageId]]] =
        messages.traverse(message => send(message).attempt.map(_.leftMap(SendFailure.of)))
    })

  def consumer(
    destination: Destination,
    consumerSettings: ConsumerSettings,
  ): Resource[F, Consumer[F, Array[Byte]]] =
    for {
      reader <- Resource.make(
        F.blocking(
          new KafkaConsumer[Array[Byte], Array[Byte]](
            consumerProperties(settings, destination, consumerSettings)
          )
        )
      )(closing => F.blocking(closing.close()))
      _ <- Resource.eval(F.blocking(reader.subscribe(Collections.singletonList(destination.name))))
      guard <- Resource.eval(Mutex[F])
      buffered <- Resource.eval(
        F.ref(Vector.empty[ConsumerRecord[Array[Byte], Array[Byte]]])
      )
      tracked <- Resource.eval(F.ref(Map.empty[TopicPartition, Offsets]))
    } yield new Consumer[F, Array[Byte]] {

      def receive: F[Option[Delivery[F, Array[Byte]]]] = receiveBatch(1).map(_.headOption)

      def receiveBatch(max: Int): F[List[Delivery[F, Array[Byte]]]] =
        guard.lock.surround {
          for {
            offsets <- tracked.get
            held = offsets.values.map(_.inflight.size).sum
            room = math.max(0, math.min(max, consumerSettings.prefetch - held))
            records <- if (room <= 0) F.pure(List.empty[ConsumerRecord[Array[Byte], Array[Byte]]])
            else fetch(room)
            _ <- tracked.update(current =>
              records.foldLeft(current) { (offsets, record) =>
                val partition = partitionOf(record)
                offsets.updated(
                  partition,
                  offsets.getOrElse(partition, Offsets.empty).taken(record.offset),
                )
              }
            )
          } yield records.map(delivered(_, destination, consumerSettings))
        }

      private def fetch(room: Int): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
        for {
          _ <- buffered.get.flatMap(current => poll.whenA(current.isEmpty))
          taken <- buffered.modify(current => (current.drop(room), current.take(room).toList))
        } yield taken

      private val poll: F[Unit] =
        F.blocking(reader.poll(JavaDuration.ofMillis(settings.pollTimeout.toMillis)))
          .flatMap(records => buffered.update(_ ++ records.asScala.toVector))

      private def delivered(
        record: ConsumerRecord[Array[Byte], Array[Byte]],
        destination: Destination,
        consumerSettings: ConsumerSettings,
      ): Delivery[F, Array[Byte]] =
        new Delivery[F, Array[Byte]] {

          val envelope: Envelope[Array[Byte]] = envelopeOf(record)

          val ack: F[Unit] = guard.lock.surround(commit(record))

          val reject: F[Unit] =
            for {
              sample <- entropy.nextDouble
              _ <-
                if (envelope.attempt >= consumerSettings.maxAttempts)
                  consumerSettings.deadLetter.traverse_(parked =>
                    publish(parked, envelope.message, envelope.attempt)
                  )
                else
                  F.sleep(consumerSettings.backoff.delay(envelope.attempt, sample)) *>
                    publish(destination, envelope.message, envelope.attempt + 1).void
              _ <- guard.lock.surround(commit(record))
            } yield ()

          def extend(by: FiniteDuration): F[Unit] =
            F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))
        }

      private def commit(record: ConsumerRecord[Array[Byte], Array[Byte]]): F[Unit] =
        for {
          offsets <- tracked.updateAndGet { current =>
            val partition = partitionOf(record)
            current.updated(
              partition,
              current.getOrElse(partition, Offsets.empty).settle(record.offset),
            )
          }
          partition = partitionOf(record)
          _ <- offsets
            .get(partition)
            .flatMap(_.position)
            .traverse_(position =>
              F.blocking(
                reader.commitSync(
                  Collections.singletonMap(partition, new OffsetAndMetadata(position))
                )
              )
            )
        } yield ()
    }

  private def partitionOf(record: ConsumerRecord[Array[Byte], Array[Byte]]): TopicPartition =
    new TopicPartition(record.topic, record.partition)

  private def envelopeOf(record: ConsumerRecord[Array[Byte], Array[Byte]]): Envelope[Array[Byte]] = {
    val headers = record
      .headers()
      .asScala
      .map(header => header.key -> new String(header.value, UTF_8))
      .toMap
    val attempt = headers.get(attemptHeader).flatMap(_.toIntOption).getOrElse(1)
    Envelope(
      MessageId(s"${record.topic}-${record.partition}-${record.offset}"),
      Message(
        record.value,
        headers - attemptHeader,
        Option(record.key).map(key => MessageKey(new String(key, UTF_8))),
      ),
      attempt,
    )
  }

  private def publish(
    destination: Destination,
    message: Message[Array[Byte]],
    attempt: Int,
  ): F[MessageId] = {
    val record = new ProducerRecord[Array[Byte], Array[Byte]](
      destination.name,
      message.key.map(_.value.getBytes(UTF_8)).orNull,
      message.payload,
    )
    message.headers.foreach { case (name, value) =>
      record.headers.add(new RecordHeader(name, value.getBytes(UTF_8)))
    }
    record.headers.add(new RecordHeader(attemptHeader, attempt.toString.getBytes(UTF_8)))
    F.async_[RecordMetadata] { callback =>
      client.send(
        record,
        new Callback {
          def onCompletion(metadata: RecordMetadata, failure: Exception): Unit =
            if (failure != null) callback(Left(failure)) else callback(Right(metadata))
        },
      )
      ()
    }.map(metadata =>
      MessageId(s"${metadata.topic}-${metadata.partition}-${metadata.offset}")
    )
  }
}

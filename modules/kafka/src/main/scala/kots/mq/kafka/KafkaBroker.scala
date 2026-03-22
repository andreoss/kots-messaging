package kots.mq.kafka

import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.all._
import kots.mq._
import org.apache.kafka.clients.admin.{
  Admin => KafkaAdmin,
  AdminClientConfig,
  NewTopic,
  OffsetSpec,
  RecordsToDelete,
}
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
        client => F.blocking(client.close(JavaDuration.ofSeconds(5)))
      )
      .map(new KafkaBroker[F](_, settings, entropy))

  private def producerProperties(settings: KafkaSettings): Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    settings.clientId.foreach(properties.put(ProducerConfig.CLIENT_ID_CONFIG, _))
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
    settings.clientId.foreach(name =>
      properties.put(ConsumerConfig.CLIENT_ID_CONFIG, s"$name-${destination.name}")
    )
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
    Capabilities.of(
      Capability.Batch,
      Capability.DeadLetter,
      Capability.OrderingGroup,
      Capability.Topology,
    )

  val events: BrokerEvents[F] = BrokerEvents.quiet[F]

  private def adminClient: Resource[F, KafkaAdmin] =
    Resource.make(
      F.blocking {
        val properties = new Properties()
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers)
        KafkaAdmin.create(properties)
      }
    )(client => F.blocking(client.close(JavaDuration.ofSeconds(5))))

  val admin: Admin[F] = new Admin[F] {

    def declare(destination: Destination): F[Unit] =
      adminClient
        .use(client =>
          F.blocking(
            client
              .createTopics(Collections.singletonList(new NewTopic(destination.name, 1, 1.toShort)))
              .all()
              .get()
          )
        )
        .void
        .recover { case _: Throwable => () }

    def purge(destination: Destination): F[Option[Long]] =
      adminClient.use { client =>
        F.blocking {
          val description = client.describeTopics(Collections.singletonList(destination.name))
          val partitions = description
            .allTopicNames()
            .get()
            .get(destination.name)
            .partitions()
            .asScala
            .map(partition => new TopicPartition(destination.name, partition.partition()))
            .toList
          val ends = client
            .listOffsets(
              partitions
                .map(partition =>
                  partition -> (OffsetSpec.latest(): OffsetSpec)
                )
                .toMap
                .asJava
            )
            .all()
            .get()
          val removals = partitions
            .map(partition => partition -> RecordsToDelete.beforeOffset(ends.get(partition).offset()))
            .toMap
          client.deleteRecords(removals.asJava).all().get()
          Option(removals.values.map(_.beforeOffset()).sum)
        }
      }.recover { case _: Throwable => None }

    def delete(destination: Destination): F[Unit] =
      adminClient
        .use(client =>
          F.blocking(client.deleteTopics(Collections.singletonList(destination.name)).all().get())
        )
        .void
        .recover { case _: Throwable => () }

    def depth(destination: Destination): F[Option[Long]] =
      Resource
        .make(
          F.blocking(
            new KafkaConsumer[Array[Byte], Array[Byte]](
              consumerProperties(settings, destination, ConsumerSettings.default)
            )
          )
        )(reader => F.blocking(reader.close(JavaDuration.ofSeconds(5))))
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
      )(closing => F.blocking(closing.close(JavaDuration.ofSeconds(5))))
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
          } yield records.map(new RecordDelivery(_))
        }

      def ackAll(deliveries: List[Delivery[F, Array[Byte]]]): F[Unit] = {
        val records = deliveries.collect { case delivery: RecordDelivery => delivery.record }
        val others = deliveries.filterNot(_.isInstanceOf[RecordDelivery])
        guard.lock.surround(commit(records)) *> others.traverse_(_.ack)
      }

      def extendAll(deliveries: List[Delivery[F, Array[Byte]]], by: FiniteDuration): F[Unit] =
        F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))

      private def fetch(room: Int): F[List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
        for {
          _ <- buffered.get.flatMap(current => poll.whenA(current.isEmpty))
          taken <- buffered.modify(current => (current.drop(room), current.take(room).toList))
        } yield taken

      private val poll: F[Unit] =
        F.blocking(reader.poll(JavaDuration.ofMillis(settings.pollTimeout.toMillis)))
          .flatMap(records => buffered.update(_ ++ records.asScala.toVector))

      private final class RecordDelivery(
        val record: ConsumerRecord[Array[Byte], Array[Byte]]
      ) extends Delivery[F, Array[Byte]] {

        val envelope: Envelope[Array[Byte]] = envelopeOf(record)

        val ack: F[Unit] = guard.lock.surround(commit(List(record)))

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
            _ <- guard.lock.surround(commit(List(record)))
          } yield ()

        val release: F[Unit] = guard.lock.surround(buffered.update(record +: _))

        val deadLetter: F[Unit] =
          consumerSettings.deadLetter.traverse_(parked =>
            publish(parked, envelope.message, envelope.attempt)
          ) *> guard.lock.surround(commit(List(record)))

        def extend(by: FiniteDuration): F[Unit] =
          F.raiseError(CapabilityUnsupported(Capability.LeaseExtension))
      }

      private def commit(records: List[ConsumerRecord[Array[Byte], Array[Byte]]]): F[Unit] =
        if (records.isEmpty) F.unit
        else
          for {
            offsets <- tracked.updateAndGet(current =>
              records.foldLeft(current) { (acc, record) =>
                val partition = partitionOf(record)
                acc.updated(partition, acc.getOrElse(partition, Offsets.empty).settle(record.offset))
              }
            )
            positions = records
              .map(partitionOf)
              .distinct
              .flatMap(partition =>
                offsets.get(partition).flatMap(_.position).map(partition -> _)
              )
            _ <- F
              .blocking(
                reader.commitSync(
                  positions.map { case (partition, position) =>
                    partition -> new OffsetAndMetadata(position)
                  }.toMap.asJava
                )
              )
              .whenA(positions.nonEmpty)
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

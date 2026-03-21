package kots.mq.sqs

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import kots.mq._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.{
  ChangeMessageVisibilityBatchRequest,
  ChangeMessageVisibilityBatchRequestEntry,
  ChangeMessageVisibilityRequest,
  CreateQueueRequest,
  DeleteMessageBatchRequest,
  DeleteMessageBatchRequestEntry,
  DeleteMessageRequest,
  GetQueueAttributesRequest,
  GetQueueUrlRequest,
  Message => SqsMessage,
  MessageAttributeValue,
  MessageSystemAttributeName,
  QueueAttributeName,
  QueueDoesNotExistException,
  ReceiveMessageRequest,
  SendMessageBatchRequest,
  SendMessageBatchRequestEntry,
  SendMessageRequest,
  SetQueueAttributesRequest,
}

import java.net.URI
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** SQS adapter: the lease is a visibility timeout, the budget a redrive. */
object SqsBroker {

  private[sqs] val keyAttribute = "x-mq-key"

  private[sqs] val recoverableCodes: Set[String] =
    Set("ServiceUnavailable", "ThrottlingException", "RequestThrottled", "InternalError")

  def bytes[F[_]](settings: SqsSettings, entropy: Entropy[F])(implicit
    F: Async[F]
  ): Resource[F, Broker[F, Array[Byte]]] =
    client(settings).map(fromClient(_, settings, entropy))

  /** Over a client the caller built and owns. */
  def fromClient[F[_]](client: SqsClient, settings: SqsSettings, entropy: Entropy[F])(implicit
    F: Async[F]
  ): Broker[F, Array[Byte]] = new SqsBroker[F](client, settings, entropy)

  private def client[F[_]](settings: SqsSettings)(implicit F: Async[F]): Resource[F, SqsClient] =
    Resource
      .make(F.blocking {
        val builder = SqsClient
          .builder()
          .region(Region.of(settings.region))
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(settings.accessKey, settings.secretKey)
            )
          )
        settings.endpoint.fold(builder)(uri => builder.endpointOverride(URI.create(uri))).build()
      })(open => F.blocking(open.close()))
}

private final class SqsBroker[F[_]](
  client: SqsClient,
  settings: SqsSettings,
  entropy: Entropy[F],
)(implicit F: Async[F])
  extends Broker[F, Array[Byte]] {

  import SqsBroker._

  val capabilities: Capabilities =
    Capabilities.of(
      Capability.Batch,
      Capability.Delay,
      Capability.DeadLetter,
      Capability.LeaseExtension,
    )

  val admin: Admin[F] = new Admin[F] {
    def depth(destination: Destination): F[Option[Long]] =
      queueUrl(destination)
        .flatMap(url =>
          F.blocking(
            client
              .getQueueAttributes(
                GetQueueAttributesRequest
                  .builder()
                  .queueUrl(url)
                  .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                  .build()
              )
              .attributes()
              .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
          )
        )
        .map(value => Option(value).flatMap(_.toLongOption))
        .recover { case _: Throwable => None }
  }

  def producer(destination: Destination): Resource[F, Producer[F, Array[Byte]]] =
    Resource.eval(queueUrl(destination)).map { url =>
      new Producer[F, Array[Byte]] {

        def send(message: Message[Array[Byte]]): F[MessageId] = publish(url, message, None)

        def sendAfter(message: Message[Array[Byte]], delay: FiniteDuration): F[MessageId] =
          publish(url, message, Some(delay))

        def sendBatch(
          messages: List[Message[Array[Byte]]]
        ): F[List[Either[SendFailure, MessageId]]] =
          messages.grouped(10).toList.flatTraverse(publishBatch(url, _))
      }
    }

  def consumer(
    destination: Destination,
    consumerSettings: ConsumerSettings,
  ): Resource[F, Consumer[F, Array[Byte]]] =
    for {
      url <- Resource.eval(queueUrl(destination))
      _ <- Resource.eval(redrive(url, consumerSettings))
      inflight <- Resource.eval(F.ref(Set.empty[String]))
    } yield new Consumer[F, Array[Byte]] {

      def receive: F[Option[Delivery[F, Array[Byte]]]] = receiveBatch(1).map(_.headOption)

      def receiveBatch(max: Int): F[List[Delivery[F, Array[Byte]]]] =
        for {
          held <- inflight.get
          room = math.max(0, math.min(math.min(max, 10), consumerSettings.prefetch - held.size))
          messages <-
            if (room <= 0) F.pure(List.empty[SqsMessage])
            else
              F.blocking(
                client
                  .receiveMessage(
                    ReceiveMessageRequest
                      .builder()
                      .queueUrl(url)
                      .maxNumberOfMessages(room)
                      .waitTimeSeconds(settings.waitTime.toSeconds.toInt)
                      .visibilityTimeout(consumerSettings.lease.toSeconds.toInt)
                      .messageAttributeNames("All")
                      .messageSystemAttributeNames(
                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT
                      )
                      .build()
                  )
                  .messages()
                  .asScala
                  .toList
              )
          _ <- inflight.update(_ ++ messages.map(_.receiptHandle))
        } yield messages.map(new QueuedDelivery(_))

      def ackAll(deliveries: List[Delivery[F, Array[Byte]]]): F[Unit] = {
        val handles = deliveries.collect { case delivery: QueuedDelivery => delivery.handle }
        val others = deliveries.filterNot(_.isInstanceOf[QueuedDelivery])
        deleteBatch(handles) *> others.traverse_(_.ack)
      }

      def extendAll(deliveries: List[Delivery[F, Array[Byte]]], by: FiniteDuration): F[Unit] = {
        val handles = deliveries.collect { case delivery: QueuedDelivery => delivery.handle }
        val others = deliveries.filterNot(_.isInstanceOf[QueuedDelivery])
        visibilityBatch(handles, by.toSeconds.toInt) *> others.traverse_(_.extend(by))
      }

      private def deleteBatch(handles: List[String]): F[Unit] =
        handles.grouped(10).toList.traverse_ { batch =>
          F.blocking(
            client.deleteMessageBatch(
              DeleteMessageBatchRequest
                .builder()
                .queueUrl(url)
                .entries(
                  batch.zipWithIndex.map { case (handle, index) =>
                    DeleteMessageBatchRequestEntry
                      .builder()
                      .id(index.toString)
                      .receiptHandle(handle)
                      .build()
                  }.asJava
                )
                .build()
            )
          ) *> inflight.update(_ -- batch)
        }

      private def visibilityBatch(handles: List[String], seconds: Int): F[Unit] =
        handles.grouped(10).toList.traverse_ { batch =>
          F.blocking(
            client.changeMessageVisibilityBatch(
              ChangeMessageVisibilityBatchRequest
                .builder()
                .queueUrl(url)
                .entries(
                  batch.zipWithIndex.map { case (handle, index) =>
                    ChangeMessageVisibilityBatchRequestEntry
                      .builder()
                      .id(index.toString)
                      .receiptHandle(handle)
                      .visibilityTimeout(seconds)
                      .build()
                  }.asJava
                )
                .build()
            )
          ).void
        }

      private final class QueuedDelivery(message: SqsMessage) extends Delivery[F, Array[Byte]] {

        val handle: String = message.receiptHandle

        val envelope: Envelope[Array[Byte]] = envelopeOf(message)

        val ack: F[Unit] =
          F.blocking(
            client.deleteMessage(
              DeleteMessageRequest.builder().queueUrl(url).receiptHandle(handle).build()
            )
          ).void *> inflight.update(_ - handle)

        val reject: F[Unit] =
          if (envelope.attempt >= consumerSettings.maxAttempts && consumerSettings.deadLetter.isEmpty)
            ack
          else
            for {
              sample <- entropy.nextDouble
              seconds = consumerSettings.backoff.delay(envelope.attempt, sample).toSeconds.toInt
              _ <- changeVisibility(url, handle, seconds)
              _ <- inflight.update(_ - handle)
            } yield ()

        val release: F[Unit] =
          changeVisibility(url, handle, 0) *> inflight.update(_ - handle)

        val deadLetter: F[Unit] =
          consumerSettings.deadLetter.traverse_(parked =>
            queueUrl(parked).flatMap(parkedUrl => publish(parkedUrl, envelope.message, None))
          ) *> ack

        def extend(by: FiniteDuration): F[Unit] =
          changeVisibility(url, handle, by.toSeconds.toInt)
      }
    }
  private def changeVisibility(url: String, handle: String, seconds: Int): F[Unit] =
    F.blocking(
      client.changeMessageVisibility(
        ChangeMessageVisibilityRequest
          .builder()
          .queueUrl(url)
          .receiptHandle(handle)
          .visibilityTimeout(seconds)
          .build()
      )
    ).void

  private def queueUrl(destination: Destination): F[String] =
    F.blocking(
      client.getQueueUrl(GetQueueUrlRequest.builder().queueName(destination.name).build()).queueUrl()
    ).recoverWith {
      case _: QueueDoesNotExistException if settings.createIfMissing =>
        F.blocking(
          client
            .createQueue(CreateQueueRequest.builder().queueName(destination.name).build())
            .queueUrl()
        )
    }

  private def redrive(url: String, consumerSettings: ConsumerSettings): F[Unit] =
    consumerSettings.deadLetter.traverse_ { parked =>
      for {
        parkedUrl <- queueUrl(parked)
        arn <- F.blocking(
          client
            .getQueueAttributes(
              GetQueueAttributesRequest
                .builder()
                .queueUrl(parkedUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build()
            )
            .attributes()
            .get(QueueAttributeName.QUEUE_ARN)
        )
        policy =
          s"""{"deadLetterTargetArn":"$arn","maxReceiveCount":"${consumerSettings.maxAttempts}"}"""
        _ <- F.blocking(
          client.setQueueAttributes(
            SetQueueAttributesRequest
              .builder()
              .queueUrl(url)
              .attributes(Map(QueueAttributeName.REDRIVE_POLICY -> policy).asJava)
              .build()
          )
        )
      } yield ()
    }

  private def envelopeOf(message: SqsMessage): Envelope[Array[Byte]] = {
    val attributes = message.messageAttributes.asScala
    val headers = attributes.view
      .collect {
        case (name, value) if name != keyAttribute => name -> value.stringValue
      }
      .toMap
    val attempt = Option(
      message.attributes.get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
    ).flatMap(_.toIntOption).getOrElse(1)
    Envelope(
      MessageId(message.messageId),
      Message(
        message.body.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1),
        headers,
        attributes.get(keyAttribute).map(value => MessageKey(value.stringValue)),
      ),
      attempt,
    )
  }

  private def attributesOf(message: Message[Array[Byte]]): Map[String, MessageAttributeValue] =
    (message.headers.map { case (name, value) =>
      name -> MessageAttributeValue.builder().dataType("String").stringValue(value).build()
    } ++ message.key.map(key =>
      keyAttribute -> MessageAttributeValue
        .builder()
        .dataType("String")
        .stringValue(key.value)
        .build()
    )).toMap

  private def bodyOf(message: Message[Array[Byte]]): String =
    new String(message.payload, java.nio.charset.StandardCharsets.ISO_8859_1)

  private def publish(
    url: String,
    message: Message[Array[Byte]],
    delay: Option[FiniteDuration],
  ): F[MessageId] =
    F.blocking {
      val builder = SendMessageRequest
        .builder()
        .queueUrl(url)
        .messageBody(bodyOf(message))
        .messageAttributes(attributesOf(message).asJava)
      delay
        .fold(builder)(value => builder.delaySeconds(value.toSeconds.toInt))
        .build()
    }.flatMap(request => F.blocking(client.sendMessage(request)))
      .map(response => MessageId(response.messageId))

  private def publishBatch(
    url: String,
    messages: List[Message[Array[Byte]]],
  ): F[List[Either[SendFailure, MessageId]]] =
    if (messages.isEmpty) F.pure(List.empty)
    else {
      val entries = messages.zipWithIndex.map { case (message, index) =>
        SendMessageBatchRequestEntry
          .builder()
          .id(index.toString)
          .messageBody(bodyOf(message))
          .messageAttributes(attributesOf(message).asJava)
          .build()
      }
      F.blocking(
        client.sendMessageBatch(
          SendMessageBatchRequest.builder().queueUrl(url).entries(entries.asJava).build()
        )
      ).map { response =>
        val succeeded = response
          .successful()
          .asScala
          .map(entry => entry.id -> MessageId(entry.messageId))
          .toMap
        val failed = response
          .failed()
          .asScala
          .map(entry =>
            entry.id -> SendFailure(
              entry.code,
              entry.message,
              recoverable = !entry.senderFault || recoverableCodes(entry.code),
            )
          )
          .toMap
        messages.indices.toList.map { index =>
          val id = index.toString
          succeeded
            .get(id)
            .map(Right(_))
            .orElse(failed.get(id).map(Left(_)))
            .getOrElse(
              Left(SendFailure("Unknown", "the service reported no outcome", recoverable = false))
            )
        }
      }
    }
}

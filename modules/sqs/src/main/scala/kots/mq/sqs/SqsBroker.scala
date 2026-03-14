package kots.mq.sqs

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._
import kots.mq._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.{
  ChangeMessageVisibilityRequest,
  CreateQueueRequest,
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

  def bytes[F[_]](settings: SqsSettings, entropy: Entropy[F])(implicit
    F: Async[F]
  ): Resource[F, Broker[F, Array[Byte]]] =
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
      })(client => F.blocking(client.close()))
      .map(new SqsBroker[F](_, settings, entropy))
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
        } yield messages.map(delivered(url, _, consumerSettings, inflight))

      private def delivered(
        url: String,
        message: SqsMessage,
        consumerSettings: ConsumerSettings,
        inflight: cats.effect.kernel.Ref[F, Set[String]],
      ): Delivery[F, Array[Byte]] =
        new Delivery[F, Array[Byte]] {

          val envelope: Envelope[Array[Byte]] = envelopeOf(message)

          val ack: F[Unit] =
            F.blocking(
              client.deleteMessage(
                DeleteMessageRequest
                  .builder()
                  .queueUrl(url)
                  .receiptHandle(message.receiptHandle)
                  .build()
              )
            ).void *> inflight.update(_ - message.receiptHandle)

          val reject: F[Unit] =
            if (envelope.attempt >= consumerSettings.maxAttempts && consumerSettings.deadLetter.isEmpty)
              ack
            else
              for {
                sample <- entropy.nextDouble
                seconds = consumerSettings.backoff.delay(envelope.attempt, sample).toSeconds.toInt
                _ <- changeVisibility(url, message.receiptHandle, seconds)
                _ <- inflight.update(_ - message.receiptHandle)
              } yield ()

          def extend(by: FiniteDuration): F[Unit] =
            changeVisibility(url, message.receiptHandle, by.toSeconds.toInt)
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
          .map(entry => entry.id -> SendFailure(entry.code, entry.message))
          .toMap
        messages.indices.toList.map { index =>
          val id = index.toString
          succeeded
            .get(id)
            .map(Right(_))
            .orElse(failed.get(id).map(Left(_)))
            .getOrElse(Left(SendFailure("Unknown", "the service reported no outcome")))
        }
      }
    }
}

package kots.messaging.sqs

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model._

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** Counts the requests the adapter makes, so batching is evidence not hope. */
final class CountingSqsClient(underlying: SqsClient) extends SqsClient {

  val deleteBatches = new AtomicInteger(0)
  val singleDeletes = new AtomicInteger(0)
  val visibilityBatches = new AtomicInteger(0)

  def serviceName(): String = underlying.serviceName()

  def close(): Unit = underlying.close()

  override def receiveMessage(request: ReceiveMessageRequest): ReceiveMessageResponse =
    underlying.receiveMessage(request)

  override def deleteMessage(request: DeleteMessageRequest): DeleteMessageResponse = {
    singleDeletes.incrementAndGet()
    underlying.deleteMessage(request)
  }

  override def deleteMessageBatch(
    request: DeleteMessageBatchRequest
  ): DeleteMessageBatchResponse = {
    deleteBatches.incrementAndGet()
    underlying.deleteMessageBatch(request)
  }

  override def changeMessageVisibility(
    request: ChangeMessageVisibilityRequest
  ): ChangeMessageVisibilityResponse = underlying.changeMessageVisibility(request)

  override def changeMessageVisibilityBatch(
    request: ChangeMessageVisibilityBatchRequest
  ): ChangeMessageVisibilityBatchResponse = {
    visibilityBatches.incrementAndGet()
    underlying.changeMessageVisibilityBatch(request)
  }

  override def sendMessage(request: SendMessageRequest): SendMessageResponse =
    underlying.sendMessage(request)

  override def sendMessageBatch(request: SendMessageBatchRequest): SendMessageBatchResponse =
    underlying.sendMessageBatch(request)

  override def getQueueUrl(request: GetQueueUrlRequest): GetQueueUrlResponse =
    underlying.getQueueUrl(request)

  override def createQueue(request: CreateQueueRequest): CreateQueueResponse =
    underlying.createQueue(request)

  override def getQueueAttributes(
    request: GetQueueAttributesRequest
  ): GetQueueAttributesResponse = underlying.getQueueAttributes(request)

  override def setQueueAttributes(
    request: SetQueueAttributesRequest
  ): SetQueueAttributesResponse = underlying.setQueueAttributes(request)
}

final class SqsBatchSettleSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  private def counting: Resource[IO, CountingSqsClient] =
    Resource.make(
      IO.blocking {
        val settings = SqsTestSupport.settings
        val builder = SqsClient
          .builder()
          .region(Region.of(settings.region))
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(settings.accessKey, settings.secretKey)
            )
          )
        new CountingSqsClient(
          settings.endpoint
            .fold(builder)(uri => builder.endpointOverride(URI.create(uri)))
            .build()
        )
      }
    )(client => IO.blocking(client.close()))

  test("acknowledging twelve deliveries costs two requests, not twelve") {
    val destination = SqsTestSupport.queue("batch-settle")
    val bodies = List.range(0, 12).map(index => kots.messaging.Message.of(s"body-$index"))
    counting.use { client =>
      val broker = Transcode.broker(
        SqsBroker.fromClient[IO](client, SqsTestSupport.settings, Entropy.system[IO]),
        Codec.utf8,
      )
      (
        broker.producer(destination),
        broker.consumer(destination, SqsTestSupport.settingsWithoutBackoff),
      ).tupled.use { case (producer, consumer) =>
        for {
          _ <- producer.sendBatch(bodies)
          held <- CapabilityChecks.batchWithin(consumer, bodies.size, 60.seconds)
          _ <- consumer.ackAll(held)
          drained <- CapabilityChecks.receiveWithin(consumer, 3.seconds)
        } yield {
          assertEquals(held.size, bodies.size)
          assertEquals(client.deleteBatches.get(), 2)
          assertEquals(client.singleDeletes.get(), 0)
          assertEquals(drained.map(_.envelope.message.payload), None)
        }
      }
    }
  }

  test("extending twelve leases costs two requests as well") {
    val destination = SqsTestSupport.queue("batch-extend")
    val bodies = List.range(0, 12).map(index => kots.messaging.Message.of(s"body-$index"))
    counting.use { client =>
      val broker = Transcode.broker(
        SqsBroker.fromClient[IO](client, SqsTestSupport.settings, Entropy.system[IO]),
        Codec.utf8,
      )
      (
        broker.producer(destination),
        broker.consumer(destination, SqsTestSupport.settingsWithoutBackoff),
      ).tupled.use { case (producer, consumer) =>
        for {
          _ <- producer.sendBatch(bodies)
          held <- CapabilityChecks.batchWithin(consumer, bodies.size, 60.seconds)
          _ <- consumer.extendAll(held, 30.seconds)
          _ <- consumer.ackAll(held)
        } yield assertEquals(client.visibilityBatches.get(), 2)
      }
    }
  }
}

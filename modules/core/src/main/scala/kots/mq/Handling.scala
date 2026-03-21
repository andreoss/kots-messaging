package kots.mq

import scala.concurrent.duration.FiniteDuration

/** How a handler's run is bounded and what becomes of the message. */
final case class Handling(
  onError: Throwable => Settlement,
  renewEvery: Option[FiniteDuration],
  timeout: Option[FiniteDuration],
  onTimeout: Settlement,
) {

  /** Renews the lease at this interval while the handler runs. */
  def renewingEvery(interval: FiniteDuration): Handling = copy(renewEvery = Some(interval))

  /** Cancels a handler past this limit and settles it the stated way. */
  def withTimeout(limit: FiniteDuration, settlement: Settlement): Handling =
    copy(timeout = Some(limit), onTimeout = settlement)

  def onErrorSettle(settlement: Settlement): Handling = copy(onError = _ => settlement)
}

object Handling {

  /** A failure retries; the lease is neither renewed nor bounded. */
  val default: Handling = Handling(_ => Settlement.Retry, None, None, Settlement.Retry)

  /** Renews at half the lease and gives the handler the lease to finish in. */
  def within(lease: FiniteDuration): Handling =
    default.renewingEvery(lease / 2).withTimeout(lease, Settlement.Retry)
}

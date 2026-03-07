package kots.mq

/** Hands out deliveries from one destination; settling is the caller's. */
trait Consumer[F[_], A] {
  def receive: F[Option[Delivery[F, A]]]
  def receiveBatch(max: Int): F[List[Delivery[F, A]]]
}

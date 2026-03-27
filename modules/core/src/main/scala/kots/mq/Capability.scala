package kots.mq

/** Optional broker feature an adapter declares and the contract then tests. */
sealed trait Capability

object Capability {
  case object Delay extends Capability
  case object Priority extends Capability
  case object OrderingGroup extends Capability
  case object DeadLetter extends Capability
  case object Batch extends Capability
  case object LeaseExtension extends Capability
  case object Topology extends Capability
  case object Expiry extends Capability
}

/** What one adapter declares it honours. */
final case class Capabilities(values: Set[Capability]) {
  def has(capability: Capability): Boolean = values.contains(capability)
  def and(capability: Capability): Capabilities = Capabilities(values + capability)
}

object Capabilities {
  val none: Capabilities = Capabilities(Set.empty)
  def of(capabilities: Capability*): Capabilities = Capabilities(capabilities.toSet)
}

/** Raised when a caller asks for a feature the broker does not have. */
final case class CapabilityUnsupported(capability: Capability)
  extends RuntimeException(s"unsupported capability: $capability")

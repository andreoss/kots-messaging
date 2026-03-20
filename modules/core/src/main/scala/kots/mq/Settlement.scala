package kots.mq

/** What a handler decides should happen to the message it was given. */
sealed trait Settlement

object Settlement {

  /** The work succeeded; the message is finished. */
  case object Done extends Settlement

  /** The work failed and should be tried again; the attempt counts. */
  case object Retry extends Settlement

  /** Nothing was done with the message; return it as it was found. */
  case object Release extends Settlement

  /** The work failed and must not be tried again; the message is discarded. */
  case object Drop extends Settlement

  /** The message cannot succeed; park it now rather than spend its budget. */
  case object DeadLetter extends Settlement
}

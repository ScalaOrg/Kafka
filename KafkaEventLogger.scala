package com.clara.kafka

import com.expeditelabs.thrift.{events => thriftEvents, scala => thrift}
import com.expeditelabs.util.{JavaFutureConversion, ObservedEventLog}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util._

// Event logger that logs event stream to kafka bus
// Topic is configured at initialization
class KafkaEventLogger(
  topic: String,
  sender: EventSender,
  statsReceiver: StatsReceiver
) extends ObservedEventLog with JavaFutureConversion {

  val scopedStats = statsReceiver.scope("KafkaEventLog")
  val requestLogger = Logger("KafkaEventRequestLog")

  override def saveEvent(message: thriftEvents.EventMessage): Future[Unit] =
    observe("saveEvent") {
      sender.send(topic)(message)
    }
}

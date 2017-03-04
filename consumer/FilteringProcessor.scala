package com.clara.kafka.consumer

import com.expeditelabs.thrift.events
import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.logging.Logger
import com.twitter.util.Future

object FilteringProcessor {
  /**
    * A simple FilteringProcessor that does no filtering at all, really, and calls your very
    * favorite callback for every batch. Be careful what topic you listen to with this! It's
    * really here for debugging purposes but I'm sure someone will find a new and interesting
    * way to abuse it.
    *
    * @param callback functor to handle your event messages
    */
  def withCallback(
    callback: Seq[events.EventMessage] => Future[Unit],
    statsReceiver: StatsReceiver,
    logging: Logger
  ): FilteringProcessor =
    new FilteringProcessor {
      override val scopedStats = statsReceiver.scope("callback_processor")
      override val logger = logging
      override def isDefinedAt(messages: events.EventMessage): Future[Boolean] = Future.True
      override def apply(messages: Seq[events.EventMessage]): Future[Unit] = callback(messages)
    }
}

/**
  * FilteringProcessor
  *
  * Base trait for processors that listen to a stream of incoming events. The processor defines
  * two things: isDefinedAt defines what type of events a processor is capable of responding to
  * and its apply(...) method determines how it will process those messages.
  *
  * Additionally, it must define a scopedStats which will be used to provide a base set of
  * counters for export.
  */
trait FilteringProcessor extends (Seq[events.EventMessage] => Future[Unit]) { self =>
  protected[this] def scopedStats: StatsReceiver
  protected[this] def logger: Logger

  private[this] def processScope    = scopedStats.scope("process")
  private[this] def processCounter  = processScope.counter("requests")
  private[this] def successCounter  = processScope.counter("success")
  private[this] def failureCounter  = processScope.counter("failure")

  private[this] def batchSizeStat   = scopedStats.stat("batch_size")
  private[this] def processTimeStat = scopedStats.stat("process_time_ms")

  def isDefinedAt(message: events.EventMessage): Future[Boolean]

  def process(messages: Seq[events.EventMessage]): Future[Unit] = {
    Future.collect(
      messages.map { message =>
        isDefinedAt(message).map((_, message))
      }
    ).flatMap { isDefinedAtPairs =>
      // check if isDefinedAt evaluated to `true` and only take message
      val filteredMessages = isDefinedAtPairs.filter(_._1).map(_._2)

      if (filteredMessages.nonEmpty) {
        processCounter.incr()
        batchSizeStat.add(messages.size.toFloat)

        // TODO(wz): make use of StatsObserver (com.expeditelabs.util)
        Stat.timeFuture(processTimeStat) {
          self(filteredMessages).onSuccess { _ =>
            successCounter.incr()
          }.onFailure {
            case ex: Throwable =>
              failureCounter.incr()
              logger.info(ex, "Failed to process batch")
          }
        }
      } else {
        Future.Done
      }
    }
  }
}
Contact GitHub API Training Shop Blog About

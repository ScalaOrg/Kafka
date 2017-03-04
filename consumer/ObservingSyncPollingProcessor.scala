package com.clara.kafka.consumer

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Await, Future}

trait ObservingSyncPollingProcessor[T] extends SynchronousPollingProcessor[T] {
  def service: (T) => Future[Unit]
  def processBatch(messages: Seq[T]): Unit = {
    val futureResponses = messages.map(service).map { response =>
      response.onSuccess { _ =>
        scopedStats.counter("success").incr()
      }.onFailure { _ =>
        scopedStats.counter("failure").incr()
      }.rescue {
        case ex: Throwable =>
          scopedStats.counter("rescue").incr()
          logger.error(ex, s"uncaught exception in stream processing: ${ex.getMessage}")
          Future.Unit
      }
    }
    Await.result(Future.collect(futureResponses).unit)
  }

  protected[this] def scopedStats: StatsReceiver
  protected[this] def logger: Logger
}

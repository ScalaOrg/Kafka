package com.clara.kafka.consumer

import com.expeditelabs.util.rollbar.{ExceptionNotifier, NullExceptionNotifier}
import com.twitter.logging.Logger
import com.twitter.util.{Await, CloseAwaitably, Future, Time}

/**
  * A MessageConsumer that runs in the current thread and polls a MessageSource for new messages to process
  *
  * Outside of testing, this should generally be launched in it's own worker thread, since it polls the messageSource
  * until `close`-d; see KafkaDbEventIndexer for an example of how this might be launched using an executorService:
  *
  * ```
  * val indexingJob = new KafkaDbEventIndexer(
  *   messageSource,
  *   indexer,
  *   statsReceiver)
  *
  * val executor = Executors.newSingleThreadExecutor()
  * executor.submit(indexingJob)```
  */
trait SynchronousPollingProcessor[T]
  extends MessageProcessor[T] with CloseAwaitably {

  def messageSource: MessageSource[T]

  val processorLogger: Logger = Logger("SynchronousPollingProcessor")

  @volatile var running: Boolean = true

  override def run(): Unit = {
    try {
      while (running) {
        val batch = messageSource.getBatch()
        processBatch(batch)
      }
    } catch {
      case ex: Throwable =>
        processorLogger.error(s"Caught Exception: ", ex.getMessage)
        ()
    } finally {
      Await.result(messageSource.close())
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    running = false
    messageSource.interrupt()
    Future.Done
  }
}
Contact GitHub API Training Shop Blog About

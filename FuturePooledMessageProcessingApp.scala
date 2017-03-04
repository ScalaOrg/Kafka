package com.clara.kafka.app

import java.util.concurrent.{ExecutorService, Executors}

import com.clara.kafka.consumer.SynchronousPollingProcessor
import com.twitter.app.App
import com.twitter.logging.Logging
import com.twitter.util.{Await, FuturePool}

/**
  * extend from this class to implement an App that uses a [[SynchronousPollingProcessor]] to process
  * kafka messages of type [[T]]
  *
  * this class will take care of FuturePooling and running the message processor in a separate thread
  *
  * @tparam T
  */
abstract class FuturePooledMessageProcessingApp[T] extends App with Logging {
  val executor: ExecutorService = Executors.newSingleThreadExecutor()
  val futurePool: FuturePool = FuturePool(executor)

  /**
    * run the processor - should generally be the last thing in the `main` method
    *
    * @param proc - the message processor
    */
  def run(proc: SynchronousPollingProcessor[T]): Unit = {
    onExit {
      log.info(s"onExit - closing message processor ${proc} and executor")
      executor.shutdown()
      proc.close()
      log.info(s"onExit - successfully closed message processor ${proc} and executor")
    }

    log.info(s"starting message processor ${proc} in futurePool")
    val procRun = futurePool(proc.run())
    Await.ready(procRun)
    log.info(s"message processor ${proc} ready")
  }
}

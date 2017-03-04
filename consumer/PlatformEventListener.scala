package com.clara.kafka.consumer

import com.clara.kafka.{KafkaThriftEventDeserializer, KafkaTopics}
import com.expeditelabs.thrift.events
import com.twitter.finagle.stats.{Stat, StatsReceiver}

object PlatformEventListener {
  /**
    * creates a kafka message source to be used for listening to [[events.EventMessage]]s
    *
    * @param kafkaServerName - comma separated list of kafka server:port
    * @param kafkaGroupIdName - kafka group identifier
    * @param statsReceiver
    * @return - new kafka message source instance
    */
  def makeKafkaMessageSource(
    kafkaServerName: String,
    kafkaGroupIdName: String,
    topics: Seq[String],
    statsReceiver: StatsReceiver
  ): MessageSource[events.EventMessage] = {
    new KafkaMessageSource[events.EventMessage](
      kafkaServer       = kafkaServerName,
      kafkaGroupId      = kafkaGroupIdName,
      // N.B. This is the kafka topic, which is a different concept from the event topic
      // on the [[com.expeditelabs.thrift.events.EventMessage]]
      topics            = topics,
      valueDeserializer = new KafkaThriftEventDeserializer,
      statsReceiver     = statsReceiver
    )
  }

  /**
    * Creates a PlatformEventListener for the given kafkaServerName and kafkaGroupIdName, along
    * with a specified Seq of FilteringProcessor's that will actually do the processing
    *
    * @param kafkaServerName - comma separated list of kafka server:port
    * @param kafkaGroupIdName - kafka group identifier
    * @param processors - a Seq of FilteringProcessors that will actually process the messages
    * @param statsReceiver
    * @return
    */
  def withProcessors(
    kafkaServerName: String,
    kafkaGroupIdName: String,
    processors: Seq[FilteringProcessor],
    topics: Seq[String],
    statsReceiver: StatsReceiver
  ) = {
    new PlatformEventListener(
      makeKafkaMessageSource(kafkaServerName, kafkaGroupIdName, topics, statsReceiver),
      processors,
      statsReceiver
    )
  }
}

class PlatformEventListener(
  val messageSource: MessageSource[events.EventMessage],
  processors: Seq[FilteringProcessor],
  statsReceiver: StatsReceiver
) extends SynchronousPollingProcessor[events.EventMessage] {
  private[this] val scopedStats     = statsReceiver.scope("platform_event_listener")
  private[this] val processCounter  = scopedStats.counter("process")
  private[this] val batchSizeStat   = scopedStats.stat("batch_size")
  private[this] val processTimeStat = scopedStats.stat("process_time_ms")

  scopedStats.addGauge("num_processors")(processors.size.toFloat)

  // TODO(vc): Potentially add a Async semaphore here to control concurrency of processing
  override def processBatch(messages: Seq[events.EventMessage]): Unit = {
    processCounter.incr()
    batchSizeStat.add(messages.size.toFloat)

    Stat.time(processTimeStat) {
      processors.foreach { processor =>
        processor.process(messages)
      }
    }
  }
}

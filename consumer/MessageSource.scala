package com.clara.kafka.consumer

import java.util

import com.clara.kafka._
import com.expeditelabs.thrift.{events => eventsThrift}
import com.expeditelabs.util.GaugeField
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Closable, CloseAwaitably, Future, Time}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}

import scala.collection.JavaConversions.{collectionAsScalaIterable, asScalaIterator, mapAsJavaMap, seqAsJavaList}

trait MessageSource[T] extends Closable {
  /**
    * retrieve the next batch of messages
    *
    * @return the next batch of messages to process
    */
  def getBatch(): Seq[T]

  /**
    * interrupt any blocking or waiting for messages this MessageSource might be busy doing - should be called
    * during shutdown _before_ running any close or resource teardown
    */
  def interrupt(): Unit
}

/**
  * A KafkaMessageSource that starts at the end of the stream for topics indicated in where `topicsToFastForwardSetting`
  *
  * @tparam T
  */
class FastForwardKafkaMessageSource[T](
  kafkaServer: String,
  kafkaGroupId: String,
  topicsToFastForwardSetting: Map[String, Boolean],
  valueDeserializer: Deserializer[T],
  statsReceiver: StatsReceiver
) extends KafkaMessageSource[T](
  kafkaServer,
  kafkaGroupId,
  topicsToFastForwardSetting.keys.toSeq,
  valueDeserializer,
  statsReceiver
) {
  override def mkRebalanceListener: ConsumerRebalanceListener = new ConsumerRebalanceListener {
    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      partitions.foreach { partition =>
        if (topicsToFastForwardSetting.get(partition.topic()).getOrElse(false)) {
          kafkaConsumer.seekToEnd(partition)
        }
      }
    }

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {

    }
  }
}

/**
  * A message source that consumes messages from Kafka. Should generally be used in a worker that polls `getBatch` in it's
  * own thread. See [[SynchronousPollingProcessor]] for an example of such a worker
  *
  * @param kafkaServer
  * @param kafkaGroupId
  * @param topics
  * @param valueDeserializer
  * @param statsReceiver
  * @tparam T
  */
class KafkaMessageSource[T](
  kafkaServer: String,
  kafkaGroupId: String,
  topics: Seq[String],
  valueDeserializer: Deserializer[T],
  statsReceiver: StatsReceiver
) extends MessageSource[T]
  with CloseAwaitably {

  protected[this] def config: java.util.Map[String, Object] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaServer,
    ConsumerConfig.GROUP_ID_CONFIG -> kafkaGroupId,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000",
    ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> messageSize.toString
  )

  protected[this] val kafkaConsumer = new KafkaConsumer[String, T](config, new StringDeserializer, valueDeserializer)

  // operations must be done in rebalance callback because `subscribe` is "async" in that it'll return before assignments
  // have been made per https://qnalist.com/questions/6209394/new-consumer-subscribe-then-seek
  protected[this] def mkRebalanceListener: ConsumerRebalanceListener = new NoOpConsumerRebalanceListener()

  kafkaConsumer.subscribe(topics, mkRebalanceListener)

  val log: Logger = Logger("KafkaMessageSource")
  val scopedStats: StatsReceiver = statsReceiver.scope(kafkaGroupId)
  val offsets: GaugeField = new GaugeField(scopedStats)


  override def getBatch(): Seq[T] = {
    try {
      val recordsObject = kafkaConsumer.poll(Long.MaxValue)
      scopedStats.counter("poll").incr()
      val recordsIterator = recordsObject.iterator().map { record =>
        val key = s"consumer/offset/$kafkaGroupId/${record.topic}/${record.partition()}"
        offsets.setGauge(key, record.offset().toFloat)
        record.value()
      }
      val records = recordsIterator.toSeq
      scopedStats.counter("consumed").incr(records.length)
      records
    } catch {
      case ex: WakeupException => Nil // ignore to allow interrupt
    }
  }

  override def interrupt(): Unit = {
    try {
      kafkaConsumer.wakeup()
    } catch {
      case e: Throwable =>
        log.error(e, "error encountered closing KafkaMessageSource - kafka consumer connection may not be cleanly released")
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    closeAwaitably {
      kafkaConsumer.unsubscribe()
      kafkaConsumer.close()
      log.info("KafkaMessageSource terminated cleanly")

      Future.Done
    }
  }
}

object KafkaMessageSource {
  /**
    * creates a kafka message source to be used for listening for task events
    *
    * @param kafkaServerName - comma separated list of kafka server:port
    * @param kafkaGroupIdName - kafka group identifier
    * @param topics - sequence of kafka message topics
    * @param statsReceiver
    * @return - new kafka message source instance
    */
  def makeThriftEventSource(
    kafkaServerName: String,
    kafkaGroupIdName: String,
    topics: Seq[String],
    statsReceiver: StatsReceiver
  ): MessageSource[eventsThrift.EventMessage] = {
    new KafkaMessageSource[eventsThrift.EventMessage](
      kafkaServer       = kafkaServerName,
      kafkaGroupId      = kafkaGroupIdName,
      topics            = topics,
      valueDeserializer = new KafkaThriftEventDeserializer,
      statsReceiver     = statsReceiver
    )
  }
}

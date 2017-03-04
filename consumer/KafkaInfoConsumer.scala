package com.clara.kafka.consumer

import com.clara.kafka._
import com.expeditelabs.util.GaugeField
import com.twitter.finagle.stats.StatsReceiver
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.JavaConversions._

/**
  * It is surprisingly difficult to obtain a good read on
  * the current "end" of the topic partitions. This particular class
  * provides a way to measure the current offset of the partitions.
  *
  * @param kafkaServer
  * @param statsReceiver
  */
class KafkaInfoConsumer(
  kafkaServer: String,
  offsets: GaugeField,
  statsReceiver: StatsReceiver
) {

  val kafkaGroupId = "KafkaInfoConsumer" + java.util.UUID.randomUUID().toString()

  def apply(): Unit  = {
    val config = KafkaInfoConsumer.config
        .updated(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId)
        .updated(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)

    val kafkaConsumer = new KafkaConsumer(
      config,
      KafkaInfoConsumer.keyDeserializer,
      KafkaInfoConsumer.valueDeserializer)
    val topicPartitions = kafkaConsumer.listTopics().toMap
    val topicInfos = topicPartitions.map {
      case (topic, javaPartitions) => {
        if (!topic.contains("__consumer_offsets")) {
          val realParts = javaPartitions.toList
          realParts.map { partitionInfo =>
            val topicPartition = new TopicPartition(topic, partitionInfo.partition())
            kafkaConsumer.assign(List(topicPartition))
            kafkaConsumer.seekToEnd(topicPartition)
            kafkaConsumer.pause(topicPartition)
            val position = kafkaConsumer.position(topicPartition)
            offsets.setGauge(s"maxOffset/$topic/${topicPartition.partition()}", position.toFloat)
          }
        }
      }
    }
    kafkaConsumer.close()
  }
}

object KafkaInfoConsumer {
  val config: Map[String, String] = Map(
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000",
    ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> messageSize.toString
  )
  val keyDeserializer = new StringDeserializer
  val valueDeserializer = new ByteArrayDeserializer
}

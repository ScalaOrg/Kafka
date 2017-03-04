package com.clara.kafka

import com.expeditelabs.thrift.{events => thriftEvents}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.NullLogger
import com.twitter.util.{Future, Time}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

trait EventSender extends MessageSender[thriftEvents.EventMessage]

// TODO: Abstract out the NullMessageSender
object NullEventSender extends EventSender {
  override def send(topic: String)(message: => thriftEvents.EventMessage): Future[Unit] = Future.Unit
  override def close(deadline: Time): Future[Unit] = Future.Unit
}


/**
  * The KafkaEventSender is intended to be shared among all threads. Instantiate once
  * and then pass in just like eventLogger, statsReceiver, etc.
  */
class KafkaEventSender(
  bootstrapServers: String,
  statsReceiver: StatsReceiver
) extends EventSender with KafkaSender[String, thriftEvents.EventMessage] {

  val requestLogger = NullLogger
  val scopedStats = statsReceiver.scope("kafkaEvents")

  val keySerializer = new StringSerializer
  val valueSerializer = new KafkaThriftEventSerializer

  val config: Map[String, String] = Map(
    "bootstrap.servers" -> bootstrapServers,
    "client.id" -> "KafkaEventProducer",
    ProducerConfig.MAX_REQUEST_SIZE_CONFIG -> messageSize.toString
  )
  val kafkaProducer = new KafkaProducer[String, thriftEvents.EventMessage](
    mapAsJavaMapConverter[String, Object](config).asJava,
    keySerializer,
    valueSerializer
  )
}

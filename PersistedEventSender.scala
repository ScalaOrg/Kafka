package com.clara.kafka

import com.expeditelabs.thrift.{events => thriftEvents}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.NullLogger
import com.twitter.util.{Time, Future}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.collection.JavaConversions.mapAsJavaMap

trait PersistedEventSender extends MessageSender[thriftEvents.PersistedEvent]

object NullPersistedEventSender extends PersistedEventSender {
  override def send(topic: String)(message: => thriftEvents.PersistedEvent): Future[Unit] = Future.Unit
  override def close(deadline: Time): Future[Unit] = Future.Unit
}

class KafkaPersistedEventSender(
  bootstrapServers: String,
  statsReceiver: StatsReceiver
) extends PersistedEventSender
  with KafkaSender[String, thriftEvents.PersistedEvent] {

  val requestLogger = NullLogger
  val scopedStats = statsReceiver.scope("kafkaPersistedEvents")

  val keySerializer: Serializer[String] = new StringSerializer
  val valueSerializer: Serializer[thriftEvents.PersistedEvent] = new KafkaThriftPersistedEventSerializer

  val config: Map[String, String] = Map(
    "bootstrap.servers" -> bootstrapServers,
    "client.id" -> "KafkaPersistedEventSender",
    ProducerConfig.MAX_REQUEST_SIZE_CONFIG -> messageSize.toString
  )
  val kafkaProducer = new KafkaProducer[String, thriftEvents.PersistedEvent](config, keySerializer, valueSerializer)
}

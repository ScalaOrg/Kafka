package com.clara.kafka

import java.util.concurrent.TimeUnit

import com.expeditelabs.thrift.{events => thriftEvents}
import com.expeditelabs.util.{JavaFutureConversion, StatsObserver}
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Closable, Future, Time}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Serializer


/** A Message Sender is a closable service that allows us to send
  * a message of a specific type on a topic specified as a string.
  * Kafka has a general mechanism for including a key in addition
  * to the topic and message. We currently support this only to
  * satisfy Kafka Api requirements
  */
trait MessageSender[V] extends Closable {
  def send(topic: String)(message: => V): Future[Unit]

  def send(topic: String, message: V): Future[Unit] = {
    send(topic)(message)
  }
  def batchSend(topic: String)(messages: Seq[V]): Future[Unit] = {
    Future.collect(messages.map(message => send(topic)(message))).unit
  }
}

trait KafkaSender[Key, Message] extends MessageSender[Message]
  with JavaFutureConversion
  with StatsObserver {
  val name = this.getClass.getName

  def send(topic: String)(message: => Message): Future[Unit] = {
    send(new ProducerRecord[Key, Message](topic, message)): Future[Unit]
  }

  def keySerializer: Serializer[Key]
  def valueSerializer: Serializer[Message]
  protected[this] def config: Map[String, String]

  private[this] val log = Logger(this.getClass)
  protected[this] def kafkaProducer: KafkaProducer[Key, Message]

  def close(deadline: Time) = {
    val timeout = (deadline - Time.now).max(Duration(0L, TimeUnit.MILLISECONDS))
    log.info(s"closing kafka sender ${name} with timeout ${timeout.inMillis}")
    kafkaProducer.close(timeout.inMillis, TimeUnit.MILLISECONDS)
    log.info(s"kafka sender ${name} successfully closed")
    Future.Unit
  }

  def send(record: ProducerRecord[Key, Message]): Future[Unit] = {
    observe(s"topics/${record.topic}") {
      val payloadSize = valueSerializer.serialize("", record.value()).length
      scopedStats.scope(s"topics/${record.topic}").stat("sentMessageSize").add(payloadSize.toFloat)
      toTwitterFuture {
        kafkaProducer.send(record)
      }.unit
    }
  }
}

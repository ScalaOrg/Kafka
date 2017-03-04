package com.clara.kafka

import java.util.{Map => JavaMap}

import com.expeditelabs.thrift.{events => thriftEvents}
import com.expeditelabs.util.ThriftWriter
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class KafkaThriftEventSerializer extends Serializer[thriftEvents.EventMessage] {

  override def configure(configs: JavaMap[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: thriftEvents.EventMessage): Array[Byte] = {
    ThriftWriter.toBytes(data)
  }

  override def close(): Unit = ()
}

class KafkaThriftEventDeserializer extends Deserializer[thriftEvents.EventMessage] {
  override def configure(configs: JavaMap[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): thriftEvents.EventMessage = {
    ThriftWriter.fromBytes(data, thriftEvents.EventMessage)
  }

  override def close(): Unit = ()
}

class KafkaThriftPersistedEventSerializer extends Serializer[thriftEvents.PersistedEvent] {

  override def configure(configs: JavaMap[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: thriftEvents.PersistedEvent): Array[Byte] = {
    ThriftWriter.toBytes(data)
  }

  override def close(): Unit = ()
}

class KafkaThriftPersistedEventDeserializer extends Deserializer[thriftEvents.PersistedEvent] {
  override def configure(configs: JavaMap[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): thriftEvents.PersistedEvent = {
    ThriftWriter.fromBytes(data, thriftEvents.PersistedEvent)
  }

  override def close(): Unit = ()
}

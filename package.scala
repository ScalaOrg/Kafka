package com.clara

package object kafka {
  // n.b. This message size constant is the maximum message size in bytes
  // and is used on both producer and consumer sides of the kafka channel.
  // We increased it from the default of 1MB.
  val messageSize = 2097152

  object KafkaTopics {
    val platformEvents        = "platformEvents"
    val eventsGatewayEvents   = "eventsGatewayEvents"
    val eventsBackfill        = "eventsBackfill"
    val checklistEvents       = "checklistEvents"
    val persistedEvents       = "events-eventPersistedToS3"
    val lendingQbEvents       = "lendingQbEvents"
    val bonsaiDashboardEvents = "bonsaiDashboardEvents"
    val docfillServiceEvents  = "docfillServiceEvents"
  }
}

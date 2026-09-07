/**
 * Basic Streamline Kotlin SDK usage example.
 *
 * Prerequisites:
 *   1. Start a Streamline server:  streamline --playground
 *   2. Run this example:           gradle run (or copy into your project)
 *
 * Demonstrates: connecting, producing, consuming, admin operations,
 * and SQL queries.
 */
package io.streamline.examples

import io.streamline.sdk.AdminClient
import io.streamline.sdk.StreamlineClient
import io.streamline.sdk.StreamlineConfiguration
import kotlinx.coroutines.flow.take

suspend fun main() {
    val config =
        StreamlineConfiguration(
            url = "ws://localhost:9092",
            autoReconnect = true,
            maxRetries = 5,
        )
    val admin = AdminClient("http://localhost:9094")

    admin.createTopic("kotlin-demo", partitions = 3)
    println("✓ Created topic 'kotlin-demo'")

    val topics = admin.listTopics()
    println("Topics: ${topics.map { it.name }}")

    val client = StreamlineClient(config)
    client.connect()
    println("✓ Connected to Streamline")

    for (i in 1..5) {
        client.produce(
            topic = "kotlin-demo",
            key = "user-$i",
            value = """{"event":"click","count":$i}""",
        )
    }
    println("✓ Produced 5 messages")

    println("Consuming messages:")
    client.messages("kotlin-demo").take(5).collect { message ->
        println("  topic=${message.topic} key=${message.key} value=${message.value}")
    }

    val result = admin.query("SELECT * FROM `kotlin-demo` LIMIT 3")
    println("Query result: ${result.rowCount} rows, columns: ${result.columns}")

    val groups = admin.listConsumerGroups()
    println("Consumer groups: ${groups.map { it.id }}")

    val info = admin.serverInfo()
    println("Server: v${info.version}, uptime=${info.uptime}s, topics=${info.topicCount}")

    admin.deleteTopic("kotlin-demo")
    client.disconnect()
    client.close()
    admin.close()
    println("✓ Done")
}

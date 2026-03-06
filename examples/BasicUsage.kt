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

import io.streamline.sdk.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.take

suspend fun main() {
    // -- Configuration --
    val config = StreamlineConfiguration(
        url = "ws://localhost:9092",
        autoReconnect = true,
        maxRetries = 5,
    )

    // -- Admin Client (HTTP API) --
    val admin = AdminClient("http://localhost:9094")

    // Create a topic
    admin.createTopic("kotlin-demo", partitions = 3)
    println("✓ Created topic 'kotlin-demo'")

    // List topics
    val topics = admin.listTopics()
    println("Topics: ${topics.map { it.name }}")

    // -- Streaming Client (WebSocket) --
    val client = StreamlineClient(config)
    client.connect()
    println("✓ Connected to Streamline")

    // Produce messages
    for (i in 1..5) {
        client.produce("kotlin-demo", key = "user-$i", value = """{"event":"click","count":$i}""")
    }
    println("✓ Produced 5 messages")

    // Consume messages using Flow
    println("Consuming messages:")
    client.messages("kotlin-demo").take(5).collect { msg ->
        println("  topic=${msg.topic} key=${msg.key} value=${msg.value}")
    }

    // -- SQL Query --
    val result = admin.query("SELECT * FROM `kotlin-demo` LIMIT 3")
    println("Query result: ${result.rowCount} rows, columns: ${result.columns}")

    // -- Consumer Groups --
    val groups = admin.listConsumerGroups()
    println("Consumer groups: ${groups.map { it.id }}")

    // -- Server Info --
    val info = admin.serverInfo()
    println("Server: v${info.version}, uptime=${info.uptime}s, topics=${info.topicCount}")

    // Cleanup
    admin.deleteTopic("kotlin-demo")
    client.disconnect()
    client.close()
    admin.close()
    println("✓ Done")
}

/**
 * Streamline SQL query example.
 */
package io.streamline.examples

import io.streamline.sdk.AdminClient
import io.streamline.sdk.StreamlineClient
import io.streamline.sdk.StreamlineConfiguration
import kotlinx.coroutines.runBlocking

fun main() =
    runBlocking {
        val bootstrap = System.getenv("STREAMLINE_BOOTSTRAP") ?: "localhost:9092"
        val httpUrl = System.getenv("STREAMLINE_HTTP") ?: "http://localhost:9094"
        val wsUrl =
            bootstrap.takeIf { it.startsWith("ws://") || it.startsWith("wss://") }
                ?: "ws://$bootstrap"

        val client = StreamlineClient(StreamlineConfiguration(url = wsUrl))
        val admin = AdminClient(httpUrl)
        client.connect()

        admin.createTopic("events", partitions = 1)
        for (i in 0 until 10) {
            client.produce(
                topic = "events",
                value = """{"user":"user-$i","action":"click","value":${i * 10}}""",
            )
        }
        println("Produced 10 events")

        println("\n--- All events (limit 5) ---")
        val result = admin.query("SELECT * FROM `events` LIMIT 5")
        println("Columns: ${result.columns}")
        println("Rows: ${result.rowCount}")
        result.rows.forEach { row -> println("  $row") }

        println("\n--- Count by action ---")
        val aggregate = admin.query("SELECT action, COUNT(*) as cnt FROM `events` GROUP BY action")
        aggregate.rows.forEach { row -> println("  $row") }

        println("\n--- High-value events ---")
        val filtered = admin.query("SELECT * FROM `events` WHERE value > 50 ORDER BY value DESC")
        println("Found ${filtered.rowCount} high-value events")
        filtered.rows.forEach { row -> println("  $row") }

        client.disconnect()
        client.close()
        admin.close()
        println("\nDone!")
    }

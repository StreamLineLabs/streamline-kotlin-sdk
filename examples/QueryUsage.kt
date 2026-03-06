/**
 * Streamline SQL Query Example (Kotlin)
 *
 * Demonstrates using Streamline's embedded analytics engine (DuckDB)
 * to run SQL queries on streaming data.
 *
 * Prerequisites:
 *   - Streamline server running
 *   - Add streamline-kotlin-sdk dependency
 *
 * Run:
 *   kotlinc -script QueryUsage.kt
 */
import io.streamline.sdk.StreamlineClient
import io.streamline.sdk.AdminClient
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val bootstrap = System.getenv("STREAMLINE_BOOTSTRAP") ?: "localhost:9092"
    val httpUrl = System.getenv("STREAMLINE_HTTP") ?: "http://localhost:9094"

    val client = StreamlineClient(bootstrap)
    val admin = AdminClient(httpUrl)

    // Produce sample data
    admin.createTopic("events", partitions = 1)
    for (i in 0 until 10) {
        client.produce("events", """{"user":"user-$i","action":"click","value":${i * 10}}""")
    }
    println("Produced 10 events")

    // Simple SELECT
    println("\n--- All events (limit 5) ---")
    val result = admin.query("SELECT * FROM `events` LIMIT 5")
    println("Columns: ${result.columns}")
    println("Rows: ${result.rowCount}")
    result.rows.forEach { row -> println("  $row") }

    // Aggregation
    println("\n--- Count by action ---")
    val agg = admin.query("SELECT action, COUNT(*) as cnt FROM `events` GROUP BY action")
    agg.rows.forEach { row -> println("  $row") }

    // Filtered query
    println("\n--- High-value events ---")
    val filtered = admin.query("SELECT * FROM `events` WHERE value > 50 ORDER BY value DESC")
    println("Found ${filtered.rowCount} high-value events")
    filtered.rows.forEach { row -> println("  $row") }

    client.close()
    println("\nDone!")
}

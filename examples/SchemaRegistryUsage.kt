/**
 * Schema Registry usage example for the Streamline Kotlin SDK.
 *
 * Prerequisites:
 *   1. Start a Streamline server with schema registry enabled:
 *      streamline --playground
 *   2. Run this example.
 *
 * Demonstrates: registering schemas, retrieving schemas, checking
 * compatibility, and managing subjects.
 */
package io.streamline.examples

import io.streamline.sdk.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val registry = SchemaRegistryClient("http://localhost:9094")

    // -- Register an Avro schema --
    val avroSchema = """
    {
      "type": "record",
      "name": "UserEvent",
      "namespace": "io.streamline.examples",
      "fields": [
        {"name": "userId", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "timestamp", "type": "long"}
      ]
    }
    """.trimIndent()

    val schemaId = registry.registerSchema("user-events-value", avroSchema, SchemaFormat.AVRO)
    println("✓ Registered schema with ID: $schemaId")

    // -- Retrieve the latest schema --
    val latest = registry.getLatestSchema("user-events-value")
    println("Latest schema: subject=${latest.subject}, version=${latest.version}, type=${latest.schemaType}")

    // -- List all subjects --
    val subjects = registry.listSubjects()
    println("Registered subjects: $subjects")

    // -- List versions for a subject --
    val versions = registry.listVersions("user-events-value")
    println("Versions for user-events-value: $versions")

    // -- Evolve the schema (add optional field) --
    val evolvedSchema = """
    {
      "type": "record",
      "name": "UserEvent",
      "namespace": "io.streamline.examples",
      "fields": [
        {"name": "userId", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "source", "type": ["null", "string"], "default": null}
      ]
    }
    """.trimIndent()

    // Check compatibility before registering
    val compatible = registry.checkCompatibility("user-events-value", evolvedSchema, SchemaFormat.AVRO)
    println("Schema compatible: $compatible")

    if (compatible) {
        val newId = registry.registerSchema("user-events-value", evolvedSchema, SchemaFormat.AVRO)
        println("✓ Registered evolved schema with ID: $newId")
    }

    // -- JSON Schema example --
    val jsonSchema = """{"type":"object","required":["orderId","amount"],"properties":{"orderId":{"type":"string"},"amount":{"type":"number"}}}"""
    val jsonId = registry.registerSchema("orders-value", jsonSchema, SchemaFormat.JSON)
    println("✓ Registered JSON schema with ID: $jsonId")

    // Cleanup
    registry.deleteSubject("user-events-value")
    registry.deleteSubject("orders-value")
    registry.close()
    println("✓ Done")
}


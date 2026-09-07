/**
 * Schema Registry usage example for the Streamline Kotlin SDK.
 */
package io.streamline.examples

import io.streamline.sdk.SchemaFormat
import io.streamline.sdk.SchemaRegistryClient
import kotlinx.coroutines.runBlocking

fun main() =
    runBlocking {
        val registry = SchemaRegistryClient("http://localhost:9094")

        val avroSchema =
            """
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

        val latest = registry.getLatestSchema("user-events-value")
        println("Latest schema: subject=${latest.subject}, version=${latest.version}, type=${latest.schemaType}")

        val subjects = registry.listSubjects()
        println("Registered subjects: $subjects")

        val versions = registry.listVersions("user-events-value")
        println("Versions for user-events-value: $versions")

        val evolvedSchema =
            """
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

        val compatible =
            registry.checkCompatibility(
                "user-events-value",
                evolvedSchema,
                SchemaFormat.AVRO,
            )
        println("Schema compatible: $compatible")

        if (compatible) {
            val newId = registry.registerSchema("user-events-value", evolvedSchema, SchemaFormat.AVRO)
            println("✓ Registered evolved schema with ID: $newId")
        }

        val jsonSchema =
            """
            {
              "type": "object",
              "required": ["orderId", "amount"],
              "properties": {
                "orderId": {"type": "string"},
                "amount": {"type": "number"}
              }
            }
            """.trimIndent()
        val jsonId = registry.registerSchema("orders-value", jsonSchema, SchemaFormat.JSON)
        println("✓ Registered JSON schema with ID: $jsonId")

        registry.deleteSubject("user-events-value")
        registry.deleteSubject("orders-value")
        registry.close()
        println("✓ Done")
    }

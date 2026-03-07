package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * HTTP-based client for the Streamline Schema Registry, which follows the
 * Confluent Schema Registry wire format.
 *
 * ```kotlin
 * val registry = SchemaRegistryClient("http://localhost:9094")
 * val id = registry.registerSchema("events-value", avroSchema, SchemaFormat.AVRO)
 * val latest = registry.getLatestSchema("events-value")
 * registry.close()
 * ```
 */
class SchemaRegistryClient(
    private val baseUrl: String,
    private val authToken: String? = null,
    private val httpClient: HttpClient = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true; encodeDefaults = true })
        }
    },
) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    // -- Schema cache (TTL-based) --

    private data class CacheEntry<T>(val value: T, val expiresAt: Long)

    private val schemaByIdCache = java.util.concurrent.ConcurrentHashMap<Int, CacheEntry<SchemaInfo>>()
    private val latestSchemaCache = java.util.concurrent.ConcurrentHashMap<String, CacheEntry<SchemaInfo>>()
    private val cacheTtlMs: Long = 60_000 // 1 minute default TTL

    private fun <T> getFromCache(cache: java.util.concurrent.ConcurrentHashMap<*, CacheEntry<T>>, key: Any): T? {
        @Suppress("UNCHECKED_CAST")
        val entry = (cache as Map<Any, CacheEntry<T>>)[key] ?: return null
        if (System.currentTimeMillis() > entry.expiresAt) {
            (cache as java.util.concurrent.ConcurrentHashMap<Any, CacheEntry<T>>).remove(key)
            return null
        }
        return entry.value
    }

    /** Clear all cached schemas. */
    fun clearCache() {
        schemaByIdCache.clear()
        latestSchemaCache.clear()
    }

    // -- Schema Registration --

    /**
     * Register a new schema under the given subject.
     *
     * @return The globally unique schema ID assigned by the registry.
     */
    suspend fun registerSchema(subject: String, schema: String, schemaType: SchemaFormat = SchemaFormat.AVRO): Int {
        val body = buildJsonObject {
            put("schema", schema)
            put("schemaType", schemaType.name)
        }
        val response = request(HttpMethod.Post, "/subjects/$subject/versions", json.encodeToString(JsonObject.serializer(), body))
        val obj = json.parseToJsonElement(response).jsonObject
        return obj["id"]?.jsonPrimitive?.int
            ?: throw SchemaRegistryException("Missing 'id' in registration response")
    }

    // -- Schema Retrieval --

    /** Get the latest schema registered under the given subject. */
    suspend fun getLatestSchema(subject: String): SchemaInfo {
        getFromCache(latestSchemaCache, subject)?.let { return it }
        val response = request(HttpMethod.Get, "/subjects/$subject/versions/latest")
        val info = parseSchemaInfo(response)
        latestSchemaCache[subject] = CacheEntry(info, System.currentTimeMillis() + cacheTtlMs)
        schemaByIdCache[info.id] = CacheEntry(info, System.currentTimeMillis() + cacheTtlMs)
        return info
    }

    /** Get a specific version of a schema registered under the given subject. */
    suspend fun getSchemaVersion(subject: String, version: Int): SchemaInfo {
        val response = request(HttpMethod.Get, "/subjects/$subject/versions/$version")
        val info = parseSchemaInfo(response)
        schemaByIdCache[info.id] = CacheEntry(info, System.currentTimeMillis() + cacheTtlMs)
        return info
    }

    /** Get a schema by its globally unique ID. */
    suspend fun getSchemaById(id: Int): SchemaInfo {
        getFromCache(schemaByIdCache, id)?.let { return it }
        val response = request(HttpMethod.Get, "/schemas/ids/$id")
        val info = parseSchemaInfo(response, fallbackId = id)
        schemaByIdCache[id] = CacheEntry(info, System.currentTimeMillis() + cacheTtlMs)
        return info
    }

    // -- Subject Management --

    /** List all registered subjects in the schema registry. */
    suspend fun listSubjects(): List<String> {
        val response = request(HttpMethod.Get, "/subjects")
        return json.parseToJsonElement(response).jsonArray.map { it.jsonPrimitive.content }
    }

    /** List all version numbers registered under the given subject. */
    suspend fun listVersions(subject: String): List<Int> {
        val response = request(HttpMethod.Get, "/subjects/$subject/versions")
        return json.parseToJsonElement(response).jsonArray.map { it.jsonPrimitive.int }
    }

    /** Delete a subject and all its associated schema versions. */
    suspend fun deleteSubject(subject: String): List<Int> {
        val response = request(HttpMethod.Delete, "/subjects/$subject")
        return json.parseToJsonElement(response).jsonArray.map { it.jsonPrimitive.int }
    }

    // -- Compatibility --

    /**
     * Check whether a schema is compatible with the latest version registered
     * under the given subject.
     *
     * @return `true` if the schema is compatible.
     */
    suspend fun checkCompatibility(subject: String, schema: String, schemaType: SchemaFormat = SchemaFormat.AVRO): Boolean {
        val body = buildJsonObject {
            put("schema", schema)
            put("schemaType", schemaType.name)
        }
        val response = request(
            HttpMethod.Post,
            "/compatibility/subjects/$subject/versions/latest",
            json.encodeToString(JsonObject.serializer(), body),
        )
        val obj = json.parseToJsonElement(response).jsonObject
        return obj["is_compatible"]?.jsonPrimitive?.content?.toBoolean() ?: false
    }

    /** Release HTTP client resources. */
    fun close() {
        httpClient.close()
    }

    // -- Internal --

    private fun parseSchemaInfo(responseBody: String, fallbackId: Int? = null): SchemaInfo {
        val obj = json.parseToJsonElement(responseBody).jsonObject
        return SchemaInfo(
            subject = obj["subject"]?.jsonPrimitive?.content ?: "",
            id = obj["id"]?.jsonPrimitive?.int ?: fallbackId ?: 0,
            version = obj["version"]?.jsonPrimitive?.int ?: 0,
            schemaType = obj["schemaType"]?.jsonPrimitive?.content ?: "AVRO",
            schema = obj["schema"]?.jsonPrimitive?.content ?: "",
        )
    }

    private suspend fun request(method: HttpMethod, path: String, body: String? = null): String {
        try {
            val response: HttpResponse = httpClient.request("$baseUrl$path") {
                this.method = method
                authToken?.let { header(HttpHeaders.Authorization, "Bearer $it") }
                if (body != null) {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
            }

            if (!response.status.isSuccess()) {
                val errorBody = response.bodyAsText()
                when (response.status) {
                    HttpStatusCode.NotFound -> throw SchemaRegistryException("Not found: $path")
                    HttpStatusCode.Unauthorized -> throw SchemaRegistryException("Unauthorized: $errorBody")
                    HttpStatusCode.Conflict -> throw SchemaRegistryException("Incompatible schema: $errorBody")
                    else -> throw SchemaRegistryException("HTTP ${response.status.value}: $errorBody")
                }
            }

            return response.bodyAsText()
        } catch (e: SchemaRegistryException) {
            throw e
        } catch (e: Exception) {
            throw SchemaRegistryException("Request failed: ${e.message}", e)
        }
    }
}

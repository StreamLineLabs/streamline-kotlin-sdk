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
import java.util.concurrent.ConcurrentHashMap

/**
 * HTTP-based client for the Streamline Schema Registry, which follows the
 * Confluent Schema Registry wire format.
 *
 * Supports schema caching to reduce redundant network calls and pluggable
 * authentication via [AuthConfig].
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
    private val auth: AuthConfig? = null,
    private val httpClient: HttpClient = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true; encodeDefaults = true })
        }
    },
) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    /** Cache: "subject:version" -> SchemaInfo */
    private val schemaCache = ConcurrentHashMap<String, SchemaInfo>()

    /** Cache: schema ID -> SchemaInfo */
    private val idCache = ConcurrentHashMap<Int, SchemaInfo>()

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
        val response = request(HttpMethod.Get, "/subjects/$subject/versions/latest")
        val info = parseSchemaInfo(response)
        cacheSchema(info)
        return info
    }

    /**
     * Get a specific version of a schema registered under the given subject.
     * Results are cached after the first successful fetch.
     */
    suspend fun getSchema(subject: String, version: Int): SchemaInfo {
        val cacheKey = "$subject:$version"
        schemaCache[cacheKey]?.let { return it }

        val response = request(HttpMethod.Get, "/subjects/$subject/versions/$version")
        val info = parseSchemaInfo(response)
        cacheSchema(info)
        return info
    }

    /**
     * Get a specific version of a schema registered under the given subject.
     * Alias for [getSchema].
     */
    suspend fun getSchemaVersion(subject: String, version: Int): SchemaInfo =
        getSchema(subject, version)

    /** Get a schema by its globally unique ID. */
    suspend fun getSchemaById(id: Int): SchemaInfo {
        idCache[id]?.let { return it }

        val response = request(HttpMethod.Get, "/schemas/ids/$id")
        val info = parseSchemaInfo(response, fallbackId = id)
        cacheSchema(info)
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

    /** Delete a subject and all its associated schema versions. Evicts cached entries. */
    suspend fun deleteSubject(subject: String): List<Int> {
        val response = request(HttpMethod.Delete, "/subjects/$subject")
        // Evict cache entries for this subject
        schemaCache.keys.removeAll { it.startsWith("$subject:") }
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

    /**
     * Get the compatibility level configured for a subject.
     * Falls back to the global default if no subject-level override exists.
     */
    suspend fun getCompatibilityLevel(subject: String): CompatibilityLevel {
        val response = request(HttpMethod.Get, "/config/$subject")
        val obj = json.parseToJsonElement(response).jsonObject
        val level = obj["compatibilityLevel"]?.jsonPrimitive?.content
            ?: throw SchemaRegistryException("Missing 'compatibilityLevel' in response")
        return CompatibilityLevel.valueOf(level)
    }

    /**
     * Set the compatibility level for a subject.
     */
    suspend fun setCompatibilityLevel(subject: String, level: CompatibilityLevel) {
        val body = buildJsonObject {
            put("compatibility", level.name)
        }
        request(
            HttpMethod.Put,
            "/config/$subject",
            json.encodeToString(JsonObject.serializer(), body),
        )
    }

    // -- Cache Management --

    /** Clear all cached schema entries. */
    fun clearCache() {
        schemaCache.clear()
        idCache.clear()
    }

    /** Release HTTP client resources. */
    fun close() {
        httpClient.close()
    }

    // -- Internal --

    private fun cacheSchema(info: SchemaInfo) {
        if (info.subject.isNotEmpty() && info.version > 0) {
            schemaCache["${info.subject}:${info.version}"] = info
        }
        if (info.id > 0) {
            idCache[info.id] = info
        }
    }

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
                // Apply auth: prefer AuthConfig, fall back to legacy authToken
                if (auth != null) {
                    applyAuth(auth)
                } else {
                    authToken?.let { header(HttpHeaders.Authorization, "Bearer $it") }
                }
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

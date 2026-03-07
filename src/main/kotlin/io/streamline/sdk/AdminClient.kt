package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put

/**
 * HTTP-based admin client for managing Streamline server resources.
 *
 * Unlike [StreamlineClient] which uses WebSocket for real-time messaging,
 * the admin client communicates via the HTTP REST API (default port 9094)
 * for topic management, consumer group inspection, and SQL queries.
 *
 * ```kotlin
 * val admin = AdminClient("http://localhost:9094")
 * admin.createTopic("events", partitions = 3)
 * val topics = admin.listTopics()
 * val result = admin.query("SELECT * FROM events LIMIT 10")
 * admin.close()
 * ```
 */
class AdminClient(
    private val baseUrl: String,
    private val authToken: String? = null,
    private val saslConfig: SaslConfig? = null,
    private val httpClient: HttpClient = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true; encodeDefaults = true })
        }
    },
) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    // -- Topic Operations --

    /** List all topics on the server. */
    suspend fun listTopics(): List<TopicInfo> {
        val response = request(HttpMethod.Get, "/v1/topics")
        val body = json.parseToJsonElement(response).jsonArray
        return body.map { element ->
            val obj = element.jsonObject
            TopicInfo(
                name = obj["name"]?.jsonPrimitive?.content ?: "",
                partitions = obj["partitions"]?.jsonPrimitive?.int ?: 1,
                replicationFactor = obj["replication_factor"]?.jsonPrimitive?.int ?: 1,
                messageCount = obj["message_count"]?.jsonPrimitive?.long ?: 0,
            )
        }
    }

    /** Get detailed information about a specific topic. */
    suspend fun describeTopic(name: String): TopicDescription {
        val response = request(HttpMethod.Get, "/v1/topics/$name")
        val obj = json.parseToJsonElement(response).jsonObject
        val configObj = obj["config"]?.jsonObject
        val config = configObj?.mapValues { it.value.jsonPrimitive.content } ?: emptyMap()
        return TopicDescription(
            name = obj["name"]?.jsonPrimitive?.content ?: name,
            partitions = obj["partitions"]?.jsonPrimitive?.int ?: 1,
            replicationFactor = obj["replication_factor"]?.jsonPrimitive?.int ?: 1,
            messageCount = obj["message_count"]?.jsonPrimitive?.long ?: 0,
            config = config,
        )
    }

    /** Create a new topic. */
    suspend fun createTopic(
        name: String,
        partitions: Int = 1,
        replicationFactor: Int = 1,
        config: Map<String, String> = emptyMap(),
    ) {
        val body = buildJsonObject {
            put("name", name)
            put("partitions", partitions)
            put("replication_factor", replicationFactor)
            if (config.isNotEmpty()) {
                put("config", JsonObject(config.mapValues { JsonPrimitive(it.value) }))
            }
        }
        request(HttpMethod.Post, "/v1/topics", json.encodeToString(JsonObject.serializer(), body))
    }

    /** Delete a topic by name. */
    suspend fun deleteTopic(name: String) {
        request(HttpMethod.Delete, "/v1/topics/$name")
    }

    // -- Consumer Group Operations --

    /** List all consumer groups. */
    suspend fun listConsumerGroups(): List<ConsumerGroup> {
        val response = request(HttpMethod.Get, "/v1/consumer-groups")
        val body = json.parseToJsonElement(response).jsonArray
        return body.map { element ->
            val obj = element.jsonObject
            val members = obj["members"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList()
            ConsumerGroup(
                id = obj["id"]?.jsonPrimitive?.content ?: "",
                members = members,
                state = obj["state"]?.jsonPrimitive?.content ?: "unknown",
            )
        }
    }

    /** Get detailed information about a specific consumer group. */
    suspend fun describeConsumerGroup(groupId: String): ConsumerGroupDescription {
        val response = request(HttpMethod.Get, "/v1/consumer-groups/$groupId")
        val obj = json.parseToJsonElement(response).jsonObject
        val members = obj["members"]?.jsonArray?.map { member ->
            val m = member.jsonObject
            ConsumerGroupMember(
                id = m["id"]?.jsonPrimitive?.content ?: "",
                clientId = m["client_id"]?.jsonPrimitive?.content ?: "",
                host = m["host"]?.jsonPrimitive?.content ?: "",
                assignments = m["assignments"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList(),
            )
        } ?: emptyList()

        return ConsumerGroupDescription(
            id = obj["id"]?.jsonPrimitive?.content ?: groupId,
            state = obj["state"]?.jsonPrimitive?.content ?: "unknown",
            members = members,
            protocol = obj["protocol"]?.jsonPrimitive?.content ?: "",
        )
    }

    /** Delete a consumer group. */
    suspend fun deleteConsumerGroup(groupId: String) {
        request(HttpMethod.Delete, "/v1/consumer-groups/$groupId")
    }

    // -- Query Operations --

    /**
     * Execute a SQL query against the streaming data.
     *
     * ```kotlin
     * val result = admin.query("SELECT * FROM events WHERE key = 'user-1' LIMIT 10")
     * result.rows.forEach { row -> println(row) }
     * ```
     */
    suspend fun query(sql: String): QueryResult {
        val body = buildJsonObject { put("query", sql) }
        val response = request(HttpMethod.Post, "/v1/query", json.encodeToString(JsonObject.serializer(), body))
        val obj = json.parseToJsonElement(response).jsonObject
        val columns = obj["columns"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList()
        val rows = obj["rows"]?.jsonArray?.map { row ->
            row.jsonArray.map { it.jsonPrimitive.content }
        } ?: emptyList()
        return QueryResult(
            columns = columns,
            rows = rows,
            rowCount = obj["row_count"]?.jsonPrimitive?.int ?: rows.size,
        )
    }

    // -- Server Info --

    /** Get server health and version information. */
    suspend fun serverInfo(): ServerInfo {
        val response = request(HttpMethod.Get, "/v1/info")
        val obj = json.parseToJsonElement(response).jsonObject
        return ServerInfo(
            version = obj["version"]?.jsonPrimitive?.content ?: "",
            uptime = obj["uptime"]?.jsonPrimitive?.long ?: 0,
            topicCount = obj["topic_count"]?.jsonPrimitive?.int ?: 0,
            messageCount = obj["message_count"]?.jsonPrimitive?.long ?: 0,
        )
    }

    /** Check if the server is healthy. */
    suspend fun isHealthy(): Boolean {
        return try {
            request(HttpMethod.Get, "/health/live")
            true
        } catch (_: Exception) {
            false
        }
    }

    /** Release HTTP client resources. */
    fun close() {
        httpClient.close()
    }

    // -- Internal HTTP --

    private suspend fun request(method: HttpMethod, path: String, body: String? = null): String {
        try {
            val response: HttpResponse = httpClient.request("$baseUrl$path") {
                this.method = method
                // Apply auth: bearer token takes precedence, then SASL credentials
                when {
                    authToken != null -> header(HttpHeaders.Authorization, "Bearer $authToken")
                    saslConfig != null -> {
                        val credentials = java.util.Base64.getEncoder()
                            .encodeToString("${saslConfig.username}:${saslConfig.password}".toByteArray())
                        header(HttpHeaders.Authorization, "Basic $credentials")
                        header("X-Streamline-SASL-Mechanism", saslConfig.mechanism.name)
                    }
                }
                if (body != null) {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
            }

            if (!response.status.isSuccess()) {
                val errorBody = response.bodyAsText()
                when (response.status) {
                    HttpStatusCode.NotFound -> throw TopicNotFoundException(path)
                    HttpStatusCode.Unauthorized -> throw AuthenticationFailedException("Unauthorized: $errorBody")
                    else -> throw AdminOperationException(
                        "HTTP ${response.status.value}: $errorBody"
                    )
                }
            }

            return response.bodyAsText()
        } catch (e: StreamlineException) {
            throw e
        } catch (e: Exception) {
            throw AdminOperationException("Request failed: ${e.message}", e)
        }
    }
}

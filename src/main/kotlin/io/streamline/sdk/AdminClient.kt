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
    private val auth: AuthConfig? = null,
    private val tls: TlsConfig? = null,
    private val httpClient: HttpClient = HttpClient(CIO) {
        engine { configureTls(tls) }
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

    // -- Cluster Operations --

    /** Get cluster overview including broker list and controller info. */
    suspend fun clusterInfo(): ClusterInfo {
        val response = request(HttpMethod.Get, "/v1/cluster")
        val obj = json.parseToJsonElement(response).jsonObject
        val brokers = obj["brokers"]?.jsonArray?.map { b ->
            val bo = b.jsonObject
            BrokerInfo(
                id = bo["id"]?.jsonPrimitive?.int ?: 0,
                host = bo["host"]?.jsonPrimitive?.content ?: "",
                port = bo["port"]?.jsonPrimitive?.int ?: 9092,
                rack = bo["rack"]?.jsonPrimitive?.content,
            )
        } ?: emptyList()
        return ClusterInfo(
            clusterId = obj["cluster_id"]?.jsonPrimitive?.content ?: "",
            brokerId = obj["broker_id"]?.jsonPrimitive?.int ?: 0,
            brokers = brokers,
            controller = obj["controller"]?.jsonPrimitive?.int ?: -1,
        )
    }

    /** List all brokers in the cluster. */
    suspend fun listBrokers(): List<BrokerInfo> {
        return clusterInfo().brokers
    }

    // -- Consumer Group Lag --

    /** Get consumer lag for a specific consumer group. */
    suspend fun consumerGroupLag(groupId: String): ConsumerGroupLag {
        val response = request(HttpMethod.Get, "/v1/consumer-groups/$groupId/lag")
        val obj = json.parseToJsonElement(response).jsonObject
        val partitions = obj["partitions"]?.jsonArray?.map { p ->
            val po = p.jsonObject
            ConsumerLag(
                topic = po["topic"]?.jsonPrimitive?.content ?: "",
                partition = po["partition"]?.jsonPrimitive?.int ?: 0,
                currentOffset = po["current_offset"]?.jsonPrimitive?.long ?: 0,
                endOffset = po["end_offset"]?.jsonPrimitive?.long ?: 0,
                lag = po["lag"]?.jsonPrimitive?.long ?: 0,
            )
        } ?: emptyList()
        return ConsumerGroupLag(
            groupId = obj["group_id"]?.jsonPrimitive?.content ?: groupId,
            partitions = partitions,
            totalLag = obj["total_lag"]?.jsonPrimitive?.long ?: partitions.sumOf { it.lag },
        )
    }

    /** Get consumer lag for a specific topic within a consumer group. */
    suspend fun consumerGroupTopicLag(groupId: String, topic: String): ConsumerGroupLag {
        val response = request(HttpMethod.Get, "/v1/consumer-groups/$groupId/lag/$topic")
        val obj = json.parseToJsonElement(response).jsonObject
        val partitions = obj["partitions"]?.jsonArray?.map { p ->
            val po = p.jsonObject
            ConsumerLag(
                topic = po["topic"]?.jsonPrimitive?.content ?: topic,
                partition = po["partition"]?.jsonPrimitive?.int ?: 0,
                currentOffset = po["current_offset"]?.jsonPrimitive?.long ?: 0,
                endOffset = po["end_offset"]?.jsonPrimitive?.long ?: 0,
                lag = po["lag"]?.jsonPrimitive?.long ?: 0,
            )
        } ?: emptyList()
        return ConsumerGroupLag(
            groupId = obj["group_id"]?.jsonPrimitive?.content ?: groupId,
            partitions = partitions,
            totalLag = obj["total_lag"]?.jsonPrimitive?.long ?: partitions.sumOf { it.lag },
        )
    }

    /** Reset consumer group offsets (dry run — returns what would change). */
    suspend fun resetOffsetsDryRun(
        groupId: String,
        topic: String,
        strategy: String = "earliest",
    ): List<ConsumerLag> {
        val body = buildJsonObject {
            put("topic", topic)
            put("strategy", strategy)
        }
        val response = request(
            HttpMethod.Post,
            "/v1/consumer-groups/$groupId/reset-offsets/dry-run",
            json.encodeToString(JsonObject.serializer(), body),
        )
        val arr = json.parseToJsonElement(response).jsonArray
        return arr.map { p ->
            val po = p.jsonObject
            ConsumerLag(
                topic = po["topic"]?.jsonPrimitive?.content ?: topic,
                partition = po["partition"]?.jsonPrimitive?.int ?: 0,
                currentOffset = po["current_offset"]?.jsonPrimitive?.long ?: 0,
                endOffset = po["end_offset"]?.jsonPrimitive?.long ?: 0,
                lag = po["lag"]?.jsonPrimitive?.long ?: 0,
            )
        }
    }

    /** Reset consumer group offsets (executes the reset). */
    suspend fun resetOffsets(
        groupId: String,
        topic: String,
        strategy: String = "earliest",
    ) {
        val body = buildJsonObject {
            put("topic", topic)
            put("strategy", strategy)
        }
        request(
            HttpMethod.Post,
            "/v1/consumer-groups/$groupId/reset-offsets",
            json.encodeToString(JsonObject.serializer(), body),
        )
    }

    // -- Message Inspection --

    // -- ACL Management --

    /** List all ACL entries, optionally filtered by resource type. */
    suspend fun listAcls(resourceType: AclResourceType? = null): List<AclEntry> {
        val path = if (resourceType != null) {
            "/v1/acls?resourceType=${resourceType.name.lowercase()}"
        } else {
            "/v1/acls"
        }
        val response = request(HttpMethod.Get, path)
        val arr = json.parseToJsonElement(response).jsonArray
        return arr.map { e ->
            val obj = e.jsonObject
            AclEntry(
                principal = obj["principal"]?.jsonPrimitive?.content ?: "",
                resourceType = obj["resource_type"]?.jsonPrimitive?.content ?: "",
                resourceName = obj["resource_name"]?.jsonPrimitive?.content ?: "",
                operation = obj["operation"]?.jsonPrimitive?.content ?: "",
                permission = obj["permission"]?.jsonPrimitive?.content ?: "",
                host = obj["host"]?.jsonPrimitive?.content ?: "*",
            )
        }
    }

    /** Create a new ACL entry. */
    suspend fun createAcl(
        principal: String,
        resourceType: AclResourceType,
        resourceName: String,
        operation: AclOperation,
        permission: AclPermission = AclPermission.ALLOW,
        host: String = "*",
    ) {
        val body = buildJsonObject {
            put("principal", principal)
            put("resource_type", resourceType.name.lowercase())
            put("resource_name", resourceName)
            put("operation", operation.name.lowercase())
            put("permission", permission.name.lowercase())
            put("host", host)
        }
        request(HttpMethod.Post, "/v1/acls", json.encodeToString(JsonObject.serializer(), body))
    }

    /** Delete ACL entries matching the given filter criteria. */
    suspend fun deleteAcl(
        principal: String,
        resourceType: AclResourceType,
        resourceName: String,
        operation: AclOperation,
    ) {
        val params = "?principal=$principal&resource_type=${resourceType.name.lowercase()}" +
            "&resource_name=$resourceName&operation=${operation.name.lowercase()}"
        request(HttpMethod.Delete, "/v1/acls$params")
    }

    // -- Message Inspection (continued) --

    /** Browse messages from a topic partition. */
    suspend fun inspectMessages(
        topic: String,
        partition: Int = 0,
        offset: Long? = null,
        limit: Int = 20,
    ): List<InspectedMessage> {
        val params = buildString {
            append("?partition=$partition&limit=$limit")
            if (offset != null) append("&offset=$offset")
        }
        val response = request(HttpMethod.Get, "/v1/inspect/$topic$params")
        val arr = json.parseToJsonElement(response).jsonArray
        return arr.map { m ->
            val mo = m.jsonObject
            val headers = mo["headers"]?.jsonObject
                ?.mapValues { it.value.jsonPrimitive.content } ?: emptyMap()
            InspectedMessage(
                offset = mo["offset"]?.jsonPrimitive?.long ?: 0,
                key = mo["key"]?.jsonPrimitive?.content,
                value = mo["value"]?.jsonPrimitive?.content ?: "",
                timestamp = mo["timestamp"]?.jsonPrimitive?.long ?: 0,
                partition = mo["partition"]?.jsonPrimitive?.int ?: partition,
                headers = headers,
            )
        }
    }

    /** Get the latest messages from a topic. */
    suspend fun latestMessages(topic: String, count: Int = 10): List<InspectedMessage> {
        val response = request(HttpMethod.Get, "/v1/inspect/$topic/latest?count=$count")
        val arr = json.parseToJsonElement(response).jsonArray
        return arr.map { m ->
            val mo = m.jsonObject
            InspectedMessage(
                offset = mo["offset"]?.jsonPrimitive?.long ?: 0,
                key = mo["key"]?.jsonPrimitive?.content,
                value = mo["value"]?.jsonPrimitive?.content ?: "",
                timestamp = mo["timestamp"]?.jsonPrimitive?.long ?: 0,
                partition = mo["partition"]?.jsonPrimitive?.int ?: 0,
                headers = mo["headers"]?.jsonObject
                    ?.mapValues { it.value.jsonPrimitive.content } ?: emptyMap(),
            )
        }
    }

    // -- Metrics --

    /** Get metrics history from the server. */
    suspend fun metricsHistory(): List<MetricPoint> {
        val response = request(HttpMethod.Get, "/v1/metrics/history")
        val arr = json.parseToJsonElement(response).jsonArray
        return arr.map { m ->
            val mo = m.jsonObject
            MetricPoint(
                name = mo["name"]?.jsonPrimitive?.content ?: "",
                value = mo["value"]?.jsonPrimitive?.content?.toDoubleOrNull() ?: 0.0,
                labels = mo["labels"]?.jsonObject
                    ?.mapValues { it.value.jsonPrimitive.content } ?: emptyMap(),
                timestamp = mo["timestamp"]?.jsonPrimitive?.long ?: 0,
            )
        }
    }

    // -- HTTP Offset Management --

    /**
     * Commit consumer offsets via HTTP REST API.
     *
     * @param groupId Consumer group identifier.
     * @param offsets Map of "topic:partition" to offset values to commit.
     */
    suspend fun commitOffsets(groupId: String, offsets: Map<String, Long>) {
        val entries = offsets.entries.map { (tp, offset) ->
            val parts = tp.split(":")
            buildJsonObject {
                put("topic", parts.getOrElse(0) { tp })
                put("partition", parts.getOrElse(1) { "0" }.toIntOrNull() ?: 0)
                put("offset", offset)
            }
        }
        val body = buildJsonObject {
            put("offsets", JsonArray(entries))
        }
        request(
            HttpMethod.Post,
            "/v1/consumer-groups/$groupId/offsets",
            json.encodeToString(JsonObject.serializer(), body),
        )
    }

    /**
     * Fetch committed offsets for a consumer group via HTTP REST API.
     *
     * @param groupId Consumer group identifier.
     * @param topic Optional topic filter. If null, returns offsets for all topics.
     * @return Map of "topic:partition" to committed offset.
     */
    suspend fun getOffsets(groupId: String, topic: String? = null): Map<String, Long> {
        val path = if (topic != null) {
            "/v1/consumer-groups/$groupId/offsets?topic=$topic"
        } else {
            "/v1/consumer-groups/$groupId/offsets"
        }
        val response = request(HttpMethod.Get, path)
        val arr = json.parseToJsonElement(response).jsonArray
        val result = mutableMapOf<String, Long>()
        for (element in arr) {
            val obj = element.jsonObject
            val t = obj["topic"]?.jsonPrimitive?.content ?: continue
            val p = obj["partition"]?.jsonPrimitive?.int ?: 0
            val o = obj["offset"]?.jsonPrimitive?.long ?: continue
            result["$t:$p"] = o
        }
        return result
    }

    // -- Partition Reassignment --

    /**
     * Reassign partitions to specific brokers.
     *
     * @param assignments Map of "topic:partition" to list of broker IDs.
     */
    suspend fun reassignPartitions(assignments: Map<String, List<Int>>) {
        val entries = assignments.entries.map { (tp, brokers) ->
            val parts = tp.split(":")
            buildJsonObject {
                put("topic", parts.getOrElse(0) { tp })
                put("partition", parts.getOrElse(1) { "0" }.toIntOrNull() ?: 0)
                put("replicas", JsonArray(brokers.map { JsonPrimitive(it) }))
            }
        }
        val body = buildJsonObject {
            put("reassignments", JsonArray(entries))
        }
        request(
            HttpMethod.Post,
            "/v1/partitions/reassign",
            json.encodeToString(JsonObject.serializer(), body),
        )
    }

    /**
     * List in-progress partition reassignments.
     *
     * @return List of reassignment status entries.
     */
    suspend fun listReassignments(): List<ReassignmentStatus> {
        val response = request(HttpMethod.Get, "/v1/partitions/reassignments")
        val arr = json.parseToJsonElement(response).jsonArray
        return arr.map { element ->
            val obj = element.jsonObject
            ReassignmentStatus(
                topic = obj["topic"]?.jsonPrimitive?.content ?: "",
                partition = obj["partition"]?.jsonPrimitive?.int ?: 0,
                replicas = obj["replicas"]?.jsonArray?.map { it.jsonPrimitive.int } ?: emptyList(),
                addingReplicas = obj["adding_replicas"]?.jsonArray?.map { it.jsonPrimitive.int } ?: emptyList(),
                removingReplicas = obj["removing_replicas"]?.jsonArray?.map { it.jsonPrimitive.int } ?: emptyList(),
            )
        }
    }

    // -- Search --

    /**
     * Searches a topic using semantic search via the HTTP API.
     *
     * @param topic Topic to search.
     * @param query Free-text search query.
     * @param k Maximum number of results (default 10).
     * @return List of search results ordered by descending score.
     */
    suspend fun search(topic: String, query: String, k: Int = 10): List<SearchResult> {
        val payload = buildJsonObject {
            put("query", query)
            put("k", k)
        }
        val responseText = request(HttpMethod.Post, "/api/v1/topics/$topic/search", payload.toString())
        val parsed = json.decodeFromString<SearchApiResponse>(responseText)
        return parsed.hits
    }

    // -- Internal HTTP --

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

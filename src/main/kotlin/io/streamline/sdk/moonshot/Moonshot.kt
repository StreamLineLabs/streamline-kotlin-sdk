package io.streamline.sdk.moonshot

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.net.URLEncoder
import java.util.Base64

/**
 * Exception thrown by Streamline moonshot HTTP clients.
 *
 * @property status HTTP status code, or `0` for transport / decode errors.
 * @property body   Raw response body, when available.
 */
class MoonshotException(
    message: String,
    val status: Int = 0,
    val body: String? = null,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

/**
 * Common configuration for moonshot HTTP clients.
 *
 * @property httpUrl    Base URL of the broker HTTP API (e.g. `http://localhost:9094`).
 * @property authToken  Optional bearer token sent on every request.
 * @property httpClient Optional shared Ktor [HttpClient]. A default CIO-backed
 *                     client is created when null.
 */
data class MoonshotOptions(
    val httpUrl: String,
    val authToken: String? = null,
    val httpClient: HttpClient? = null,
)

abstract class MoonshotHttpBase(opts: MoonshotOptions) : AutoCloseable {
    protected val baseUrl: String = opts.httpUrl.trimEnd('/')
    protected val authToken: String? = opts.authToken
    private val ownsClient: Boolean = opts.httpClient == null
    protected val client: HttpClient = opts.httpClient ?: HttpClient(CIO)
    protected val json: Json = Json { ignoreUnknownKeys = true; encodeDefaults = false }

    protected suspend fun request(
        method: HttpMethod,
        path: String,
        body: String? = null,
    ): String {
        val response: HttpResponse = try {
            client.request("$baseUrl$path") {
                this.method = method
                authToken?.let { header("Authorization", "Bearer $it") }
                if (body != null) {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
            }
        } catch (e: MoonshotException) {
            throw e
        } catch (e: Exception) {
            throw MoonshotException("transport error: ${e.message}", cause = e)
        }
        val text = response.bodyAsText()
        if (!response.status.isSuccess()) {
            throw MoonshotException(
                "moonshot HTTP ${response.status.value}: $text",
                status = response.status.value,
                body = text,
            )
        }
        return text
    }

    protected fun encode(v: String): String =
        URLEncoder.encode(v, Charsets.UTF_8).replace("+", "%20")

    protected fun parseJson(s: String): JsonElement = json.parseToJsonElement(s)

    override fun close() {
        if (ownsClient) client.close()
    }
}

// ---------------- M5 Branches ----------------

data class Branch(
    val name: String,
    val parent: String?,
    val createdAtMs: Long,
)

data class MergeReport(
    val merged: Int,
    val conflicts: List<String>,
)

class BranchAdminClient(opts: MoonshotOptions) : MoonshotHttpBase(opts) {

    suspend fun listBranches(): List<Branch> {
        val text = request(HttpMethod.Get, "/api/v1/branches")
        val root = parseJson(text).jsonObject
        val arr = root["branches"]?.jsonArray ?: JsonArray(emptyList())
        return arr.map { it.jsonObject.toBranch() }
    }

    suspend fun createBranch(name: String, parent: String? = null): Branch {
        require(name.isNotBlank()) { "branch name required" }
        val body = buildJsonObject {
            put("name", name)
            if (parent != null) put("parent", parent)
        }
        val text = request(HttpMethod.Post, "/api/v1/branches", json.encodeToString(JsonObject.serializer(), body))
        return parseJson(text).jsonObject.toBranch()
    }

    suspend fun deleteBranch(name: String) {
        require(name.isNotBlank()) { "branch name required" }
        request(HttpMethod.Delete, "/api/v1/branches/${encode(name)}")
    }

    suspend fun mergeBranch(source: String, target: String): MergeReport {
        require(source.isNotBlank() && target.isNotBlank()) { "source/target required" }
        val body = buildJsonObject {
            put("source", source)
            put("target", target)
        }
        val text = request(HttpMethod.Post, "/api/v1/branches/merge", json.encodeToString(JsonObject.serializer(), body))
        val obj = parseJson(text).jsonObject
        val merged = obj["merged"]?.jsonPrimitive?.long?.toInt() ?: 0
        val conflicts = obj["conflicts"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull ?: "" } ?: emptyList()
        return MergeReport(merged, conflicts)
    }

    private fun JsonObject.toBranch(): Branch = Branch(
        name = this["name"]?.jsonPrimitive?.contentOrNull ?: "",
        parent = this["parent"]?.jsonPrimitive?.contentOrNull,
        createdAtMs = this["created_at_ms"]?.jsonPrimitive?.long ?: 0L,
    )
}

// ---------------- M4 Contracts ----------------

data class ContractValidationResult(
    val valid: Boolean,
    val errors: List<String>,
    val warnings: List<String>,
)

class ContractsClient(opts: MoonshotOptions) : MoonshotHttpBase(opts) {

    suspend fun registerContract(contract: JsonObject): JsonObject {
        val text = request(HttpMethod.Post, "/api/v1/contracts", json.encodeToString(JsonObject.serializer(), contract))
        return parseJson(text).jsonObject
    }

    suspend fun getContract(name: String): JsonObject {
        require(name.isNotBlank()) { "contract name required" }
        val text = request(HttpMethod.Get, "/api/v1/contracts/${encode(name)}")
        return parseJson(text).jsonObject
    }

    /**
     * Validate a payload against a contract. Returns a result whether the
     * server returns 200 (valid) or 400 (invalid).
     */
    suspend fun validate(name: String, payload: JsonElement): ContractValidationResult {
        require(name.isNotBlank()) { "contract name required" }
        val body = buildJsonObject {
            put("contract", name)
            put("payload", payload)
        }
        val response: HttpResponse = try {
            client.request("$baseUrl/api/v1/contracts/validate") {
                this.method = HttpMethod.Post
                authToken?.let { header("Authorization", "Bearer $it") }
                contentType(ContentType.Application.Json)
                setBody(json.encodeToString(JsonObject.serializer(), body))
            }
        } catch (e: Exception) {
            throw MoonshotException("transport error: ${e.message}", cause = e)
        }
        val text = response.bodyAsText()
        if (response.status != HttpStatusCode.OK && response.status != HttpStatusCode.BadRequest) {
            throw MoonshotException(
                "moonshot HTTP ${response.status.value}: $text",
                status = response.status.value,
                body = text,
            )
        }
        val obj = parseJson(text).jsonObject
        return ContractValidationResult(
            valid = obj["valid"]?.jsonPrimitive?.booleanOrNull ?: (response.status == HttpStatusCode.OK),
            errors = obj["errors"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull ?: "" } ?: emptyList(),
            warnings = obj["warnings"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull ?: "" } ?: emptyList(),
        )
    }
}

// ---------------- M4 Attestation ----------------

data class Attestation(
    val keyId: String,
    val algorithm: String,
    val signature: String,
    val payloadHash: String,
)

/**
 * Client for the broker-managed attestation/signing service. The broker holds
 * private keys; this client submits payloads for signing and submits
 * attestations for verification.
 */
class AttestationClient(
    opts: MoonshotOptions,
    private val defaultKeyId: String = "broker-0",
    private val defaultAlgorithm: String = "ed25519",
) : MoonshotHttpBase(opts) {

    suspend fun sign(payload: ByteArray, keyId: String? = null, algorithm: String? = null): Attestation {
        val body = buildJsonObject {
            put("key_id", keyId ?: defaultKeyId)
            put("algorithm", algorithm ?: defaultAlgorithm)
            put("payload_b64", Base64.getEncoder().encodeToString(payload))
        }
        val text = request(HttpMethod.Post, "/api/v1/attest/sign", json.encodeToString(JsonObject.serializer(), body))
        val o = parseJson(text).jsonObject
        return Attestation(
            keyId = o["key_id"]?.jsonPrimitive?.contentOrNull ?: keyId ?: defaultKeyId,
            algorithm = o["algorithm"]?.jsonPrimitive?.contentOrNull ?: algorithm ?: defaultAlgorithm,
            signature = o["signature"]?.jsonPrimitive?.contentOrNull ?: "",
            payloadHash = o["payload_hash"]?.jsonPrimitive?.contentOrNull ?: "",
        )
    }

    suspend fun verify(payload: ByteArray, attestation: Attestation): Boolean {
        val body = buildJsonObject {
            put("key_id", attestation.keyId)
            put("algorithm", attestation.algorithm)
            put("signature", attestation.signature)
            put("payload_b64", Base64.getEncoder().encodeToString(payload))
        }
        val text = request(HttpMethod.Post, "/api/v1/attest/verify", json.encodeToString(JsonObject.serializer(), body))
        val o = parseJson(text).jsonObject
        return o["valid"]?.jsonPrimitive?.booleanOrNull ?: false
    }
}

// ---------------- M2 Semantic Search ----------------

data class SearchHit(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val score: Double,
    val snippet: String?,
)

class SemanticSearchClient(opts: MoonshotOptions) : MoonshotHttpBase(opts) {

    suspend fun search(topic: String, query: String, k: Int = 10): List<SearchHit> {
        require(topic.isNotBlank()) { "topic required" }
        require(query.isNotBlank()) { "query required" }
        require(k > 0) { "k must be > 0" }
        val body = buildJsonObject {
            put("topic", topic)
            put("query", query)
            put("k", k)
        }
        val text = request(HttpMethod.Post, "/api/v1/search", json.encodeToString(JsonObject.serializer(), body))
        val obj = parseJson(text).jsonObject
        val arr = obj["hits"]?.jsonArray ?: JsonArray(emptyList())
        return arr.map {
            val h = it.jsonObject
            SearchHit(
                topic = h["topic"]?.jsonPrimitive?.contentOrNull ?: topic,
                partition = h["partition"]?.jsonPrimitive?.long?.toInt() ?: 0,
                offset = h["offset"]?.jsonPrimitive?.long ?: 0L,
                score = h["score"]?.jsonPrimitive?.doubleOrNull ?: 0.0,
                snippet = h["snippet"]?.jsonPrimitive?.contentOrNull,
            )
        }
    }
}

// ---------------- M1 Memory ----------------

enum class MemoryKind(val wire: String) {
    OBSERVATION("observation"),
    FACT("fact"),
    PROCEDURE("procedure"),
}

data class MemoryRecord(
    val agent: String,
    val kind: MemoryKind,
    val text: String,
    val tags: List<String> = emptyList(),
    val timestampMs: Long = 0L,
)

class MemoryClient(opts: MoonshotOptions) : MoonshotHttpBase(opts) {

    suspend fun remember(agent: String, kind: MemoryKind, text: String, tags: List<String> = emptyList()) {
        require(agent.isNotBlank()) { "agent required" }
        require(text.isNotBlank()) { "text required" }
        val body = buildJsonObject {
            put("agent", agent)
            put("kind", kind.wire)
            put("text", text)
            put("tags", buildJsonArray { tags.forEach { add(JsonPrimitive(it)) } })
        }
        request(HttpMethod.Post, "/api/v1/memory", json.encodeToString(JsonObject.serializer(), body))
    }

    suspend fun recall(agent: String, query: String, k: Int = 5): List<MemoryRecord> {
        require(agent.isNotBlank()) { "agent required" }
        require(query.isNotBlank()) { "query required" }
        require(k > 0) { "k must be > 0" }
        val body = buildJsonObject {
            put("agent", agent)
            put("query", query)
            put("k", k)
        }
        val text = request(HttpMethod.Post, "/api/v1/memory/recall", json.encodeToString(JsonObject.serializer(), body))
        val arr = parseJson(text).jsonObject["memories"]?.jsonArray ?: JsonArray(emptyList())
        return arr.map {
            val o = it.jsonObject
            MemoryRecord(
                agent = o["agent"]?.jsonPrimitive?.contentOrNull ?: agent,
                kind = parseKind(o["kind"]?.jsonPrimitive?.contentOrNull),
                text = o["text"]?.jsonPrimitive?.contentOrNull ?: "",
                tags = o["tags"]?.jsonArray?.map { t -> t.jsonPrimitive.contentOrNull ?: "" } ?: emptyList(),
                timestampMs = o["timestamp_ms"]?.jsonPrimitive?.long ?: 0L,
            )
        }
    }

    private fun parseKind(s: String?): MemoryKind = when (s) {
        "fact" -> MemoryKind.FACT
        "procedure" -> MemoryKind.PROCEDURE
        else -> MemoryKind.OBSERVATION
    }
}

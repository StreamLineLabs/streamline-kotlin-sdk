package io.streamline.sdk.moonshot

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MoonshotClientsTest {

    private fun mockClient(handler: suspend MockRequestHandleScope.(HttpRequestData) -> io.ktor.client.request.HttpResponseData): HttpClient {
        return HttpClient(MockEngine { request -> handler(this, request) })
    }

    private fun jsonOK(body: String): suspend MockRequestHandleScope.(HttpRequestData) -> io.ktor.client.request.HttpResponseData = {
        respond(
            content = ByteReadChannel(body),
            status = HttpStatusCode.OK,
            headers = headersOf(HttpHeaders.ContentType, "application/json"),
        )
    }

    @Test
    fun branchesListAndCreate() = runTest {
        val captured = mutableListOf<HttpRequestData>()
        val client = mockClient { req ->
            captured += req
            when (req.url.encodedPath) {
                "/api/v1/branches" -> when (req.method) {
                    HttpMethod.Get -> respond(
                        ByteReadChannel("""{"branches":[{"name":"main","parent":null,"created_at_ms":1},{"name":"f","parent":"main","created_at_ms":2}]}"""),
                        HttpStatusCode.OK,
                        headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                    HttpMethod.Post -> respond(
                        ByteReadChannel("""{"name":"newb","parent":"main","created_at_ms":99}"""),
                        HttpStatusCode.Created,
                        headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                    else -> error("unexpected ${req.method}")
                }
                else -> error("unexpected path ${req.url.encodedPath}")
            }
        }
        val admin = BranchAdminClient(MoonshotOptions("http://h", httpClient = client))
        val list = admin.listBranches()
        assertEquals(2, list.size)
        assertEquals("main", list[0].name)
        assertEquals("main", list[1].parent)

        val created = admin.createBranch("newb", "main")
        assertEquals("newb", created.name)
        assertEquals("main", created.parent)
        assertEquals(99L, created.createdAtMs)
        admin.close()
    }

    @Test
    fun branchesDeleteAndMerge() = runTest {
        val client = mockClient { req ->
            when {
                req.url.encodedPath == "/api/v1/branches/feature%2Fa" && req.method == HttpMethod.Delete ->
                    respond(ByteReadChannel(""), HttpStatusCode.NoContent)
                req.url.encodedPath == "/api/v1/branches/merge" && req.method == HttpMethod.Post ->
                    respond(
                        ByteReadChannel("""{"merged":7,"conflicts":["topic-a"]}"""),
                        HttpStatusCode.OK,
                        headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                else -> error("unexpected ${req.method} ${req.url.encodedPath}")
            }
        }
        val admin = BranchAdminClient(MoonshotOptions("http://h", httpClient = client))
        admin.deleteBranch("feature/a")
        val report = admin.mergeBranch("feature/a", "main")
        assertEquals(7, report.merged)
        assertEquals(listOf("topic-a"), report.conflicts)
        admin.close()
    }

    @Test
    fun branchesEmptyNameThrows() = runTest {
        val admin = BranchAdminClient(MoonshotOptions("http://h", httpClient = HttpClient(MockEngine { error("unreachable") })))
        assertFailsWith<IllegalArgumentException> { admin.createBranch("") }
        assertFailsWith<IllegalArgumentException> { admin.deleteBranch("") }
        admin.close()
    }

    @Test
    fun contractsValidateValid() = runTest {
        val client = mockClient { req ->
            assertEquals("/api/v1/contracts/validate", req.url.encodedPath)
            respond(
                ByteReadChannel("""{"valid":true,"errors":[],"warnings":["w1"]}"""),
                HttpStatusCode.OK,
                headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val c = ContractsClient(MoonshotOptions("http://h", httpClient = client))
        val r = c.validate("orders", buildJsonObject { put("foo", 1) })
        assertTrue(r.valid)
        assertEquals(listOf("w1"), r.warnings)
        c.close()
    }

    @Test
    fun contractsValidateInvalidStatus400() = runTest {
        val client = mockClient {
            respond(
                ByteReadChannel("""{"valid":false,"errors":["bad"]}"""),
                HttpStatusCode.BadRequest,
                headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val c = ContractsClient(MoonshotOptions("http://h", httpClient = client))
        val r = c.validate("orders", JsonPrimitive("x"))
        assertFalse(r.valid)
        assertEquals(listOf("bad"), r.errors)
        c.close()
    }

    @Test
    fun contractsValidate500Throws() = runTest {
        val client = mockClient {
            respond(ByteReadChannel("nope"), HttpStatusCode.InternalServerError)
        }
        val c = ContractsClient(MoonshotOptions("http://h", httpClient = client))
        val ex = assertFailsWith<MoonshotException> { c.validate("x", JsonPrimitive(1)) }
        assertEquals(500, ex.status)
        c.close()
    }

    @Test
    fun contractsRegisterAndGet() = runTest {
        val client = mockClient { req ->
            when (req.url.encodedPath) {
                "/api/v1/contracts" -> respond(
                    ByteReadChannel("""{"name":"orders","version":1}"""),
                    HttpStatusCode.OK,
                    headersOf(HttpHeaders.ContentType, "application/json"),
                )
                "/api/v1/contracts/orders" -> respond(
                    ByteReadChannel("""{"name":"orders","version":2}"""),
                    HttpStatusCode.OK,
                    headersOf(HttpHeaders.ContentType, "application/json"),
                )
                else -> error("unexpected ${req.url.encodedPath}")
            }
        }
        val c = ContractsClient(MoonshotOptions("http://h", httpClient = client))
        val reg = c.registerContract(buildJsonObject { put("name", "orders") })
        assertEquals(1L, (reg["version"] as JsonPrimitive).content.toLong())
        val got = c.getContract("orders")
        assertEquals("orders", (got["name"] as JsonPrimitive).content)
        c.close()
    }

    @Test
    fun attestationSignAndVerify() = runTest {
        val client = mockClient { req ->
            when (req.url.encodedPath) {
                "/api/v1/attest/sign" -> respond(
                    ByteReadChannel("""{"key_id":"k","algorithm":"ed25519","signature":"sig","payload_hash":"h"}"""),
                    HttpStatusCode.OK,
                    headersOf(HttpHeaders.ContentType, "application/json"),
                )
                "/api/v1/attest/verify" -> respond(
                    ByteReadChannel("""{"valid":true}"""),
                    HttpStatusCode.OK,
                    headersOf(HttpHeaders.ContentType, "application/json"),
                )
                else -> error("unexpected ${req.url.encodedPath}")
            }
        }
        val a = AttestationClient(MoonshotOptions("http://h", httpClient = client), defaultKeyId = "k")
        val payload = "hello".toByteArray()
        val att = a.sign(payload)
        assertEquals("k", att.keyId)
        assertEquals("sig", att.signature)
        // ensure base64 encoding works
        Base64.getDecoder().decode(Base64.getEncoder().encodeToString(payload))
        assertTrue(a.verify(payload, att))
        a.close()
    }

    @Test
    fun searchOK() = runTest {
        val client = mockClient { _ ->
            respond(
                ByteReadChannel("""{"hits":[{"topic":"t","partition":1,"offset":42,"score":0.91,"snippet":"s"}]}"""),
                HttpStatusCode.OK,
                headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val s = SemanticSearchClient(MoonshotOptions("http://h", httpClient = client))
        val hits = s.search("t", "q", k = 3)
        assertEquals(1, hits.size)
        assertEquals("t", hits[0].topic)
        assertEquals(1, hits[0].partition)
        assertEquals(42L, hits[0].offset)
        assertEquals(0.91, hits[0].score)
        assertEquals("s", hits[0].snippet)
        s.close()
    }

    @Test
    fun searchInvalidArgs() = runTest {
        val s = SemanticSearchClient(MoonshotOptions("http://h", httpClient = HttpClient(MockEngine { error("never") })))
        assertFailsWith<IllegalArgumentException> { s.search("", "q") }
        assertFailsWith<IllegalArgumentException> { s.search("t", "") }
        assertFailsWith<IllegalArgumentException> { s.search("t", "q", k = 0) }
        s.close()
    }

    @Test
    fun memoryRememberAndRecall() = runTest {
        val client = mockClient { req ->
            when (req.url.encodedPath) {
                "/api/v1/memory" -> respond(ByteReadChannel("""{}"""), HttpStatusCode.OK, headersOf(HttpHeaders.ContentType, "application/json"))
                "/api/v1/memory/recall" -> respond(
                    ByteReadChannel("""{"memories":[{"agent":"a","kind":"fact","text":"x","tags":["t1"],"timestamp_ms":1}]}"""),
                    HttpStatusCode.OK,
                    headersOf(HttpHeaders.ContentType, "application/json"),
                )
                else -> error("unexpected ${req.url.encodedPath}")
            }
        }
        val m = MemoryClient(MoonshotOptions("http://h", httpClient = client))
        m.remember("a", MemoryKind.OBSERVATION, "saw x")
        val recs = m.recall("a", "x")
        assertEquals(1, recs.size)
        assertEquals(MemoryKind.FACT, recs[0].kind)
        assertEquals(listOf("t1"), recs[0].tags)
        m.close()
    }

    @Test
    fun memoryInvalidArgs() = runTest {
        val m = MemoryClient(MoonshotOptions("http://h", httpClient = HttpClient(MockEngine { error("never") })))
        assertFailsWith<IllegalArgumentException> { m.remember("", MemoryKind.FACT, "x") }
        assertFailsWith<IllegalArgumentException> { m.remember("a", MemoryKind.FACT, "") }
        assertFailsWith<IllegalArgumentException> { m.recall("", "q") }
        assertFailsWith<IllegalArgumentException> { m.recall("a", "") }
        assertFailsWith<IllegalArgumentException> { m.recall("a", "q", k = 0) }
        m.close()
    }

    @Test
    fun authTokenHeaderIncluded() = runTest {
        var seen: String? = null
        val client = mockClient { req ->
            seen = req.headers[HttpHeaders.Authorization]
            respond(ByteReadChannel("""{"branches":[]}"""), HttpStatusCode.OK, headersOf(HttpHeaders.ContentType, "application/json"))
        }
        val a = BranchAdminClient(MoonshotOptions("http://h", authToken = "T0K3N", httpClient = client))
        a.listBranches()
        assertContains(seen ?: "", "Bearer T0K3N")
        a.close()
    }
}

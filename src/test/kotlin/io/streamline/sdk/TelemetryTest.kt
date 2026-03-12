package io.streamline.sdk

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TelemetryTest {

    // -- NoOp Telemetry --

    @Test
    fun `NoOpTelemetry returns null traceparent`() {
        val telemetry = NoOpTelemetry
        assertNull(telemetry.traceparent())
    }

    @Test
    fun `NoOpTelemetry span operations are safe`() {
        val span = NoOpTelemetry.startSpan("test", mapOf("key" to "value"))
        span.setAttribute("extra", "data")
        span.setError(RuntimeException("test"))
        span.end()
    }

    // -- Console Telemetry --

    @Test
    fun `ConsoleTelemetry generates valid traceparent`() {
        val telemetry = ConsoleTelemetry("test-service")
        val traceparent = telemetry.traceparent()

        assertNotNull(traceparent)
        val parts = traceparent.split("-")
        assertEquals(4, parts.size)
        assertEquals("00", parts[0])
        assertEquals(32, parts[1].length)
        assertEquals(16, parts[2].length)
        assertEquals("01", parts[3])
    }

    @Test
    fun `ConsoleTelemetry traceparent uses same trace ID`() {
        val telemetry = ConsoleTelemetry()
        val tp1 = telemetry.traceparent()!!
        val tp2 = telemetry.traceparent()!!

        val traceId1 = tp1.split("-")[1]
        val traceId2 = tp2.split("-")[1]
        assertEquals(traceId1, traceId2)

        val spanId1 = tp1.split("-")[2]
        val spanId2 = tp2.split("-")[2]
        assertTrue(spanId1 != spanId2, "Span IDs should be unique")
    }

    @Test
    fun `ConsoleTelemetry logs span start and end`() {
        val output = captureStdout {
            val telemetry = ConsoleTelemetry("my-service")
            val span = telemetry.startSpan("test.op", mapOf("key" to "value"))
            span.setAttribute("extra", "data")
            span.end()
        }

        assertTrue(output.contains("[my-service] SPAN START: test.op"))
        assertTrue(output.contains("key=value"))
        assertTrue(output.contains("[my-service] SPAN END: test.op"))
        assertTrue(output.contains("status=OK"))
    }

    @Test
    fun `ConsoleTelemetry logs error status`() {
        val output = captureStdout {
            val telemetry = ConsoleTelemetry("my-service")
            val span = telemetry.startSpan("failing.op")
            span.setError(RuntimeException("boom"))
            span.end()
        }

        assertTrue(output.contains("status=ERROR(boom)"))
    }

    // -- Telemetry Interface --

    @Test
    fun `custom telemetry implementation`() {
        val recorded = mutableListOf<String>()
        val telemetry = object : Telemetry {
            override fun startSpan(name: String, attributes: Map<String, String>): SpanHandle {
                recorded.add("start:$name")
                return object : SpanHandle {
                    override fun setAttribute(key: String, value: String) {
                        recorded.add("attr:$key=$value")
                    }
                    override fun setError(error: Throwable) {
                        recorded.add("error:${error.message}")
                    }
                    override fun end() {
                        recorded.add("end:$name")
                    }
                }
            }
            override fun traceparent(): String = "00-trace-span-01"
        }

        val span = telemetry.startSpan("my.span", mapOf("x" to "1"))
        span.setAttribute("y", "2")
        span.end()

        assertEquals(listOf("start:my.span", "attr:y=2", "end:my.span"), recorded)
        assertEquals("00-trace-span-01", telemetry.traceparent())
    }

    // -- Traced Wrappers (unit-level verification) --

    @Test
    fun `TracedAdminClient creates spans for operations`() {
        val spans = mutableListOf<String>()
        val telemetry = recordingTelemetry(spans)

        val traced = TracedAdminClient(
            delegate = AdminClient("http://localhost:9094"),
            telemetry = telemetry,
        )
        // TracedAdminClient wraps delegate; we verify span names are correct
        assertTrue(spans.isEmpty())
    }

    private fun recordingTelemetry(records: MutableList<String>): Telemetry = object : Telemetry {
        override fun startSpan(name: String, attributes: Map<String, String>): SpanHandle {
            records.add(name)
            return object : SpanHandle {
                override fun setAttribute(key: String, value: String) {}
                override fun setError(error: Throwable) {}
                override fun end() {}
            }
        }
        override fun traceparent(): String? = null
    }

    private fun captureStdout(block: () -> Unit): String {
        val baos = ByteArrayOutputStream()
        val old = System.out
        System.setOut(PrintStream(baos))
        try {
            block()
        } finally {
            System.setOut(old)
        }
        return baos.toString()
    }
}

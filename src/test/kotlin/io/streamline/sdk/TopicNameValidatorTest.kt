package io.streamline.sdk

import kotlin.test.Test
import kotlin.test.assertFailsWith

class TopicNameValidatorTest {

    @Test
    fun `valid topic names are accepted`() {
        TopicNameValidator.validate("my-topic")
        TopicNameValidator.validate("my.topic")
        TopicNameValidator.validate("my_topic")
        TopicNameValidator.validate("Topic123")
        TopicNameValidator.validate("a")
        TopicNameValidator.validate("a-b.c_d")
    }

    @Test
    fun `max length topic name is accepted`() {
        val name = "a".repeat(249)
        TopicNameValidator.validate(name)
    }

    @Test
    fun `blank topic name is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("")
        }
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("   ")
        }
    }

    @Test
    fun `topic name exceeding max length is rejected`() {
        val name = "a".repeat(250)
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate(name)
        }
    }

    @Test
    fun `dot topic name is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate(".")
        }
    }

    @Test
    fun `double dot topic name is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("..")
        }
    }

    @Test
    fun `topic name with invalid characters is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("my topic")
        }
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("my/topic")
        }
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("topic@name")
        }
        assertFailsWith<IllegalArgumentException> {
            TopicNameValidator.validate("topic#name")
        }
    }
}

package io.streamline.sdk

/**
 * Validates Kafka topic names according to the Kafka protocol specification.
 */
object TopicNameValidator {
    private const val MAX_LENGTH = 249
    private val VALID_PATTERN = Regex("^[a-zA-Z0-9._-]+$")

    fun validate(topic: String) {
        require(topic.isNotBlank()) { "Topic name must not be blank" }
        require(topic.length <= MAX_LENGTH) { "Topic name must not exceed $MAX_LENGTH characters" }
        require(topic != "." && topic != "..") { "Topic name must not be \".\" or \"..\"" }
        require(VALID_PATTERN.matches(topic)) {
            "Topic name contains invalid characters. Only alphanumeric, '.', '_', '-' allowed: $topic"
        }
    }
}

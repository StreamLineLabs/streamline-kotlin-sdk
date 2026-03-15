package io.streamline.sdk

/**
 * Interface for custom message serialization.
 *
 * Implement this to use custom data formats (Avro, Protobuf, MessagePack, etc.)
 * with the Streamline producer and consumer.
 *
 * @param T the type to serialize/deserialize
 */
interface MessageSerializer<T> {
    /**
     * Serialize a value to a [String] for transmission.
     *
     * @param topic the target topic (useful for topic-specific serialization logic).
     * @param value the value to serialize.
     * @return the serialized string payload.
     */
    fun serialize(topic: String, value: T): String

    /**
     * Deserialize a [String] payload back to the original type.
     *
     * @param topic the source topic.
     * @param data the raw string payload received from the server.
     * @return the deserialized value.
     */
    fun deserialize(topic: String, data: String): T
}

/**
 * Default pass-through serializer for [String] values.
 *
 * This is the implicit serializer when no custom serializer is configured.
 */
class StringSerializer : MessageSerializer<String> {
    override fun serialize(topic: String, value: String): String = value
    override fun deserialize(topic: String, data: String): String = data
}

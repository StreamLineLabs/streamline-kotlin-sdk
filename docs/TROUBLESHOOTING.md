# Troubleshooting

## Connection Issues

### KafkaException: Connection refused
- Verify broker is running on the expected port (default: 9092)
- Check network connectivity between client and broker
- If using Docker, ensure port mapping is correct

### Authentication Errors
- Verify SASL credentials match broker configuration
- Check that the SASL mechanism is supported by the broker
- Ensure SSL certificates are valid and not expired

## Common Pitfalls

### Consumer not receiving messages
- Check consumer group ID — reusing a group that already consumed all messages
  will start from the latest offset by default
- Use `OffsetResetStrategy.EARLIEST` to replay from the beginning

### Producer performance
- Use batch producing for high-throughput scenarios
- Consider enabling compression (LZ4 recommended for best throughput)

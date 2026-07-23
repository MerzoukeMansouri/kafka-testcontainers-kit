# kafka-testcontainers-kit

Reusable Kafka + Confluent Schema Registry testcontainers bootstrap, plus a generic Avro message
collector, for Spring Kafka integration tests.

Targets Spring Kafka projects using Confluent Avro serialization. JSON/Protobuf and non-Spring
consumer support are not in scope for v1.

## What's in it

- **`KafkaTestBroker`** — starts a `KafkaContainer` + `SchemaRegistryContainer` pair on a shared
  Docker network, picking an arm64-compatible Kafka image automatically on Apple Silicon.
- **`SchemaRegistryContainer`** — Confluent Schema Registry testcontainer wired to a Kafka broker.
- **`KafkaMessageCollector`** — collects Avro-deserialized records per topic, registered at
  runtime with no `@KafkaListener` boilerplate:

  ```java
  KafkaMessageCollector collector = new KafkaMessageCollector(bootstrapServers, schemaRegistryUrl);
  List<MyEvent> received = collector.forTopic("my-topic", MyEvent.class);

  await().atMost(10, SECONDS).until(() -> !received.isEmpty());
  ```

This kit only bootstraps Kafka + Schema Registry. Bring your own Postgres/other container setup —
compose it alongside `KafkaTestBroker` in your own test base class, same as before.

## Usage

```java
public abstract class MyKafkaIntegrationTest {

    private static final Network network = Network.newNetwork();
    private static final KafkaContainer kafka = KafkaTestBroker.newKafkaContainer(network);
    private static final SchemaRegistryContainer schemaRegistry =
            KafkaTestBroker.newSchemaRegistryContainer(kafka, network);

    static {
        kafka.start();
        schemaRegistry.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", schemaRegistry::getSchemaRegistryUrl);
    }
}
```

## Status

`0.1.0-SNAPSHOT`, not yet published. Coordinates: `io.github.merzoukemansouri:kafka-testcontainers-kit`.

## License

MIT — see [LICENSE](LICENSE).

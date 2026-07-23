# kafka-testcontainers-kit

Reusable Kafka + Confluent Schema Registry testcontainers bootstrap for Spring Kafka integration
tests, behind one class.

Targets Spring Kafka projects using Confluent Avro serialization. JSON/Protobuf and non-Spring
consumer support are not in scope for v1. Kafka-only — bring your own Postgres/other container
setup and compose it alongside `KafkaTestKit` in your own test base class.

## Usage

```java
public abstract class MyKafkaIntegrationTest {

    private static final KafkaTestKit kafka = KafkaTestKit.start();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        kafka.registerProperties(registry);
    }
}
```

```java
class MyKafkaIntegrationTest extends MyKafkaIntegrationTestBase {

    @Test
    void publishesEvent() {
        List<MyEvent> received = kafka.forTopic("my-topic", MyEvent.class);

        // trigger production...

        await().atMost(10, SECONDS).until(() -> !received.isEmpty());
    }
}
```

`KafkaTestKit.start()` is memoized — first call starts the containers, every later call (including
from other test classes in the same JVM) returns the same instance. `forTopic` creates the topic if
it doesn't exist yet and starts a listener for it on first call; the returned list is live and grows
as messages arrive.

## Status

`0.1.0-SNAPSHOT`, not yet published. Coordinates: `io.github.merzoukemansouri:kafka-testcontainers-kit`.

## License

MIT — see [LICENSE](LICENSE).

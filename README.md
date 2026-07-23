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
    void testProduction() {
        List<MyEvent> received = kafka.forTopic("my-topic", MyEvent.class);

        // trigger the app under test's production...

        await().atMost(10, SECONDS).until(() -> !received.isEmpty());
    }

    @Test
    void testConsumption() {
        MyEvent event = MyEvent.newBuilder()
                .setId("evt-1")
                .setStatus("CREATED")
                .build();

        kafka.publish("my-topic", "evt-1", event);

        await().atMost(10, SECONDS).until(() -> repository.findById("evt-1").isPresent());
    }
}
```

`KafkaTestKit.start()` is memoized — first call starts the containers, every later call (including
from other test classes in the same JVM) returns the same instance.

`forTopic` (test production) creates the topic if missing and starts a listener on first call; the
returned list is live and grows as messages arrive — assert with polling (e.g. Awaitility).

`publish` (test consumption) creates the topic if missing, Avro-serializes and sends the value
(schema auto-registered), and blocks until the broker acks — so the app under test's consumer has
something to consume by the time the call returns.

## Status

`0.1.0-SNAPSHOT`, not yet published. Coordinates: `io.github.merzoukemansouri:kafka-testcontainers-kit`.

Publishing to Maven Central runs via `.github/workflows/publish.yml` on every GitHub Release
publish — it takes the release tag as the version, signs artifacts with GPG, and deploys through
the Central Portal. Requires the `CENTRAL_USERNAME`, `CENTRAL_PASSWORD`, `GPG_PRIVATE_KEY` and
`GPG_PASSPHRASE` repo secrets (Sonatype Central account + verified `io.github.merzoukemansouri`
namespace + a GPG key) to be set up first.

## License

MIT — see [LICENSE](LICENSE).

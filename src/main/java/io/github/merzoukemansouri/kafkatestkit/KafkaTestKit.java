package io.github.merzoukemansouri.kafkatestkit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.springframework.test.context.DynamicPropertyRegistry;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Single entry point for Kafka integration tests: starts a Kafka + Schema Registry testcontainer
 * pair once per JVM and exposes topic collection and Spring property registration off one object.
 *
 * <pre>{@code
 * public abstract class MyKafkaIntegrationTest {
 *     private static final KafkaTestKit kafka = KafkaTestKit.start();
 *
 *     @DynamicPropertySource
 *     static void properties(DynamicPropertyRegistry registry) {
 *         kafka.registerProperties(registry);
 *     }
 * }
 *
 * // in a test:
 * List<MyEvent> received = kafka.forTopic("my-topic", MyEvent.class);
 * await().atMost(10, SECONDS).until(() -> !received.isEmpty());
 * }</pre>
 */
public final class KafkaTestKit {

    private static final String SPRING_KAFKA_AUTO_REGISTER_SCHEMAS = "spring.kafka.producer.properties.auto.register.schemas";
    private static final String SPRING_KAFKA_SCHEMA_REGISTRY_URL = "spring.kafka.properties.schema.registry.url";
    private static final String SPRING_KAFKA_BOOTSTRAP_SERVERS = "spring.kafka.bootstrap-servers";

    private static volatile KafkaTestKit instance;

    private final KafkaContainer kafka;
    private final SchemaRegistryContainer schemaRegistry;
    private final KafkaMessageCollector collector;

    private KafkaTestKit() {
        Network network = Network.newNetwork();
        this.kafka = KafkaTestBroker.newKafkaContainer(network);
        this.schemaRegistry = KafkaTestBroker.newSchemaRegistryContainer(kafka, network);
        kafka.start();
        schemaRegistry.start();
        this.collector = new KafkaMessageCollector(kafka.getBootstrapServers(), schemaRegistry.getSchemaRegistryUrl());
    }

    /**
     * Starts the shared Kafka + Schema Registry containers on first call; later calls return the
     * same instance. Containers are never explicitly stopped — reused across every test class in
     * the JVM the same way a plain testcontainers static field would be.
     */
    public static synchronized KafkaTestKit start() {
        if (instance == null) {
            instance = new KafkaTestKit();
        }
        return instance;
    }

    /**
     * Registers {@code spring.kafka.bootstrap-servers}, {@code spring.kafka.properties.schema.registry.url}
     * and {@code spring.kafka.producer.properties.auto.register.schemas} against a
     * {@code @DynamicPropertySource} registry.
     */
    public void registerProperties(DynamicPropertyRegistry registry) {
        registry.add(SPRING_KAFKA_AUTO_REGISTER_SCHEMAS, () -> true);
        registry.add(SPRING_KAFKA_SCHEMA_REGISTRY_URL, schemaRegistry::getSchemaRegistryUrl);
        registry.add(SPRING_KAFKA_BOOTSTRAP_SERVERS, kafka::getBootstrapServers);
    }

    /**
     * Returns the live list of Avro-deserialized messages received on {@code topic}, creating the
     * topic (if missing) and starting a listener for it on first call. The returned list is backed
     * by the collector and grows as messages arrive — assert against it with polling (e.g.
     * Awaitility) rather than a single read.
     */
    public <T> List<T> forTopic(String topic, Class<T> type) {
        createTopicIfMissing(topic);
        return collector.forTopic(topic, type);
    }

    private void createTopicIfMissing(String topic) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("Failed to create topic " + topic, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating topic " + topic, e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Timed out creating topic " + topic, e);
        }
    }
}

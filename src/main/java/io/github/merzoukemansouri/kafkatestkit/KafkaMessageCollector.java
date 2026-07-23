package io.github.merzoukemansouri.kafkatestkit;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * Collects Avro-deserialized Kafka records per topic for assertions in integration tests.
 * <p>
 * Unlike a {@code @KafkaListener} bean, topics are registered lazily and dynamically at runtime via
 * {@link #forTopic(String, Class)} — no per-topic annotated method or Spring context wiring
 * required. Each call starts its own {@link KafkaMessageListenerContainer} with a fresh, unique
 * consumer group so registrations never share rebalance state across topics.
 */
public class KafkaMessageCollector implements AutoCloseable {

    private final ConsumerFactory<String, Object> consumerFactory;
    private final Map<String, List<Object>> messagesByTopic = new ConcurrentHashMap<>();
    private final Map<String, MessageListenerContainer> containersByTopic = new ConcurrentHashMap<>();

    public KafkaMessageCollector(String bootstrapServers, String schemaRegistryUrl) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        );
        this.consumerFactory = new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Returns the live list of messages received on {@code topic}, starting a listener for it on
     * first call. The returned list is backed by the collector and grows as messages arrive —
     * assert against it with polling (e.g. Awaitility) rather than a single read.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> forTopic(String topic, Class<T> type) {
        containersByTopic.computeIfAbsent(topic, this::startContainer);
        return (List<T>) messagesByTopic.computeIfAbsent(topic, t -> new CopyOnWriteArrayList<>());
    }

    private MessageListenerContainer startContainer(String topic) {
        List<Object> sink = messagesByTopic.computeIfAbsent(topic, t -> new CopyOnWriteArrayList<>());

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId("kafka-test-kit-" + topic + "-" + UUID.randomUUID());
        containerProperties.setMessageListener(
                (org.springframework.kafka.listener.MessageListener<String, Object>) record -> sink.add(record.value()));

        KafkaMessageListenerContainer<String, Object> container =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.start();
        return container;
    }

    @Override
    public void close() {
        containersByTopic.values().forEach(MessageListenerContainer::stop);
    }
}

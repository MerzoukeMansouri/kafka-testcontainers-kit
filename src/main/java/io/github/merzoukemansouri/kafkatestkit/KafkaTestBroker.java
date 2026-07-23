package io.github.merzoukemansouri.kafkatestkit;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Factory for a Kafka + Schema Registry testcontainer pair, wired onto the same Docker network.
 * Picks an arm64-compatible Kafka image transparently on Apple Silicon hosts. Internal to
 * {@link KafkaTestKit}.
 */
final class KafkaTestBroker {

    private static final String KAFKA_ARM_IMAGE = "niciqy/cp-kafka-arm64:7.0.1";
    private static final String KAFKA_X86_IMAGE = "confluentinc/cp-kafka:5.4.3";
    private static final String KAFKA_SUBSTITUTE = "confluentinc/cp-kafka";
    private static final String MAC_OS_PREFIX = "Mac";

    private KafkaTestBroker() {
    }

    /**
     * Picks the Kafka docker image for the current JVM's {@code os.name}, substituting an
     * arm64-compatible image on Apple Silicon since upstream Confluent images predate native arm64
     * support.
     */
    static DockerImageName selectKafkaImage(String osName) {
        return osName.startsWith(MAC_OS_PREFIX)
                ? DockerImageName.parse(KAFKA_ARM_IMAGE).asCompatibleSubstituteFor(KAFKA_SUBSTITUTE)
                : DockerImageName.parse(KAFKA_X86_IMAGE);
    }

    static KafkaContainer newKafkaContainer(Network network) {
        return new KafkaContainer(selectKafkaImage(System.getProperty("os.name")))
                .withNetwork(network);
    }

    static SchemaRegistryContainer newSchemaRegistryContainer(KafkaContainer kafkaContainer, Network network) {
        return new SchemaRegistryContainer(kafkaContainer).withNetwork(network);
    }
}

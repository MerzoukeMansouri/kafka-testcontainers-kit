package io.github.merzoukemansouri.kafkatestkit;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

/**
 * Confluent Schema Registry testcontainer, wired to run against a {@link KafkaContainer} on the
 * same Docker network.
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final String DEFAULT_IMAGE = "confluentinc/cp-schema-registry:7.5.2";
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
    private static final String NETWORK_ALIAS = "schema-registry";

    public SchemaRegistryContainer(KafkaContainer kafkaContainer) {
        this(kafkaContainer, DEFAULT_IMAGE);
    }

    public SchemaRegistryContainer(KafkaContainer kafkaContainer, String dockerImageName) {
        super(dockerImageName);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", NETWORK_ALIAS);
        addEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081");

        withNetworkAliases(NETWORK_ALIAS);
        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                "PLAINTEXT://" + kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%d", getHost(), getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }
}

package io.github.merzoukemansouri.kafkatestkit;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaTestBrokerTest {

    @Test
    void selectsArmSubstituteImageOnMac() {
        DockerImageName image = KafkaTestBroker.selectKafkaImage("Mac OS X");
        assertEquals("niciqy/cp-kafka-arm64:7.0.1", image.asCanonicalNameString());
    }

    @Test
    void selectsX86ImageOnLinux() {
        DockerImageName image = KafkaTestBroker.selectKafkaImage("Linux");
        assertEquals("confluentinc/cp-kafka:5.4.3", image.asCanonicalNameString());
    }
}

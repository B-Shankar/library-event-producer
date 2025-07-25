package com.learnkafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("spring.kafka")
public class KafkaProperties {
    private String topic;
}

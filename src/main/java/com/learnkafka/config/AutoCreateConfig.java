package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class AutoCreateConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public NewTopic libraryEvents() {
        return TopicBuilder
                .name(kafkaProperties.getTopic())
                .partitions(3)
                .replicas(3).build();
    }
}

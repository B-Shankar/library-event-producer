package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.config.KafkaProperties;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    @Autowired
    private KafkaProperties kafkaProperties;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var completableFuture = kafkaTemplate.send(kafkaProperties.getTopic(), key, value);

        return completableFuture.whenComplete((sendResult, exception) -> {
            if (exception != null) {
                // Handle the exception
                handleFailure(key, value, exception);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully with key: {}, value: {}, partition: {}, offset: {}",
                key, value, sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());
    }

    private void handleFailure(Integer key, String value, Throwable exception) {
        log.error("Error sending message with key: {}, value: {}", key, value);
        log.error("Error sending the message and exception :{}", exception.getMessage(), exception);
    }
}

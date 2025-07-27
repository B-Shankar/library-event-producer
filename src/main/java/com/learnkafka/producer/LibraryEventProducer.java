package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.config.KafkaProperties;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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

        //1. Blocking Call - get metadata about the kafka cluster.
        //2. Send message happens - Returns the CompletableFuture
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

    public SendResult<Integer, String> sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        //1. Blocking Call - get metadata about the kafka cluster.
        //2. Block and Wait - Until the message is sent to Kafka

//        var sendResult = kafkaTemplate.send(kafkaProperties.getTopic(), key, value).get();

        var sendResult = kafkaTemplate.send(kafkaProperties.getTopic(), key, value).get(3, java.util.concurrent.TimeUnit.SECONDS); //TimeoutException also
        //it will throw an exception if the message is not sent within the specified time.

        handleSuccess(key, value, sendResult);
        return sendResult;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord = buildProducerRecord(key, value);
        var completableFuture = kafkaTemplate.send(producerRecord);

        return completableFuture.whenComplete((sendResult, exception) -> {
            if (exception != null) {
                // Handle the exception
                handleFailure(key, value, exception);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(kafkaProperties.getTopic(), null, key, value, recordHeaders);
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

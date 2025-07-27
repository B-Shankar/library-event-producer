package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventController {

    private final LibraryEventProducer libraryEventProducer;

    public LibraryEventController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent
    ) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        //Approach 1: Non-blocking call : Asynchronous Call
        //log.info("Library Event: {}", libraryEvent);
        //libraryEventProducer.sendLibraryEvent(libraryEvent);
        //log.info("After sending the library event to Kafka");

        //Approach 2: Blocking call : Synchronous Call
        log.info("Library Event: {}", libraryEvent);
        var sendResult = libraryEventProducer.sendLibraryEvent_approach2(libraryEvent);
        log.info("After sending the library event to Kafka, SendResult: {}", sendResult);

        //Invoke the kafka producer
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}

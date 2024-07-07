package com.example.conduktor_kafka_assignment.controller;

import com.example.conduktor_kafka_assignment.service.KafkaMessageConsumerService;
import com.example.conduktor_kafka_assignment.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@RestController
@Slf4j
public class Controller {

    @Autowired
    private KafkaMessageConsumerService kafkaMessageConsumerService;

    @GetMapping("/topic/{topic_name}/{offset}")
    public CompletableFuture<List<Person>> getMessages(
            @PathVariable("topic_name") String topicName,
            @PathVariable("offset") int offset,
            @RequestParam(value = "count", defaultValue = "10") int count) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return kafkaMessageConsumerService.getMessages(topicName, offset, count);
            } catch (Exception e) {
                log.error("Unexpected error occurred whilst getting messages for the following inputs: topicName {}, offset {}, count {}", topicName, offset, count, e);
            }
            return null;
        });
    }
}
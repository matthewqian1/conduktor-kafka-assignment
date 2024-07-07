package com.example.conduktor_kafka_assignment.service;

import com.example.conduktor_kafka_assignment.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@Service
public class KafkaMessageConsumerService {

    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    private final Map<String, KafkaConsumer<String, Person>> consumerMap = new ConcurrentHashMap<>();

    @Autowired
    private ConsumerFactory<String, Person> consumerFactory;

    public List<Person> getMessages(String topic, int offset, int count) throws Exception {
        return executorService.submit(() -> {
            KafkaConsumer<String, Person> consumer = consumerMap.computeIfAbsent(topic, t -> (KafkaConsumer<String, Person>) consumerFactory.createConsumer());

            //Read from all partitions for a given topic from the specified offset
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = partitions.stream()
                    .map(partition -> new TopicPartition(topic, partition.partition()))
                    .collect(Collectors.toList());

            consumer.assign(topicPartitions);

            for (TopicPartition partition : topicPartitions) {
                consumer.seek(partition, offset);
            }

            //poll for records until count is reached or no more records are found
            List<Person> personMessages = new ArrayList<>();
            boolean moreRecords = true;
            while (moreRecords) {
                ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    log.warn("Less than requested number of records found. requested {}, returned {}", count, personMessages.size());
                    break;
                }
                for (ConsumerRecord<String, Person> record : records) {
                    personMessages.add(record.value());
                    if (personMessages.size() >= count) {
                        moreRecords = false;
                        break;
                    }
                }
            }
            return personMessages;
        }).get();
    }
}
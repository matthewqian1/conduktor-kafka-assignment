package com.example.conduktor_kafka_assignment.service;

import com.example.conduktor_kafka_assignment.model.PeopleData;
import com.example.conduktor_kafka_assignment.model.Person;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Service
@Slf4j
public class TopicDataInitializer {

    @Value("${kafka.broker.name}")
    private String kafkaBrokers;

    @Value("${kafka.topic.name}")
    private String kafkaTopic;

    @Value("${kafka.bootstrap.server}")
    private String kafkaServer;

    @Value("${kafka.topic.data}")
    private String dataFileName;

    @Autowired
    private KafkaTemplate<String, Person> kafkaTemplate;

    @PostConstruct
    public void init() {
        recreateTopic(kafkaTopic, 3, 1);
        try {
            publishDataFromFileToTopic("/" + dataFileName);
        } catch (IOException e) {
            log.error("Error loading topic data from file {}", dataFileName, e);
        }
    }

    public void publishDataFromFileToTopic(String filePath) throws IOException {
        List<Person> persons = loadPersonsFromFile(filePath);
        for (Person person : persons) {
            kafkaTemplate.send(kafkaTopic, person);
            log.info("Published to topic {}, data {}", kafkaTopic, person);
        }
    }

    private List<Person> loadPersonsFromFile(String filePath) throws IOException {
        log.info("Parsing data from file {}", filePath);
        InputStream inputStream = TopicDataInitializer.class.getResourceAsStream(filePath);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        PeopleData data = objectMapper.readValue(inputStream, PeopleData.class);
        return data.getPersonList();
    }

    private void recreateTopic(String topicName, int partitions, int replicationFactor) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        try (AdminClient adminClient = AdminClient.create(config)) {
            Set<String> topics = adminClient.listTopics().names().get();

            //for debugging purposes to ensure topic has only one set of data loaded
            if (topics.contains(topicName)) {
                adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
                log.info("Deleted existing topic: {}", topicName);
                Thread.sleep(2000);
            }

            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

            NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor)
                    .configs(topicConfig);

            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            log.info("topic {} has been created with {} partitions and replication factor of {}", topicName, partitions, replicationFactor);
        } catch (Exception e) {
            log.error("Unexpected error occurred creating topic {}", topicName, e);
        }
    }
}
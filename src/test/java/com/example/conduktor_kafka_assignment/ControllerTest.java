package com.example.conduktor_kafka_assignment;

import com.example.conduktor_kafka_assignment.controller.Controller;
import com.example.conduktor_kafka_assignment.model.Person;
import com.example.conduktor_kafka_assignment.service.KafkaMessageConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(Controller.class)
public class ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaMessageConsumerService kafkaMessageConsumerService;

    private ObjectMapper objectMapper = new ObjectMapper();;

    @Autowired
    private WebApplicationContext webApplicationContext;

    @BeforeEach
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Test
    public void testGetMessages() throws Exception {
        Person person1 = Person.builder()
                .id("1")
                .name("Jack")
                .build();
        Person person2 = Person.builder()
                .id("2")
                .name("Jill")
                .build();
        List<Person> persons = Arrays.asList(person1, person2);


        Mockito.when(kafkaMessageConsumerService.getMessages(anyString(), anyInt(), anyInt()))
                .thenReturn(persons);

        //trigger the action and then extract aysnc result to check
        MvcResult mvcResult = mockMvc.perform(get("/topic/people-data/0?count=2"))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(persons)));
    }
}


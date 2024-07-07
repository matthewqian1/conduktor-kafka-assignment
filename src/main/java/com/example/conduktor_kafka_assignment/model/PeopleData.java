package com.example.conduktor_kafka_assignment.model;

import com.example.conduktor_kafka_assignment.model.Person;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class PeopleData {

    @JsonProperty("ctRoot")
    List<Person> personList;

    public List<Person> getPersonList() {
        return personList;
    }
}

package com.example.conduktor_kafka_assignment.model;

import com.example.conduktor_kafka_assignment.model.Address;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.URL;
import java.time.LocalDate;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Person {

    @JsonProperty("_id")
    private String id;
    private String name;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private LocalDate dob;
    private Address address;
    private String telephone;
    private List<String> pets;
    private double score;
    private String email;
    private URL url;
    private String description;
    private boolean verified;
    private int salary;
}

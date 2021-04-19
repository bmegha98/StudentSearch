package com.example.esdemo.StudentSearch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Student {

  @JsonProperty("FirstName")
  private String FirstName;

  @JsonProperty("LastName")
  private String LastName;

  @JsonProperty("RollNumber")
  private Long RollNumber;

  @JsonProperty("Address")
  private String Address;

  @JsonProperty("Age")
  private Integer Age;

  @JsonProperty("Gender")
  private String Gender;
}

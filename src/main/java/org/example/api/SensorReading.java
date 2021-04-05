package org.example.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class SensorReading {

  private String id;
  private String timestamp;
  private Double temperature;

}

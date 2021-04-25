package org.example.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class SensorReading2 {

    private String id;
    private Long timestamp;
    private Double temperature;

}

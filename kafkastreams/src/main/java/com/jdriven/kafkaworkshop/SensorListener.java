package com.jdriven.kafkaworkshop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SensorListener {

  private static final Logger log = LoggerFactory.getLogger(SensorListener.class);

  @KafkaListener(topics = TopicNames.RECEIVED_SENSOR_DATA)
  public void listen(SensorData sensorData) {
    log.info("listen({})", sensorData);
  }
}

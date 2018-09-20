package com.jdriven.kafkaworkshop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(topics = TopicNames.RECEIVED_SENSOR_DATA)
@Slf4j
public class TestEmbeddedKafka {

  @Value("${spring.embedded.kafka.brokers}")
  private String brokerAddress;

  @Autowired
  private SensorController sensorController;

  @Autowired
  private KafkaEmbedded embeddedKafka;

  Consumer<String, SensorData> consumer;

  @Before
  public void setuplistener() throws Exception {
    // put our test consumer in a separate consumer group
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(brokerAddress, "group", "false");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    DefaultKafkaConsumerFactory<String, SensorData> cf =
        new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
    embeddedKafka.consumeFromAllEmbeddedTopics(consumer);
  }

  @After
  public void shutdownKafka() {
    // this prevents FileNotFoundException stack traces in the log
    log.info("shutting down");
    embeddedKafka.getKafkaServers().forEach(broker -> broker.shutdown());
    embeddedKafka.getKafkaServers().forEach(broker -> broker.awaitShutdown());
  }


  @Test
  public void tryout() throws Exception {
    assertNotNull(brokerAddress);
    assertNotNull(sensorController);

    log.info("brokerAddress='{}'", brokerAddress);

    SensorData sensorData = new SensorData();
    sensorData.setId("test");
    sensorData.setVoltage(12.0);
    sensorData.setTemperature(25.0);

    sensorController.sensorSubmit(sensorData);

    ConsumerRecord<String, SensorData> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TopicNames.RECEIVED_SENSOR_DATA);
    assertThat(singleRecord.key(), is(sensorData.getId()));
    log.info(singleRecord.toString());
  }

}

package com.jdriven.kafkaworkshop;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SensorControllerTest {

  private static final Logger log = LoggerFactory.getLogger(SensorControllerTest.class);

  @Autowired
  private SensorController sensorController;

  private KafkaMessageListenerContainer<String, SensorData> container;

  private BlockingQueue<ConsumerRecord<String, SensorData>> records;


  @ClassRule
  public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TopicNames.RECEIVED_SENSOR_DATA);

  @Before
  public void setUp() throws Exception {
    // set up the Kafka consumer properties
    Map<String, Object> consumerProperties =
        KafkaTestUtils.consumerProps("sender", "false", embeddedKafka);

    // create a Kafka consumer factory
    DefaultKafkaConsumerFactory<String, SensorData> consumerFactory =
        new DefaultKafkaConsumerFactory<>(consumerProperties);

    // set the topic that needs to be consumed
    ContainerProperties containerProperties = new ContainerProperties(TopicNames.RECEIVED_SENSOR_DATA);

    // create a Kafka MessageListenerContainer
    container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

    // create a thread safe queue to store the received message
    records = new LinkedBlockingQueue<>();

    // setup a Kafka message listener
    container.setupMessageListener(new MessageListener<String, SensorData>() {
      @Override
      public void onMessage(ConsumerRecord<String, SensorData> record) {
        log.info("test-listener received message='{}'", record.toString());
        records.add(record);
      }
    });

    // start the container and underlying message listener
    container.start();

    // wait until the container has the required number of assigned partitions
    ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
  }

  @After
  public void tearDown() {
    // stop the container
    container.stop();
  }

  @Test
  public void postingSensorResultsInMessage() throws Exception {
    // given a reading
    SensorData sensorData = new SensorData();
    sensorData.setId("test");
    sensorData.setVoltage(12.0);
    sensorData.setTemperature(25.0);

    // when the reading is sent to the controller
    sensorController.sensorSubmit(sensorData);

    // a message is sent to the topic
    ConsumerRecord<String, SensorData> received = records.poll(5, TimeUnit.SECONDS);
    assertThat(received, hasValue(sensorData));

    // and the key is the ID of the data
//    assertThat(received).has(key(sensorData.getId()));
  }


}

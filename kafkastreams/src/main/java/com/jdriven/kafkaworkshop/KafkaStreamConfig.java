package com.jdriven.kafkaworkshop;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {

  private static final Logger log = LoggerFactory.getLogger(KafkaStreamConfig.class);

  @Value("${kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Value("${application.stream.applicationId}")
  private String applicationId;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public StreamsConfig streamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    return new StreamsConfig(props);
  }

//  @Bean
  public KStream<String, SensorData> streamOne(StreamsBuilder builder) {
    JsonSerde<SensorData> sensorDataSerde = new JsonSerde<>(SensorData.class);
    KStream<String, SensorData> kStream = builder.stream(TopicNames.RECEIVED_SENSOR_DATA, Consumed.with(Serdes.String(), sensorDataSerde));

    kStream.foreach((key, value) -> log.info("kstream: {}", value));

    kStream.filter((key, value) -> key.startsWith("#")).foreach((k,v) -> log.info("filtered {}", k));

    return kStream;
  }

  @Bean
  public KStream<String, SensorData> onlythea(StreamsBuilder builder) {
    JsonSerde<SensorData> sensorDataSerde = new JsonSerde<>(SensorData.class);
    KStream<String, SensorData> kStream = builder.stream(TopicNames.RECEIVED_SENSOR_DATA, Consumed.with(Serdes.String(), sensorDataSerde));
    return kStream.filter((key, value) -> key.toLowerCase().startsWith("a"));
  }

  /**
   * This will get injected the result stream from the previous bean.
   * @param onlythea
   * @return
   */
  @Bean
  public KStream<String,SensorData> filtered(KStream<String, SensorData> onlythea) {
    onlythea.foreach((k,v) -> log.info("starts with a"));
    return onlythea;
  }
}

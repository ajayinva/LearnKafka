package com.aces.learn.kafka.stream.ksia.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import com.aces.learn.kafka.stream.ksia.serializer.*;

import java.util.Map;

public class BaseSerdes<T> implements Serde<T> {

  private JsonSerializer<T> serializer;
  private JsonDeserializer<T> deserializer;

  public BaseSerdes(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}

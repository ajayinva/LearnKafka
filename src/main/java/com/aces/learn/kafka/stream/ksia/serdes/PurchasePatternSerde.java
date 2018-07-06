package com.aces.learn.kafka.stream.ksia.serdes;


import com.aces.learn.kafka.stream.ksia.model.PurchasePattern;
import com.aces.learn.kafka.stream.ksia.serializer.JsonDeserializer;
import com.aces.learn.kafka.stream.ksia.serializer.JsonSerializer;

public class PurchasePatternSerde extends BaseSerdes<PurchasePattern> {
  public PurchasePatternSerde() {
    super(new JsonSerializer<>(), new JsonDeserializer<>(PurchasePattern.class));
  }
}

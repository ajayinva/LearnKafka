package com.aces.learn.kafka.stream.ksia.serdes;

import com.aces.learn.kafka.stream.ksia.model.Purchase;
import com.aces.learn.kafka.stream.ksia.serializer.JsonDeserializer;
import com.aces.learn.kafka.stream.ksia.serializer.JsonSerializer;

/**
 *
 */
public class PurchaseSerde extends BaseSerdes<Purchase> {
  public PurchaseSerde() {
    super(new JsonSerializer<>(), new JsonDeserializer<>(Purchase.class));
  }
}
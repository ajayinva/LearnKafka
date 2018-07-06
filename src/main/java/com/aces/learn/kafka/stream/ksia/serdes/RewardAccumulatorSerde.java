package com.aces.learn.kafka.stream.ksia.serdes;


import com.aces.learn.kafka.stream.ksia.model.RewardAccumulator;
import com.aces.learn.kafka.stream.ksia.serializer.JsonDeserializer;
import com.aces.learn.kafka.stream.ksia.serializer.JsonSerializer;

public class RewardAccumulatorSerde extends BaseSerdes<RewardAccumulator> {
  public RewardAccumulatorSerde() {
    super(new JsonSerializer<>(), new JsonDeserializer<>(RewardAccumulator.class));
  }
}

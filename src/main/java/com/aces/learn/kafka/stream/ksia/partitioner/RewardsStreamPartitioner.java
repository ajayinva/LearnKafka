package com.aces.learn.kafka.stream.ksia.partitioner;

import com.aces.learn.kafka.stream.ksia.model.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;


public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {

    @Override
    public Integer partition(String key, Purchase value, int numPartitions) {
        return value.getCustomerId().hashCode() % numPartitions;
    }
}

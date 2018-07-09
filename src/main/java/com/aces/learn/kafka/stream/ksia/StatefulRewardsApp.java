package com.aces.learn.kafka.stream.ksia;

import com.aces.learn.kafka.stream.ksia.model.Purchase;
import com.aces.learn.kafka.stream.ksia.model.RewardAccumulator;
import com.aces.learn.kafka.stream.ksia.partitioner.RewardsStreamPartitioner;
import com.aces.learn.kafka.stream.ksia.serdes.PurchaseSerde;
import com.aces.learn.kafka.stream.ksia.serdes.RewardAccumulatorSerde;
import com.aces.learn.kafka.stream.ksia.transformer.PurchaseRewardTransformer;
import com.aces.learn.kafka.utils.MockDataProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *
 */
public class StatefulRewardsApp {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulRewardsApp.class);

    private static final  String REWARDS_STATE_STORE_NAME = "rewardsPointsStore";
    /**
     *
     * @param streamsBuilder
     * @return
     */
    private static KStream<String,Purchase> processPurchaseStream(
        StreamsBuilder streamsBuilder
    ){
        Serde<Purchase> purchaseSerde = new PurchaseSerde();
        KStream<String,Purchase> purchaseKStream = streamsBuilder
                .stream("transactions", Consumed.with(Serdes.String(), purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        return purchaseKStream;
    }
    /**
     *
     * @param purchaseKStream
     */
    private static void processRewardsStream(
        KStream<String,Purchase> purchaseKStream
    ){
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();
        KStream<String, Purchase> transByCustomerStream = purchaseKStream.through( "customer_transactions", Produced.with(Serdes.String(), new PurchaseSerde(), streamPartitioner));

        KStream<String, RewardAccumulator> statefulRewardAccumulator
                = transByCustomerStream.transformValues(
                    () ->  new PurchaseRewardTransformer(REWARDS_STATE_STORE_NAME)
                ,   REWARDS_STATE_STORE_NAME
        );

        statefulRewardAccumulator.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        statefulRewardAccumulator.to("rewards", Produced.with(Serdes.String(), new RewardAccumulatorSerde()));
    }

    /**
     *
     * @return
     */
    private static StoreBuilder<KeyValueStore<String, Integer>> getRewardsStateStore(){
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(REWARDS_STATE_STORE_NAME);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
        return storeBuilder;
    }
    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(getRewardsStateStore());
        KStream<String,Purchase> purchaseKStream = processPurchaseStream(streamsBuilder);
        processRewardsStream(purchaseKStream);
        MockDataProducer.producePurchaseData();
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),streamsConfig);
        LOG.info("ZMart First Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    /**
     *
     * @return
     */
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}

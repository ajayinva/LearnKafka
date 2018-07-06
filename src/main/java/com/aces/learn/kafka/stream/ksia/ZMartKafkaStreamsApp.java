package com.aces.learn.kafka.stream.ksia;

import com.aces.learn.kafka.stream.ksia.model.Purchase;
import com.aces.learn.kafka.stream.ksia.model.PurchasePattern;
import com.aces.learn.kafka.stream.ksia.model.RewardAccumulator;
import com.aces.learn.kafka.stream.ksia.serdes.PurchasePatternSerde;
import com.aces.learn.kafka.stream.ksia.serdes.PurchaseSerde;
import com.aces.learn.kafka.stream.ksia.serdes.RewardAccumulatorSerde;
import com.aces.learn.kafka.stream.ksia.service.SecurityDBService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *
 */
public class ZMartKafkaStreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsApp.class);

    /**
     *
     * @param purchaseKStream
     */
    private static void processStreamByEmployee(KStream<String,Purchase> purchaseKStream){
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(), purchase.getEmployeeId(), purchase.getItemPurchased());
        purchaseKStream.filter((key, purchase) -> purchase.getEmployeeId().equals("000000")).foreach(purchaseForeachAction);
    }

    /**
     *
     * @param purchaseKStream
     */
    private static void processBranchesByDept(KStream<String,Purchase> purchaseKStream){
        Serde<Purchase> purchaseSerde = new PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int coffee = 0;
        int electronics = 1;

        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isCoffee, isElectronics);

        kstreamByDept[coffee].to( "coffee", Produced.with(stringSerde, purchaseSerde));
        kstreamByDept[coffee].print(Printed.<String, Purchase>toSysOut().withLabel( "coffee"));

        kstreamByDept[electronics].to("electronics", Produced.with(stringSerde, new PurchaseSerde()));
        kstreamByDept[electronics].print(Printed.<String, Purchase>toSysOut().withLabel("electronics"));
    }

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

        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();
        KStream<Long, Purchase> filteredKStream = purchaseKStream.filter((key, purchase) -> purchase.getPrice() > 5.00).selectKey(purchaseDateAsKey);
        filteredKStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
        filteredKStream.to("purchases", Produced.with(Serdes.Long(),purchaseSerde));
        return purchaseKStream;
    }

    /**
     *
     * @param purchaseKStream
     */
    private static void processPatternStream(
        KStream<String,Purchase> purchaseKStream
    ){
        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(Serdes.String(),new PurchasePatternSerde()));
    }

    /**
     *
     * @param purchaseKStream
     */
    private static void processRewardsStream(
        KStream<String,Purchase> purchaseKStream
    ){
        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());
        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        rewardsKStream.to("rewards", Produced.with(Serdes.String(),new RewardAccumulatorSerde()));
    }

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,Purchase> purchaseKStream = processPurchaseStream(streamsBuilder);
        processPatternStream(purchaseKStream);
        processRewardsStream(purchaseKStream);
        processBranchesByDept(purchaseKStream);
        processStreamByEmployee(purchaseKStream);

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

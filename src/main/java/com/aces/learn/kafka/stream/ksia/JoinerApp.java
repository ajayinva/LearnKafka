package com.aces.learn.kafka.stream.ksia;

import com.aces.learn.kafka.stream.ksia.joiner.PurchaseJoiner;
import com.aces.learn.kafka.stream.ksia.model.CorrelatedPurchase;
import com.aces.learn.kafka.stream.ksia.model.Purchase;
import com.aces.learn.kafka.stream.ksia.serdes.PurchaseSerde;
import com.aces.learn.kafka.stream.ksia.timestamp_extractor.TransactionTimestampExtractor;
import com.aces.learn.kafka.utils.MockDataProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *
 */
public class JoinerApp {

    public static final int COFFEE = 0;
    public static final int ELECTRONICS = 1;

    private static final Logger LOG = LoggerFactory.getLogger(JoinerApp.class);

    /**
     *
     * @param purchaseKStream
     */
    private static void processBranchesByDept(KStream<String,Purchase> purchaseKStream){
        Serde<Purchase> purchaseSerde = new PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.selectKey((k,v)-> v.getCustomerId()).branch(isCoffee, isElectronics);

        KStream<String, Purchase> coffeeStream = kstreamByDept[COFFEE];
        KStream<String, Purchase> electronicsStream = kstreamByDept[ELECTRONICS];

        coffeeStream.to( "coffee", Produced.with(stringSerde, purchaseSerde));
        coffeeStream.print(Printed.<String, Purchase>toSysOut().withLabel( "coffee"));

        electronicsStream.to("electronics", Produced.with(stringSerde, new PurchaseSerde()));
        electronicsStream.print(Printed.<String, Purchase>toSysOut().withLabel("electronics"));


        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        JoinWindows twentyMinuteWindow =  JoinWindows.of(60 * 1000 * 20);

        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream.join(electronicsStream,
                purchaseJoiner,
                twentyMinuteWindow,
                Joined.with(stringSerde,
                        purchaseSerde,
                        purchaseSerde));

        joinedKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joined KStream"));
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
        return purchaseKStream;
    }



    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,Purchase> purchaseKStream = processPurchaseStream(streamsBuilder);
        processBranchesByDept(purchaseKStream);


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
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "JoinerAppClient");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "JoinerAppGroup");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "JoinerAppClient");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }

}

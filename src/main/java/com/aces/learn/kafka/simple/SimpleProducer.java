/**
 * 
 */
package com.aces.learn.kafka.simple;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author aagarwal
 *
 */
public class SimpleProducer {
	public static void main(String[] args) {
		System.out.println("I am Learing Kafka");
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for(int i = 0; i < 100; i++){
			 System.out.println("Sending to Kafka topic");
		     producer.send(new ProducerRecord<String, String>("test", "Sending to Kafka topic::"+Integer.toString(i), Integer.toString(i)));
		 }
		 producer.close();
	}
}

package com.poc.telemetry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class Telemetry {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        while (true) {

            String payload = "{\"speed\":50,\"autonomie\":40.0,\"distance_station\":60.0}";
            producer.send(new ProducerRecord<>("telemetrie_brute", payload));

            System.out.println("[Telemetry] Donnée brute envoyée : " + payload);

            Thread.sleep(3000);
        }
    }
}

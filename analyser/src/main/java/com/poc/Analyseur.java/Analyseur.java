package com.poc.analyser;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Analyseur {

    public static void main(String[] args) {

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        consumerProps.put("group.id", "analyseur-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("telemetrie_brute"));

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println("[Analyseur] Analyseur démarré.");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {

                System.out.println("[Analyseur] Donnée brute reçue : " + record.value());

                // Normalisation de démonstration
                String normalized = "{\"autonomie\":80.0,\"distance_station\":60.0}";

                producer.send(new ProducerRecord<>("telemetrie_norm", normalized));

                System.out.println("[Analyseur] Donnée normalisée envoyée : " + normalized);
            }
        }
    }
}

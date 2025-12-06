package com.poc.rules;

import org.apache.kafka.clients.consumer.*;
import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RulesEngine {

    private static CqlSession connectWithRetry() {
        int maxRetries = 20;
        int delayMs = 3000;

        for (int i = 1; i <= maxRetries; i++) {
            try {
                System.out.println("[RulesEngine] Tentative de connexion à Cassandra (" + i + "/" + maxRetries + ")");
                return CqlSession.builder()
                        .addContactPoint(new InetSocketAddress(System.getenv("CASSANDRA_HOST"), 9042))
                        .withLocalDatacenter("datacenter1")
                        .build();

            } catch (Exception e) {
                System.out.println("[RulesEngine] Cassandra indisponible, nouvelle tentative...");
                try { Thread.sleep(delayMs); } catch (InterruptedException ignored) {}
            }
        }

        throw new RuntimeException("Impossible de se connecter à Cassandra après plusieurs tentatives.");
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        props.put("group.id", "rules-engine-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("telemetrie_norm"));

        System.out.println("[RulesEngine] Moteur démarré.");

        try (CqlSession session = connectWithRetry()) {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("[RulesEngine] Donnée reçue : " + record.value());

                    double autonomie = 80.0;
                    double distanceStation = 60.0;
                    double seuil = distanceStation * 1.2;

                    // if (autonomie < seuil) {
                    if (true) {

                        System.out.println("[RulesEngine] Alerte déclenchée : SUGGESTION_STATION_H2");

                        SimpleStatement stmt = SimpleStatement.builder(
                                "INSERT INTO telemetrie.alertes (id, timestamp, autonomie, seuil, type_alerte) " +
                                        "VALUES (uuid(), toTimestamp(now()), ?, ?, 'SUGGESTION_STATION_H2')")
                                .addPositionalValues(autonomie, seuil)
                                .build();

                        session.execute(stmt);
                        System.out.println("[RulesEngine] Alerte enregistrée.");
                    }
                }
            }
        }
    }
}

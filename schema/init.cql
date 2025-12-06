CREATE KEYSPACE IF NOT EXISTS telemetrie
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS telemetrie.alertes (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMP,
    autonomie DOUBLE,
    seuil DOUBLE,
    type_alerte TEXT
);

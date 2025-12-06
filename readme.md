## POC TELEMETRIE — GUIDE D’EXECUTION

Le POC met en œuvre les fonctionnalités essentielles du système, depuis l’ingestion de la télémétrie jusqu’à la détection et la persistance des alertes, afin de démontrer la validité de l’architecture proposée.

Les commandes suivantes permettent de déployer l’environnement du POC, de lancer les microservices et de valider le bon déroulement du pipeline télémétrique.

1. Se placer dans le dossier du projet

```cd poc-telemetrie ```

2. Construction des images Docker

Construire l’ensemble des microservices ainsi que Kafka, Zookeeper et Cassandra :<br>

```docker-compose build```

À relancer uniquement si le code Java ou les Dockerfile sont modifiés.

3. Redémarrer un microservice spécifique

Rules Engine :

```docker-compose up --build rules-engine```

Analyseur :

```docker-compose up --build analyser```

Service Télémétrie :

```docker-compose up --build telemetry```

4. Vérifier les données stockées dans Cassandra

Ouvrir le shell Cassandra :

```docker exec -it poc-telemetrie-cassandra-1 cqlsh```

Si le nom du container diffère : 

```docker ps```

Afficher les keyspaces : 

```DESCRIBE KEYSPACES;```

Utiliser le keyspace telemetrie :

```USE telemetrie;```

Afficher les alertes enregistrées :

```SELECT * FROM alertes;```

5. Arrêter tous les services

```docker-compose down```

6. Nettoyage optionnel

Supprimer les images inutilisées :

```docker system prune -a```
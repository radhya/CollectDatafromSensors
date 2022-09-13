# Sensor Collecting Server

This project implements a remote Websocket server to receive sensor data sending from Android Sensor Data Collector and send them to a Kafka server. 

# Configuration
The "config.properties" file contains parameters for configuring the server. The file includes the following fields:
- server_port: the main port used by the server
- kafka_brokers: the list of brokers of the Kafka server (seperated by commas ",")
- kafka_topic: the topic exchange of the Kafka server

# How to run
- Download the folder "jar-package" which includes: 
    - The configuration file "config.properties"
    - The jar file "sensor-collecting-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
- Run the jar file
```
$ java -jar sensor-collecting-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```
# Kafka Cluster unit testing framework

This is a Java unit test framework for testing scenarios requiring use of [Apache Kafka](http://kafka.apache.org)

# Features

* Cluster support
* Fast startup

# Kafka version support

kafka-cluster-unit comes with support for Apache Kafka 0.10.1.1 and Apache ZooKeeper 3.4.6. May work with other version pairs.

# Usage

## Download the latest JAR

```xml
<dependency>
	<groupId>org.mandas.kafka</groupId>
	<artifactId>kafka-cluster-unit</artifactId>
	<version>LATEST-VERSION</version>
	<scope>test</scope>
</dependency>
```

## Create a cluster
```java
KafkaCluster cluster = KafkaCluster.builder()
				.withZookeeper("127.0.0.1", 2181)
				.withBroker(1, "127.0.0.1", 9092)
				.withBroker(2, "127.0.0.1", 19092)
				.build();
```

## Start the cluster
```java
cluster.start();
```
## Stop the cluster
```java
cluster.shutdown();
```
## Create a consumer
```java
Properties p = new Properties();
p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
p.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
Consumer<String, String> consumer = cluster.consumer(p, new StringDeserializer(), new StringDeserializer());
```
## Create a producer
```java
Producer<String, String> producer = cluster.producer(new Properties(), new StringSerializer(), new StringSerializer());
```

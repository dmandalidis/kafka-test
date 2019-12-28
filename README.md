# Kafka Cluster unit testing framework

[![Build Status](https://travis-ci.org/dmandalidis/kafka-cluster-unit.svg?branch=master)](https://travis-ci.org/dmandalidis/kafka-cluster-unit)
[![codecov](https://codecov.io/github/dmandalidis/kafka-cluster-unit/coverage.svg?branch=master)](https://codecov.io/github/dmandalidis/kafka-cluster-unit?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.mandas.kafka/kafka-cluster-unit.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.mandas.kafka%22%20kafka-cluster-unit)
[![License](https://img.shields.io/github/license/dmandalidis/kafka-cluster-unit.svg)](LICENSE)
[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=dmandalidis/kafka-cluster-unit)](https://dependabot.com)

This is a Java unit test framework for testing scenarios requiring use of [Apache Kafka](http://kafka.apache.org)

# Features

* Cluster support
* Fast startup

# Kafka version support

| Version | Kafka version |
| --- | --- |
| 1.0.x | 0.10.x |
| 2.0.0 | 0.11.x |
| 2.0.4 | 1.1.0 |
| 2.0.5 | 2.0.x |
| 2.1.0 | 2.1.x |
| 2.2.0 | 2.2.x |
| 2.3.0 | 2.3.x |
| 2.4.0 | 2.4.x |

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

## Have Junit Rule handle cluster lifecycle
```java
@Rule
public KafkaClusterRule rule = new KafkaClusterRule(2, 10000, 11000); // This will manage 2 kafka brokers at port 10000-11000
```

## Create a cluster manually
```java
KafkaCluster cluster = KafkaCluster.builder()
				.withZookeeper("127.0.0.1", 10000, 11000)
				.withBroker(1, "127.0.0.1", 10000, 11000)
				.withBroker(2, "127.0.0.1", 10000, 11000)
				.build();
```

## Start the cluster manually
```java
cluster.start();
```
## Stop the cluster manually
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

/*
 * kafka-cluster-unit
 * 
 * Copyright 2017 Dimitris Mandalidis
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.mandas.kafka;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * {@code KafkaCluster} represents a Kafka cluster with one or more brokers
 * and a ZooKeeper server
 * 
 *<pre>
 *{@code KafkaCluster cluster = KafkaCluster.builder()
 *	.withZookeeper("127.0.0.1", 2181, 5)
 *	.withBroker(1, "127.0.0.1", 9092)
 *	.withBroker(2, "127.0.0.1", 19092)
 *	.build();
 *		
 *cluster.start();
 *	
 *[...]
 *
 *cluster.shutdown();	
 *}
 *</pre>
 *	@author Dimitris Mandalidis
 *
 */
public class KafkaCluster {

	private final Zk zk;
	
	private final Map<Integer, KafkaBroker> brokers;

	private final Path baseLogPath;

	/**
	 * Creates a new {@link KafkaClusterBuilder}
	 * @return a new {@link KafkaClusterBuilder}
	 */
	public static KafkaClusterBuilder builder() {
		String tmpDir = System.getProperty("java.io.tmpdir");
		Path base = Paths.get(tmpDir)
			.resolve("kafka-cluster-unit")
			.resolve(String.valueOf(new Random().nextInt()));
		
		return new KafkaClusterBuilder(base);
	}
	
	/**
	 * Get the broker connection string of this cluster
	 * @return the broker connection string of this cluster
	 */
	public String brokers() {
		return brokers.values().stream().map(KafkaBroker::getListener).collect(Collectors.joining(","));
	}
	
	private Properties defaultProperties() {
		Properties prop = new Properties();
		prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers());
		
		return prop;
	}
	
	/**
	 * Get a new {@link Consumer} for this cluster
	 * @param properties additional consumer properties
	 * @return a new {@link Consumer} for this cluster
	 * @param <K> the key type
	 * @param <V> the value type
	 */
	public <K, V> Consumer<K, V> consumer(Properties properties) {
		Properties prop = new Properties();
		prop.putAll(defaultProperties());
		prop.putAll(properties);
		
		return new KafkaConsumer<K, V>(prop);
	}
	
	/**
	 * Get a new {@link Consumer} for this cluster
	 * @param properties additional consumer properties
	 * @param keyDeserializer the key deserializer
	 * @param valueDeserializer the value deserializer
	 * @return a new {@link Consumer} for this cluster
	 * @param <K> the key type
	 * @param <V> the value type
	 */
	public <K, V> Consumer<K, V> consumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		Properties prop = new Properties();
		prop.putAll(defaultProperties());
		prop.putAll(properties);
		
		return new KafkaConsumer<>(prop, keyDeserializer, valueDeserializer);
	}
	
	/**
	 * Get a new {@link Producer} for this cluster
	 * @param properties additional producer properties
	 * @return a new {@link Producer} for this cluster
	 * @param <K> the key type
	 * @param <V> the value type
	 */
	public <K, V> Producer<K, V> producer(Properties properties) {
		Properties prop = new Properties();
		prop.putAll(defaultProperties());
		prop.putAll(properties);
		
		return new KafkaProducer<>(prop);
	}
	
	/**
	 * Get a new {@link Producer} for this cluster
	 * @param properties additional producer properties
	 * @param keySerializer the key serializer
	 * @param valueSerializer the value serializer
	 * @return a new {@link Producer} for this cluster
	 * @param <K> the key type
	 * @param <V> the value type
	 */
	public <K, V> Producer<K, V> producer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		Properties prop = new Properties();
		prop.putAll(defaultProperties());
		prop.putAll(properties);
		
		return new KafkaProducer<>(prop, keySerializer, valueSerializer);
	}
	
	/**
	 * Shutdown this cluster
	 */
	public void shutdown() {
		for (KafkaBroker broker: brokers.values()) {
			broker.stop();
		}
		this.zk.stop();
		
		try {
			Files.walkFileTree(baseLogPath, recursiveDeleteVisitor);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Start this cluster
	 */
	public void start() {
		try {
			this.zk.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		for (KafkaBroker broker: brokers.values()) {
			broker.start();
		}
	}
	
	/**
	 * Create a topic (replication factor defaults to the number of available brokers
	 * @param topic the topic name
	 * @param partitions the number of partitions
	 */
	public void createTopic(String topic, int partitions) {
		this.zk.createTopic(topic, partitions, brokers.size());
	}
	
	private KafkaCluster(Zk zk, Map<Integer, KafkaBroker> brokers, Path baseLogPath) {
		this.zk = zk;
		this.brokers = brokers;
		this.baseLogPath = baseLogPath;
	}
	
	/**
	 * Builder for {@link KafkaCluster}
	 * @author Dimitris Mandalidis
	 *
	 */
	public static class KafkaClusterBuilder {
		
		private Zk zk;
		
		private Map<Integer, KafkaBroker> brokers = new HashMap<>();

		private final Path base;

		KafkaClusterBuilder(Path base) {
			this.base = base;
		}
		
		/**
		 * Modifies this builder, attaching Zookeeper server coordinates
		 * @param host the hostname that Zookeeper will listen on
		 * @param port the port that Zookeeper will bind on
		 * @param maxClientCnxns the maximum number of allowed client connections
		 * @return this
		 */
		public KafkaClusterBuilder withZookeeper(String host, int port, int maxClientCnxns) {
			Path dataDir = base.resolve("zk").resolve("data");
			Path snapDir = base.resolve("zk").resolve("log");
			zk = new Zk(dataDir, snapDir, host, port, maxClientCnxns);
			return this;
		}
		
		/**
		 * Modifies this builder, attaching a new Kafka broker
		 * @param id the broker id
		 * @param host the hostname that this broker will listen on
		 * @param port the port this broker will bind on
		 * @return this
		 * @throws IllegalStateException if the Zookeeper server has not been initialized
		 */
		public KafkaClusterBuilder withBroker(int id, String host, int port) {
			if (zk == null) {
				throw new IllegalStateException("A Kafka broker needs a Zookeeper connection");
			}
			Path logDir = base.resolve("kafka").resolve(String.valueOf(id)).resolve("log");
			KafkaBroker broker = new KafkaBroker(logDir, id, zk.getConnection(), host, port);
			brokers.put(id, broker);
			return this;
		}
		
		/**
		 * Build a {@link KafkaCluster} instance
		 * @return a new {@link KafkaCluster} instance
		 * @throws IllegalStateException if a Zookeeper server has not been configured
		 * @throws IllegalStateException if no Kafka brokers were specified
		 */
		public KafkaCluster build() {
			if (zk == null) {
				throw new IllegalStateException("You cannot build a KafkaCluster without a Zookeeper server");
			}
			if (brokers.isEmpty()) {
				throw new IllegalStateException("You cannot build a KafkaCluster without Kafka brokers");
			}
			return new KafkaCluster(zk, brokers, base);
		}
	}
	
	private FileVisitor<Path> recursiveDeleteVisitor = new SimpleFileVisitor<Path>() {

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			FileVisitResult result = super.visitFile(file, attrs);
			if (result != FileVisitResult.CONTINUE) {
				return result;
			}
			Files.delete(file);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			FileVisitResult result = super.postVisitDirectory(dir, exc);
			if (result != FileVisitResult.CONTINUE) {
				return result;
			}
			Files.delete(dir);
			return FileVisitResult.CONTINUE;
		}
	};
}

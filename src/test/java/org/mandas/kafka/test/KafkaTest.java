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
package org.mandas.kafka.test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.mandas.kafka.KafkaCluster;

public class KafkaTest { 

	@Test
	public void testCluster() throws Exception {
		KafkaCluster cluster = KafkaCluster.builder()
				.withZookeeper("127.0.0.1", 2181, 5)
				.withBroker(1, "127.0.0.1", 9092)
				.withBroker(2, "127.0.0.1", 19092)
				.build();
		
		cluster.start();
		
		cluster.createTopic("topic", 3);
		
		StringSerializer serializer = new StringSerializer();
		Producer<String, String> producer = cluster.producer(new Properties(), serializer, serializer);

		StringDeserializer deserializer = new StringDeserializer();
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic", "key", "foobar");
		producer.send(record);
		
		Properties p = new Properties();
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
		Consumer<String, String> consumer = cluster.consumer(p, deserializer, deserializer);
		
		consumer.subscribe(Arrays.asList("topic"));
		
		ConsumerRecords<String,String> records = consumer.poll(200L);
		assertEquals(1, records.count());
		
		cluster.shutdown();
	}
	
	@Test(expected=IllegalStateException.class)
	public void testNoZk() {
		KafkaCluster.builder()
				.withBroker(1, "127.0.0.1", 9092)
				.build();
	}
	
	@Test(expected=IllegalStateException.class)
	public void testNoBrokers() {
		KafkaCluster.builder()
				.withZookeeper("127.0.0.1", 2181, 5)
				.build();
	}
}

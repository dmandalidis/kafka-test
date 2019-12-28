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

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.mandas.kafka.KafkaCluster;
import org.mandas.kafka.KafkaClusterRule;

public class KafkaTest { 

	@Rule
	public KafkaClusterRule rule = new KafkaClusterRule(2, 10000, 11000);
	
	@Test
	public void testCluster() throws Exception {
		rule.cluster().createTopic("topic", 3);
		
		StringSerializer serializer = new StringSerializer();
		StringDeserializer deserializer = new StringDeserializer();
		
		try (Producer<String, String> producer = rule.cluster().producer(new Properties(), serializer, serializer)) {
			ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "foobar");
			producer.send(record).get();
		}
		
		Properties p = new Properties();
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
		try (Consumer<String, String> consumer = rule.cluster().consumer(p, deserializer, deserializer)) {
			consumer.subscribe(Arrays.asList("topic"));
			consumer.poll(ofSeconds(0L));
			
			ConsumerRecords<String,String> records = consumer.poll(ofSeconds(1L));
			assertEquals(1, records.count());
			ConsumerRecord<String, String> record = records.iterator().next();
			assertEquals("key", record.key());
			assertEquals("foobar", record.value());
			
			consumer.unsubscribe();
		}
	}
	
	@Test(expected=IllegalStateException.class)
	public void testNoZk() {
		KafkaCluster.builder()
				.withBroker(1, "127.0.0.1", 10000, 11111)
				.build();
	}
	
	@Test(expected=IllegalStateException.class)
	public void testNoBrokers() {
		KafkaCluster.builder()
				.withZookeeper("127.0.0.1", 10000, 11111)
				.build();
	}
}

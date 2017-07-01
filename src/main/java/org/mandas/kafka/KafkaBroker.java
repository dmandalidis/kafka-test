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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

class KafkaBroker {
	
	private final String listener;

	private final Map<String, Object> props;

	private KafkaServerStartable kafka;
	
	KafkaBroker(Path logDir, int brokerId, String zkConnect, String host, int port) {
		listener = String.format("%s:%d", host, port);
		props = new HashMap<>();
		props.put("zookeeper.connect", zkConnect);
		props.put("listeners", String.format("PLAINTEXT://%s", listener));
		props.put("log.dir", logDir.toString());
		props.put("broker.id", brokerId);
		props.put("offsets.topic.num.partitions", "1");
		props.put("auto.create.topics.enable", false);
	}

	String getListener() {
		return listener;
	}
	
	void start(int brokers) {
		props.put("offsets.topic.replication.factor", (short) brokers);
		KafkaConfig config = new KafkaConfig(props);
		kafka = new KafkaServerStartable(config);
		kafka.startup();
	}
	
	void stop() {
		kafka.shutdown();
	}
}
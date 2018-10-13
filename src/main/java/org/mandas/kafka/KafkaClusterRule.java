package org.mandas.kafka;

import static java.util.Collections.emptyMap;

import java.util.Map;

import org.junit.rules.ExternalResource;
import org.mandas.kafka.KafkaCluster.KafkaClusterBuilder;

public class KafkaClusterRule extends ExternalResource {

	private final KafkaCluster cluster;

	public KafkaClusterRule(int brokers, int portStart, int portEnd) {
		this(brokers, portStart, portEnd, emptyMap());
	}
	
	public KafkaClusterRule(int brokers, int portStart, int portEnd, Map<String, Object> properties) {
		KafkaClusterBuilder builder = KafkaCluster.builder().withZookeeper("127.0.0.1", portStart, portEnd);
		for (int i = 1; i <= brokers; i++) {
			builder.withBroker(i, "127.0.0.1", portStart, portEnd, properties);
		}
		cluster = builder.build();
	}

	public KafkaCluster cluster() {
		return cluster;
	}
	
	@Override
	protected void before() throws Throwable {
		cluster.start();
	}
	
	@Override
	protected void after() {
		try {
			cluster.shutdown();
		} catch (Exception e) {
		}
	}
}

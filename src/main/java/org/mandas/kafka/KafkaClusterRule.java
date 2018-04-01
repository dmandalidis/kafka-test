package org.mandas.kafka;

import static java.util.concurrent.ThreadLocalRandom.current;

import org.junit.rules.ExternalResource;
import org.mandas.kafka.KafkaCluster.KafkaClusterBuilder;

public class KafkaClusterRule extends ExternalResource {

	private final KafkaCluster cluster;

	public KafkaClusterRule() {
		cluster = KafkaCluster.builder().withZookeeper("127.0.0.1", 2181).withBroker(1, "127.0.0.1", 9092).build();
	}
	
	public KafkaClusterRule(int brokers, int portStart, int portEnd) {
		KafkaClusterBuilder builder = KafkaCluster.builder().withZookeeper("127.0.0.1", current().nextInt(portStart, portEnd));
		for (int i = 1; i <= brokers; i++) {
			builder.withBroker(i, "127.0.0.1", current().nextInt(portStart, portEnd));
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

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

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;

class Zk {
	
	private final Path dataDir;
	private final Path snapDir; 
	private final String host;
	private final int port;
	private final int maxClientCnxns;
	private final ZooKeeperServer server;
	
	private final ZkSerializer serializer = new ZkSerializer() {
		
		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			return ZKStringSerializer.serialize(data);
		}
		
		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			return ZKStringSerializer.deserialize(bytes);
		}
	};
	
	Zk(Path dataDir, Path snapDir, String host, int port, int maxClientCnxns) {
		this.dataDir = dataDir;
		this.snapDir = snapDir;
		this.host = host;
		this.port = port;
		this.maxClientCnxns = maxClientCnxns;
		this.server = new ZooKeeperServer();
	}
	
	String getConnection() {
		return String.format("%s:%d", host, port);
	}
	
	
	void start() throws Exception {
		InetSocketAddress addr = new InetSocketAddress(host, port);
		ServerCnxnFactory factory = ServerCnxnFactory.createFactory(addr, maxClientCnxns);
		server.setServerCnxnFactory(factory);
		FileTxnSnapLog snapLog = new FileTxnSnapLog(dataDir.toFile(), snapDir.toFile());
		server.setTxnLogFactory(snapLog);
		ZKDatabase zkDb = new ZKDatabase(snapLog);
		server.setZKDatabase(zkDb);
		factory.startup(server);
	}
	
	void createTopic(String topic, int partitions, int replicas) {
		createTopic(topic, partitions, replicas, new Properties(), 5000, 5000);
	}
	
	void createTopic(String topic, int partitions, int replicas, Properties properties, int sessionTimeout, int connectionTimeout) {
		String connection = getConnection();
		ZkClient client = new ZkClient(connection, sessionTimeout, connectionTimeout, serializer);
		ZkUtils utils = new ZkUtils(client, new ZkConnection(connection), false);
		AdminUtils.createTopic(utils, topic, partitions, replicas, properties, RackAwareMode.Enforced$.MODULE$);
	}
	
	void stop() {
		server.shutdown();
	}
}
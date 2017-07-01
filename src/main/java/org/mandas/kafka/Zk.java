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

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

class Zk {
	
	private final Path dataDir;
	private final Path snapDir; 
	private final String host;
	private final int port;
	private final int maxClientCnxns;
	private final ZooKeeperServer server;
	
	Zk(Path dataDir, Path snapDir, String host, int port, int maxClientCnxns) {
		this.dataDir = dataDir;
		this.snapDir = snapDir;
		this.host = host;
		this.port = port;
		this.maxClientCnxns = maxClientCnxns;
		this.server = new ZooKeeperServer();
	}
	
	String getConnectionString() {
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
	
	void stop() {
		server.getServerCnxnFactory().shutdown();
	}
}
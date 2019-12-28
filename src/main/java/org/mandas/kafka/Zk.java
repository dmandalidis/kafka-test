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
import java.nio.file.Path;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Zk {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Zk.class);
	
	private final Path dataDir;
	private final Path snapDir; 
	private final String host;
	private final int port;
	private final int maxClientCnxns;

	private Thread server;
	
	Zk(Path dataDir, Path snapDir, String host, int port, int maxClientCnxns) {
		this.dataDir = dataDir;
		this.snapDir = snapDir;
		this.host = host;
		this.port = port;
		this.maxClientCnxns = maxClientCnxns;
	}
	
	String getConnectionString() {
		return String.format("%s:%d", host, port);
	}
	
	
	void start() throws Exception {
		Properties startupProperties = new Properties();
		startupProperties.put("clientPort", port);
		startupProperties.put("dataDir", dataDir.toFile());
		startupProperties.put("snapDir", snapDir.toFile());
		startupProperties.put("maxClientCnxns", maxClientCnxns);
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
		    quorumConfiguration.parseProperties(startupProperties);
		} catch(Exception e) {
		    throw new RuntimeException(e);
		}

		ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);

		server = new Thread() {
		    @Override
			public void run() {
		        try {
		            zooKeeperServer.runFromConfig(configuration);
		        } catch (IOException | AdminServerException e) {
		        	LOGGER.error("Error starting zookeeper ", e);
		        	throw new RuntimeException(e);
		        }
		    }
		};
		
		server.start();
	}
	
	void stop() {
		server.interrupt();
	}
}
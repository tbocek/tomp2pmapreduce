/* 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.mapreduce.examplejob;

import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.mapreduce.PeerConnectionCloseListener;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;

/**
 * Example client to be run on any node that is not the job submitter. Bootstrapping node below has id 2. All other nodes have an id different from 2 and need to connect to the bootstrapper.
 * Additionally, IP and port to connect to from the bootstrapping node need to be provided. The first one starting this method should be the bootstrapping node to which all other peers (id other than
 * 2) connect to.
 * 
 * @see <a href="http://tinyurl.com/csgmtmapred">Documentation</a>
 * @author Oliver Zihler
 *
 */
public class MainBootstrapperAndWorker {
	private static final Logger LOG = LoggerFactory.getLogger(MainBootstrapperAndWorker.class);

	public static void main(String[] args) throws Exception {
		// These configurations are needed due to a bug not resolved yet
		ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_TCP_IDLE_MILLIS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP = Integer.MAX_VALUE;
		PeerConnectionCloseListener.WAITING_TIME = Integer.MAX_VALUE;

		// how long the get method should wait until it actually starts retrieving the data from the dht
		PeerMapReduce.DEFAULT_WAITING_TIME = 5000;

		// peer id generator. Bootstrapper should be ID 2. Can be adapted as needed. Any node that is not the
		// bootstrapper needs to have another id assigned.
		int peerCounter = 2;
		boolean isBootstrapper = peerCounter == 2;
		// Bootstrapping node IP
		String bootstrapperIP = "192.168.0.19";
		// Bootstrapping node port
		int bootstrapperPort = 4004;

		// Peer configuration
		Number160 peerId = new Number160(peerCounter);

		PeerMapConfiguration pmc = new PeerMapConfiguration(peerId);
		pmc.peerNoVerification();
		PeerMap pm = new PeerMap(pmc);
		PeerBuilder peerBuilder = new PeerBuilder(peerId).peerMap(pm).ports(bootstrapperPort);

		// connection to the DHT
		PeerMapReduce peerMapReduce = new PeerMapReduce(peerBuilder);

		// Connect to the bootstrapping node. peer with ID 2 in this case is the bootstrapper and does not need to be
		// connected. All other nodes connect to peer 2.
		if (!isBootstrapper) {
			peerMapReduce.peer().bootstrap().inetAddress(InetAddress.getByName(bootstrapperIP)).ports(bootstrapperPort).start().awaitUninterruptibly()
					.addListener(new BaseFutureAdapter<FutureBootstrap>() {

						@Override
						public void operationComplete(FutureBootstrap future) throws Exception {
							if (future.isSuccess()) {
								LOG.info("successfully bootstrapped to " + bootstrapperIP + "/" + bootstrapperPort);
							} else {
								LOG.warn("No success on bootstrapping: fail reason: " + future.failedReason());
							}
						}

					});
		}
	}
}

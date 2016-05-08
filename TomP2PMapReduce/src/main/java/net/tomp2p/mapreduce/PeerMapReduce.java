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
package net.tomp2p.mapreduce;

import java.util.NavigableMap;
import java.util.Random;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * Main connection point to the network. Get and Put methods can be used to store and retrieve serialised data to and
 * from the DHT. Furthermore, the contained <code>Peer</code> can directly be accessed to allow for broadcast emissions.
 * See also the documentation <a href="http://tinyurl.com/csgmtmapred">here</a>, chapter 5.
 * 
 * @author Oliver Zihler
 *
 */
public class PeerMapReduce {
	public static int DEFAULT_WAITING_TIME = 3000;
	private static final Random RND = new Random();

	/** Peer to connect to the DHT and other peers */
	private Peer peer;
	/** Broadcast handler that receives broadcast messages and executes IMapReduceBroadcastReceiver instances */
	private MapReduceBroadcastHandler broadcastHandler;
	/** Actual network access */
	private TaskRPC taskRPC;
	/** Waiting time before the actual get request is conducted. Default is 3 seconds. */
	private int waitingTime = DEFAULT_WAITING_TIME;

	public PeerMapReduce(PeerBuilder peerBuilder) {
		this(peerBuilder, DEFAULT_WAITING_TIME);
	}

	public PeerMapReduce(PeerBuilder peerBuilder, int waitingTime) {
		try {
			this.waitingTime = waitingTime;
			this.broadcastHandler = new MapReduceBroadcastHandler();
			this.peer = peerBuilder.broadcastHandler(broadcastHandler).start();
			this.broadcastHandler.peerMapReduce(this);
			this.taskRPC = new TaskRPC(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Puts serialised data into the DHT.
	 * 
	 * @param locationKey
	 *            main key, specifies on which peer the data resides
	 * @param domainKey
	 *            used to distinguish the same locationKey
	 * @param value
	 *            the actual value to put in the DHT.
	 * @param nrOfExecutions
	 *            how many time the value put into the DHT can be accessed
	 * @return
	 */
	public MapReducePutBuilder put(Number160 locationKey, Number160 domainKey, Object value, int nrOfExecutions) {
		return new MapReducePutBuilder(this, locationKey, domainKey).data(value, nrOfExecutions);
	}

	/**
	 * Get the data from the DHT. If the peer requesting the data fails, the same broadcast as before needs to be
	 * distributed again for other peers to execute the failed task. This requires the complete input the task that
	 * invokes this method received via broadcast.
	 * 
	 * @param locationKey
	 *            main key, specifies on which peer the data resides
	 * @param domainKey
	 *            used to distinguish the same locationKey
	 * @param broadcastInput
	 *            complete input the task that invokes this method received.
	 * @return
	 */
	public MapReduceGetBuilder get(Number160 locationKey, Number160 domainKey,
			NavigableMap<Number640, Data> broadcastInput) {
		try {
			int nextInt = RND.nextInt(waitingTime);
			Thread.sleep(nextInt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new MapReduceGetBuilder(this, locationKey, domainKey).broadcastInput(broadcastInput);
	}

	/**
	 * 
	 * @param locationKey
	 * @param domainKey
	 * @param broadcastInput
	 * @param waitingTime
	 *            maximal time in milliseconds to wait until get is invoked.. Will not be stored! If needs to be reused,
	 *            use the corresponding setter.
	 * @return
	 */
	public MapReduceGetBuilder get(Number160 locationKey, Number160 domainKey,
			NavigableMap<Number640, Data> broadcastInput, int waitingTime) {
		try {
			int nextInt = RND.nextInt(waitingTime);
			Thread.sleep(nextInt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new MapReduceGetBuilder(this, locationKey, domainKey).broadcastInput(broadcastInput);
	}

	/**
	 * 
	 * @return peer to send broadcast or configure
	 */
	public Peer peer() {
		return this.peer;
	}

	/**
	 * 
	 * @return actual broadcast handler, mainly used for internal mechanisms at the moment.
	 */
	public MapReduceBroadcastHandler broadcastHandler() {
		return this.broadcastHandler;
	}

	/**
	 * 
	 * @return can be accessed to use e.g. the storage object directly.
	 */
	public TaskRPC taskRPC() {
		return this.taskRPC;
	}

	/**
	 * 
	 * @param waitingTime
	 *            maximal time in milliseconds to wait until get is invoked. The actual time waited is a random number
	 *            between 0 and waitingTime.
	 */
	public void waitingTime(int waitingTime) {
		this.waitingTime = waitingTime;
	}
}

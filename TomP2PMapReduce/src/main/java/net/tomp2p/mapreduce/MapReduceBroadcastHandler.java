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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.StructuredBroadcastHandler;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

/**
 * 
 * Handles all incoming broadcasts by passing them to {@link IMapReduceBroadcastReceiver#receive(Message, PeerMapReduce)}. The received broadcast message needs to at least contain the following keys:
 * {@link NumberUtils#OUTPUT_STORAGE_KEY} defines the next task to execute and is also currently needed to ignore multiple same broadcast messages received. {@link NumberUtils#INPUT_STORAGE_KEY} the
 * key for the data on which the previous task was executed. If null, it means that this message corresponds to an initial task output as there was no previous task to execute.
 * {@link NumberUtils#SENDER} to determine who sent the broadcast. Instance of {@link PeerAddress}. {@link NumberUtils#RECEIVERS} to deserialise and instantiate user-defined implementations of
 * {@link IMapReduceBroadcastReceiver}. Should be sent with every broadcast to allow joining nodes to immediately start execution.
 * 
 * @author Oliver Zihler
 *
 */
public class MapReduceBroadcastHandler extends StructuredBroadcastHandler {
	private static Logger logger = LoggerFactory.getLogger(MapReduceBroadcastHandler.class);
	/** Duplicated messages */
        //TODO: we don't want to store the duplicates forever, in case of other peers fail, this one may 
        //need to do the job eventually
	private static Set<Number640> messages = Collections.synchronizedSet(new HashSet<>());
	/** All user-defined receivers specifying the actions on receiving a broadcast */
	private List<IMapReduceBroadcastReceiver> receivers = Collections.synchronizedList(new ArrayList<>());
	/** used for internal synchronisation mechanisms */
	private Set<PeerAddressStorageKeyTuple> receivedButNotFound = Collections.synchronizedSet(new HashSet<>());
	/** used for internal fault tolerance measures */
	private List<PeerConnectionActiveFlagRemoveListener> peerConnectionActiveFlagRemoveListeners = Collections.synchronizedList(new ArrayList<>());

	private ThreadPoolExecutor executor;
	private PeerMapReduce peerMapReduce;

	/**
	 * Defines Integer.MAX_VALUE as number of threads, meaning that all received execution requests are immediately conducted.
	 */
	public MapReduceBroadcastHandler() {
		this(Integer.MAX_VALUE);
	}

	/**
	 * 
	 * @param threads
	 *            number of parallel task executions on this node.
	 */
	public MapReduceBroadcastHandler(int threads) {
		this.executor = new ThreadPoolExecutor(threads, threads, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>());
	}

	@SuppressWarnings("unchecked")
	@Override
	public StructuredBroadcastHandler receive(Message message) {
		try {
			// Get the user-defined input from the message
			NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();

			// get the key to the data to skip additionally received duplicate messages --> NEEDS TO BE SOLVED (e.g. using timeouts)
			Data nextKeyData = input.get(NumberUtils.OUTPUT_STORAGE_KEY);
			if (nextKeyData != null) {
				Number640 nextKey = (Number640) nextKeyData.object();
				synchronized (messages) {
					if (messages.contains(nextKey)) { // Don't want duplicates
						return super.receive(message);
					} else {
						messages.add(nextKey);
					}
				}
			}
			// inform peerConnectionActiveFlagRemoveListeners about completed/finished data processing
			// newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
			// Skips in first execution where there is no input
			if (input.containsKey(NumberUtils.SENDER) && input.containsKey(NumberUtils.INPUT_STORAGE_KEY)) {
				PeerAddress peerAddress = (PeerAddress) input.get(NumberUtils.SENDER).object();
				Number640 storageKey = (Number640) input.get(NumberUtils.INPUT_STORAGE_KEY).object();
				informPeerConnectionActiveFlagRemoveListeners(peerAddress, storageKey);
			}

			// Receivers need to be generated and added if they did not exist yet
			if (input.containsKey(NumberUtils.RECEIVERS)) {
				instantiateReceivers(((List<TransferObject>) input.get(NumberUtils.RECEIVERS).object()));
			}
			// Call receivers with new input data...
			synchronized (receivers) {
				for (IMapReduceBroadcastReceiver receiver : receivers) {

					logger.info("RECEIVER: " + receiver.id());
					if (!executor.isShutdown()) {
						executor.execute(new Runnable() {

							@Override
							public void run() {
                                                            try {
								receiver.receive(message, peerMapReduce);
                                                            } catch (Exception e) {
                                                                e.printStackTrace();
                                                            }
							}
						});
					}
				}
			}
		} catch (Exception e) {
			logger.info("Exception caught", e);
		}
		return super.receive(message);
	}

	private void instantiateReceivers(List<TransferObject> receiverClasses) {
		for (TransferObject o : receiverClasses) {
			Map<String, Class<?>> rClassFiles = SerializeUtils.deserializeClassFiles(o.serialisedClassFiles());
			IMapReduceBroadcastReceiver receiver = (IMapReduceBroadcastReceiver) SerializeUtils.deserializeJavaObject(o.serialisedObject(), rClassFiles);
			synchronized (receivers) {
				for (IMapReduceBroadcastReceiver r : receivers) {
					if (r.id().equals(receiver.id())) {
						return;
					}
				}
				this.receivers.add(receiver);
			}
			logger.info("NUMBER OF RECEIVERS: " + this.receivers.size());
		}
	}

	private void informPeerConnectionActiveFlagRemoveListeners(PeerAddress sender, Number640 storageKey) throws ClassNotFoundException, IOException {
		List<PeerConnectionActiveFlagRemoveListener> toRemove = Collections.synchronizedList(new ArrayList<>());
		boolean successOnTurnOff = false;
		PeerAddressStorageKeyTuple triple = new PeerAddressStorageKeyTuple(sender, storageKey);
		if (peerMapReduce.peer().peerAddress().equals(sender)) {
			logger.info("I [" + peerMapReduce.peer().peerID().shortValue() + "] received bc from myself [" + triple + "]. Ignore");
			return;
		}
		synchronized (peerConnectionActiveFlagRemoveListeners) {
			for (PeerConnectionActiveFlagRemoveListener bL : peerConnectionActiveFlagRemoveListeners) {
				try {
					successOnTurnOff = bL.turnOffActiveOnDataFlag(triple);
					if (successOnTurnOff) {
						toRemove.add(bL);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			boolean removed = peerConnectionActiveFlagRemoveListeners.removeAll(toRemove);
			logger.info("Could remove listener on triple [" + triple + "]? [" + removed + "]");
		}

		if (!successOnTurnOff) {
			// Needs to save and check that for future RPCs
			logger.info("Possibly received triple before listener was added... triple[" + triple + "]");
			receivedButNotFound.add(triple);
		}
	}

	/**
	 * Used to shut down the {@link ThreadPoolExecutor} from tasks.
	 */
	public void shutdown() {
		try {
			executor.shutdown();
			int cnt = 0;
			while (!executor.awaitTermination(6, TimeUnit.SECONDS) && cnt++ >= 2) {
				logger.info("Await thread completion");
			}
			executor.shutdownNow();
		} catch (InterruptedException e) {
			logger.warn("Exception caught", e);
		}
	}

	public void addPeerConnectionRemoveActiveFlageListener(PeerConnectionActiveFlagRemoveListener peerConnectionActiveFlagRemoveListener) {
		logger.info("added listener for connection " + peerConnectionActiveFlagRemoveListener.tupleToAcquire());
		this.peerConnectionActiveFlagRemoveListeners.add(peerConnectionActiveFlagRemoveListener);
	}

	public MapReduceBroadcastHandler threadPoolExecutor(ThreadPoolExecutor e) {
		this.executor = e;
		return this;
	}

	/**
	 * Used internally
	 * 
	 * @return
	 */
	public Set<PeerAddressStorageKeyTuple> receivedButNotFound() {
		return this.receivedButNotFound;
	}

	public void peerMapReduce(PeerMapReduce peerMapReduce) {
		this.peerMapReduce = peerMapReduce;
	}
}
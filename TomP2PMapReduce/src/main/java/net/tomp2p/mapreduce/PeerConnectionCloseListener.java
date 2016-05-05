/* 
 * Copyright 2016 Oliver Zihler 
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.dht.Storage;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class PeerConnectionCloseListener extends BaseFutureAdapter<BaseFuture> {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionCloseListener.class);

	public static long WAITING_TIME = Integer.MAX_VALUE;

	private AtomicBoolean activeOnDataFlag;

	private Storage storage;

	private NavigableMap<Number640, Data> broadcastData;

	private Peer peer;

//	private Object value;

	private PeerAddressStorageKeyTuple requester;

	private Timer timer;

	public PeerConnectionCloseListener(AtomicBoolean activeOnDataFlag, PeerAddressStorageKeyTuple requester, Storage storage, NavigableMap<Number640, Data> broadcastData, Peer peer, Object value) {
		this.activeOnDataFlag = activeOnDataFlag;
		this.requester = requester;
		this.storage = storage;
		this.broadcastData = broadcastData;
		this.peer = peer;
//		this.value = value;
	}

	@Override
	public void operationComplete(BaseFuture future) throws Exception {
		if (future.isSuccess()) {
			this.timer = new Timer();
			timer.schedule(new TimerTask() {

				@Override
				public void run() {
					if (activeOnDataFlag.get()) {
						try {
							synchronized (storage) {
								Data data = storage.get(requester.storageKey);
								if (data != null) {
									MapReduceValue dST = (MapReduceValue) data.object();
									dST.tryDecrementCurrentNrOfExecutions(); // Makes sure the data is available again to another peer that tries to get it.
									storage.put(requester.storageKey, new Data(dST));
//									LOG.info("active is true: dST.tryDecrementCurrentNrOfExecutions() for " + requester + " and task " + (broadcastData.get(NumberUtils.NEXT_TASK).object()) + " value [ ]");
									peer.broadcast(new Number160(new Random())).dataMap(broadcastData).start();
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
//						try {
//							LOG.info("active was already set to aF[" + activeOnDataFlag.get() + "] for request " + requester + "and task " + (broadcastData.get(NumberUtils.NEXT_TASK).object()) + " value [ ]");
//						} catch (ClassNotFoundException | IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
					}
				}

			}, WAITING_TIME);
//			LOG.info("Started timer for " + requester + "and task " + (broadcastData.get(NumberUtils.NEXT_TASK).object()) + " value [ ]");

		} else {
			LOG.warn("!future.isSuccess() on PeerConnectionCloseListener and task " + (broadcastData.get(NumberUtils.NEXT_TASK).object()) + " value [ ], failed reason: " + future.failedReason());
		}
	}

}

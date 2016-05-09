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
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Responder;
import net.tomp2p.dht.Storage;
import net.tomp2p.dht.StorageMemory;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.storage.Data;

/**
 * Handles requests and responses.
 * 
 * @author Oliver Zihler
 *
 */
public class TaskRPC extends DispatchHandler {
	private static final Logger LOG = LoggerFactory.getLogger(TaskRPC.class);
	/** Actual storage object where the data on a node resides */
	private Storage storage;

	private PeerMapReduce peerMapReduce;

	public TaskRPC(final PeerMapReduce peerMapReduce) {
		super(peerMapReduce.peer().peerBean(), peerMapReduce.peer().connectionBean());
		this.storage = new StorageMemory();
		this.peerMapReduce = peerMapReduce;
                //TODO: introduce proper task for MapReduce
		register(RPC.Commands.GCM.getNr());
	}

	public void storage(Storage storage) {
		this.storage = storage;
	}

	/**
	 * Actual put invoked by {@link DistributedTask#putTaskData()}.
	 * 
	 * @param remotePeer
	 * @param taskDataBuilder
	 * @param channelCreator
	 * @return
	 */
	public FutureResponse putTaskData(final PeerAddress remotePeer, final MapReducePutBuilder taskDataBuilder,
			final ChannelCreator channelCreator) {
		// TODO: replace GCM with TASK
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_1);
		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			// the key for the values to put
			requestDataMap.dataMap().put(NumberUtils.OUTPUT_STORAGE_KEY,
					new Data(new Number640(taskDataBuilder.locationKey(), taskDataBuilder.domainKey(), Number160.ZERO,
							Number160.ZERO)));
			// The actual value to put
			requestDataMap.dataMap().put(NumberUtils.VALUE, new Data(taskDataBuilder.data()));
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
				peerBean(), connectionBean(), taskDataBuilder);

		if (taskDataBuilder.isForceUDP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	/**
	 * Actual get invoked by {@link DistributedTask#getTaskData(MapReduceGetBuilder, FutureMapReduceData)}
	 * 
	 * @param remotePeer
	 * @param taskDataBuilder
	 * @param channelCreator
	 * @return
	 */
	public FutureResponse getTaskData(final PeerAddress remotePeer, final MapReduceGetBuilder taskDataBuilder,
			final ChannelCreator channelCreator) {
		// TODO: replace GCM with TASK
		final Message message = createMessage(remotePeer, RPC.Commands.GCM.getNr(), Type.REQUEST_2).keepAlive(true);

		DataMap requestDataMap = new DataMap(new TreeMap<>());
		try {
			// the key for the values to put
			requestDataMap.dataMap().put(NumberUtils.OUTPUT_STORAGE_KEY,
					new Data(new Number640(taskDataBuilder.locationKey(), taskDataBuilder.domainKey(), Number160.ZERO,
							Number160.ZERO)));
			if (taskDataBuilder.broadcastInput() != null) { // Maybe null e.g. when job is used
				// Used to send the broadcast again if this connection fails
				requestDataMap.dataMap().put(NumberUtils.OLD_BROADCAST, new Data(taskDataBuilder.broadcastInput()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		message.setDataMap(requestDataMap);
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse,
				peerBean(), connectionBean(), taskDataBuilder);
		if (taskDataBuilder.isForceUDP()) {
			return requestHandler.fireAndForgetUDP(channelCreator);
		} else {
			return requestHandler.sendTCP(channelCreator);
		}
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign,
			Responder responder) throws Exception {
		if (!((message.type() == Type.REQUEST_1 || message.type() == Type.REQUEST_2)
				&& message.command() == RPC.Commands.GCM.getNr())) {
			throw new IllegalArgumentException("Message content is wrong for this handler.");
		}
		Message responseMessage = createResponseMessage(message, Type.NOT_FOUND);
		NavigableMap<Number640, Data> dataMap = message.dataMap(0).dataMap();

		Number640 storageKey = (Number640) dataMap.get(NumberUtils.OUTPUT_STORAGE_KEY).object();
		if (message.type() == Type.REQUEST_1) { // Put
			Data valueData = dataMap.get(NumberUtils.VALUE);
			storage.put(storageKey, valueData);
			responseMessage = createResponseMessage(message, Type.OK);
		} else if (message.type() == Type.REQUEST_2) {// Get
			Object value = null;
			// Try to acquire the value
			synchronized (storage) {
				Data valueData = storage.get(storageKey);
				if (valueData != null) {
					MapReduceValue dST = (MapReduceValue) valueData.object();
					value = dST.tryAcquireValue();
					storage.put(storageKey, new Data(dST));
					if (value == null) {
						responseMessage = createResponseMessage(message, Type.DENIED);
					}
					if (dST.nrOfExecutions() < 3) {
						LOG.info("value for key [" + storageKey.locationKey().intValue() + "] requested by peer ["
								+ (peerConnection != null ? peerConnection.remotePeer().peerId().shortValue()
										: peerMapReduce.peer().peerID().shortValue())
								+ "] is " + (value == null ? "[null]" : "[not null]")
								+ ", dst.currentNrOfExecutions(): " + dST.currentNrOfExecutions()
								+ ", possible nr of executions: [" + dST.nrOfExecutions() + "]");
					}
				}
			}
			if (value != null) {
				// LOG.info("Value is :" + value);
				responseMessage = createResponseMessage(message, Type.OK);
				// Add the value to the response message
				DataMap responseDataMap = new DataMap(new TreeMap<>());
				responseDataMap.dataMap().put(storageKey, new Data(value));
				responseMessage.setDataMap(responseDataMap);
				/*
				 * Add listener to peer connection such that if the connection dies, the broadcast is sent once again
				 * Add a broadcast listener that, in case it receives the broadcast, sets the flag of the peer
				 * connection listener to false, such that the connection listener is not invoked anymore
				 */
				if (peerConnection == null) { // This means its directly connected to himself
					// Do nothing, data on this peer is lost anyways if this peer dies
					// LOG.info("Acquired data from myself");
				} else {

					PeerAddressStorageKeyTuple senderTriple = new PeerAddressStorageKeyTuple(
							peerConnection.remotePeer(), storageKey);
					Set<PeerAddressStorageKeyTuple> receivedButNotFound = peerMapReduce.broadcastHandler()
							.receivedButNotFound();
					synchronized (receivedButNotFound) {
						if (receivedButNotFound.contains(senderTriple)) { // this means we received the broadcast before
																			// we received the get request for this item
																			// from this sender --> invalid/outdated
																			// request
							// LOG.info("Received Get request from [" + senderTriple + "], but was already received once
							// and not found. responseMessage = createResponseMessage(message,
							// Type.NOT_FOUND);");
							responseMessage = createResponseMessage(message, Type.NOT_FOUND);
							// senderTriple.nrOfAcquires--;
						} else {// Only here it is valid
							if (dataMap.containsKey(NumberUtils.OLD_BROADCAST)) { // If it is null, there is no sense
								// in decrementing it again as nobody will receive a broadcast... -->e.g in case of job
								// LOG.info("Will add senderTriple [" + senderTriple + "] to bc handler");
								final AtomicBoolean activeOnDataFlag = new AtomicBoolean(true);
								peerMapReduce.broadcastHandler().addPeerConnectionRemoveActiveFlageListener(
										new PeerConnectionActiveFlagRemoveListener(senderTriple, activeOnDataFlag));
								@SuppressWarnings("unchecked")
								NavigableMap<Number640, Data> oldBCInput = MapReduceGetBuilder
										.reconvertByteArrayToData((NavigableMap<Number640, byte[]>) dataMap
												.get(NumberUtils.OLD_BROADCAST).object());
								peerConnection.closeFuture()
										.addListener(new PeerConnectionCloseListener(activeOnDataFlag, senderTriple,
												storage, oldBCInput, peerMapReduce.peer(), value));
							}
						}

					}
				}

			}
		}
		if (message.isUdp()) {
			responder.responseFireAndForget();
		} else {
			responder.response(responseMessage);
		}
	}

	public PeerMapReduce peerMapReduce() {
		return this.peerMapReduce;
	}

	public Storage storage() {
		return storage;
	}
}

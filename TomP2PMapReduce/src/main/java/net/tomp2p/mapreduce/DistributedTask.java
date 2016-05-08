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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.DistributedRouting;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.builder.RoutingBuilder;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

/**
 * Stores and retrieves data to and from the DHT
 * 
 * @author Oliver Zihler
 *
 */
public class DistributedTask {
	final private static Logger logger = LoggerFactory.getLogger(DistributedTask.class);
	private static final NavigableSet<PeerAddress> EMPTY_NAVIGABLE_SET = new TreeSet<PeerAddress>();

	final private DistributedRouting routing;

	final private TaskRPC asyncTask;

	public DistributedTask(DistributedRouting routing, TaskRPC asyncTask) {
		this.routing = routing;
		this.asyncTask = asyncTask;
	}

	/**
	 * stores data for a task in the DHT. The node that is close to the locationKey will get the data.
	 * 
	 */
	public FutureMapReduceData putTaskData(final MapReducePutBuilder builder, final FutureMapReduceData data) {
		builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					final RoutingBuilder routingBuilder = createBuilder(builder);
					final FutureRouting futureRouting = routing.route(routingBuilder, Type.REQUEST_1,
							future.channelCreator());

					data.futureRouting(futureRouting);
					futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
						@Override
						public void operationComplete(final FutureRouting futureRouting) throws Exception {
							if (futureRouting.isSuccess()) {
								parallelRequests(builder.requestP2PConfiguration(), EMPTY_NAVIGABLE_SET,
										futureRouting.potentialHits(), data, false, future.channelCreator(),
										new MapReduceOperationMapper() {

									@Override
									public FutureResponse create(ChannelCreator channelCreator, PeerAddress address) {
										return asyncTask.putTaskData(address, builder, channelCreator);
									}

									@Override
									public void response(FutureMapReduceData futureTask,
											FutureDone<Void> futuresCompleted) {
										futureTask.done(futuresCompleted); // give raw data
									}

									@Override
									public void interMediateResponse(FutureResponse future) {
									}
								});
							} else {
								data.failed(futureRouting);
							}
						}
					});
					data.addFutureDHTReleaseListener(future.channelCreator());
				} else {
					data.failed(future);
				}
			}

		});
		return data;
	}

	private static <K extends BaseMapReduceBuilder<K>> RoutingBuilder createBuilder(BaseMapReduceBuilder<K> builder) {
		RoutingBuilder routingBuilder = new RoutingBuilder();
		routingBuilder.parallel(builder.routingConfiguration().parallel());
		routingBuilder.setMaxNoNewInfo(
				builder.routingConfiguration().maxNoNewInfo(builder.requestP2PConfiguration().minimumResults()));
		routingBuilder.maxDirectHits(builder.routingConfiguration().maxDirectHits());
		routingBuilder.maxFailures(builder.routingConfiguration().maxFailures());
		routingBuilder.maxSuccess(builder.routingConfiguration().maxSuccess());
		routingBuilder.locationKey(builder.locationKey());
		routingBuilder.domainKey(builder.domainKey());
		return routingBuilder;
	}

	/**
	 * Gets the data from the DHT. Retrieves it only if the data item is accessible by floor(N/2) nodes, where N is the
	 * number of nodes that contain the data.
	 */
	public FutureMapReduceData getTaskData(final MapReduceGetBuilder builder, final FutureMapReduceData futureTask) {
		builder.futureChannelCreator().addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					final RoutingBuilder routingBuilder = createBuilder(builder);
					final FutureRouting futureRouting = routing.route(routingBuilder, Type.REQUEST_1,
							future.channelCreator());

					futureTask.futureRouting(futureRouting);
					futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
						@Override
						public void operationComplete(final FutureRouting futureRouting) throws Exception {
							if (futureRouting.isSuccess()) {
								Map<String, Integer> deniedCntr = Collections.synchronizedMap(new HashMap<>());
								Map<String, Integer> receivedCntr = Collections.synchronizedMap(new HashMap<>());

								parallelRequests(builder.requestP2PConfiguration(), EMPTY_NAVIGABLE_SET,
										futureRouting.potentialHits(), futureTask, false, future.channelCreator(),
										new MapReduceOperationMapper() {
									Map<PeerAddress, Map<Number640, Data>> rawData = new HashMap<PeerAddress, Map<Number640, Data>>();

									@Override
									public FutureResponse create(ChannelCreator channelCreator, PeerAddress address) {
										return asyncTask.getTaskData(address, builder, channelCreator);
									}

									@Override
									public void response(FutureMapReduceData futureTask,
											FutureDone<Void> futuresCompleted) {
										// futureTask.done(futuresCompleted);
										// give raw data
										// logger.info("RESPONSE: rawData: "+rawData.size());
										int peerId = asyncTask.peerMapReduce().peer().peerID().intValue();
										String recip = peerId + "_" + builder.locationKey() + "_" + builder.domainKey();
										Integer receivedC = receivedCntr.get(recip);
										Integer deniedC = deniedCntr.get(recip);
										if (!receivedCntr.containsKey(recip)) {// all denied
											logger.info("in !receivedCntr.containsKey(recip) (requestor["
													+ recip.substring(0, recip.indexOf("_")) + "] DENIED access to ["
													+ builder.locationKey().intValue() + "]) recCntr["
													+ (receivedC == null ? "0" : receivedC) + "] denCntr["
													+ (deniedC == null ? "0" : deniedC) + "] for requestor/key: "
													+ recip);
											futureTask.failed("Too many workers on data item for key ["
													+ builder.locationKey().intValue() + "] already");
										} else if (!deniedCntr.containsKey(recip)) {// All received
											logger.info("in !deniedCntr.containsKey(recip) (requestor["
													+ recip.substring(0, recip.indexOf("_")) + "] GRANTED access to ["
													+ builder.locationKey().intValue() + "]) recCntr["
													+ (receivedC == null ? "0" : receivedC) + "] denCntr["
													+ (deniedC == null ? "0" : deniedC) + "] for requestor/key : "
													+ recip);
											futureTask.receivedData(rawData, futuresCompleted);
										} else if (receivedCntr.containsKey(recip) && deniedCntr.containsKey(recip)) {
											if (receivedC >= deniedC) { // received
												logger.info("in receivedC >= deniedC (" + receivedC + " >= " + deniedC
														+ ") (requestor[" + recip.substring(0, recip.indexOf("_"))
														+ "] GRANTED access to [" + builder.locationKey().intValue()
														+ "]) recCntr[" + (receivedC == null ? "0" : receivedC)
														+ "] denCntr[" + (deniedC == null ? "0" : deniedC)
														+ "] for requestor/key: " + recip);
												futureTask.receivedData(rawData, futuresCompleted);
											} else {// if(receivedC < deniedC){
												logger.info("in receivedC < deniedC (" + receivedC + " < " + deniedC
														+ ") (requestor[" + recip.substring(0, recip.indexOf("_"))
														+ "] DENIED access to [" + builder.locationKey().intValue()
														+ "]) recCntr[" + (receivedC == null ? "0" : receivedC)
														+ "] denCntr[" + (deniedC == null ? "0" : deniedC)
														+ "] for requestor/key: " + recip);
												futureTask.failed("Too many workers on data item for key ["
														+ builder.locationKey().intValue() + "] already");
											}
										}
									}

									@Override
									public void interMediateResponse(FutureResponse future) {
										if (future.isSuccess() && future.responseMessage().isOk()) {
											synchronized (receivedCntr) {
												String recip = asyncTask.peerMapReduce().peer().peerID().intValue()
														+ "_" + builder.locationKey() + "_" + builder.domainKey();
												Integer cntr = receivedCntr.get(recip);
												if (cntr == null) {
													cntr = 0;
												}
												receivedCntr.put(recip, ++cntr);
											}
											rawData.put(future.request().recipient(),
													future.responseMessage().dataMap(0).dataMap());
										} else if (future.isSuccess()
												&& future.responseMessage().type() == Type.DENIED) {
											synchronized (deniedCntr) {
												String recip = asyncTask.peerMapReduce().peer().peerID().intValue()
														+ "_" + builder.locationKey() + "_" + builder.domainKey();
												Integer cntr = deniedCntr.get(recip);
												if (cntr == null) {
													cntr = 0;
												}
												deniedCntr.put(recip, ++cntr);
											}
										}
									}
								});
							} else {
								futureTask.failed(futureRouting);
							}
						}
					});
					futureTask.addFutureDHTReleaseListener(future.channelCreator());
				} else {
					futureTask.failed(future);
				}
			}

		});
		return futureTask;
	}

	/**
	 * Creates RPCs and executes them parallel.
	 * 
	 * @param p2pConfiguration
	 *            The configuration that specifies e.g. how many parallel requests there are.
	 * @param queue
	 *            The sorted set that will be queries. The first RPC takes the first in the queue.
	 * @param futureDHT
	 *            The future object that tracks the progress
	 * @param cancleOnFinish
	 *            Set to true if the operation should be canceled (e.g. file transfer) if the future has finished.
	 * @param operation
	 *            The operation that creates the request
	 */
	public static FutureMapReduceData parallelRequests(final RequestP2PConfiguration p2pConfiguration,
			final NavigableSet<PeerAddress> directHit, final NavigableSet<PeerAddress> potentialHit,
			final boolean cancleOnFinish, final FutureChannelCreator futureChannelCreator,
			final MapReduceOperationMapper operation, final FutureMapReduceData futureTask) {

		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					parallelRequests(p2pConfiguration, directHit, potentialHit, futureTask, cancleOnFinish,
							future.channelCreator(), operation);
					addReleaseListener(future.channelCreator(), futureTask);
				} else {
					futureTask.failed(future);
				}
			}
		});
		return futureTask;
	}

	/**
	 * Adds a listener to the response future and releases all acquired channels in channel creator.
	 * 
	 * @param channelCreator
	 *            The channel creator that will be shutdown and all connections will be closed
	 * @param baseFutures
	 *            The futures to listen to. If all the futures finished, then the channel creator is shutdown. If null
	 *            provided, the channel creator is shutdown immediately.
	 */
	public static void addReleaseListener(final ChannelCreator channelCreator, final FutureMapReduceData futureTask) {
		if (futureTask == null) {
			channelCreator.shutdown();
			return;
		}

		futureTask.addListener(new BaseFutureAdapter<FutureMapReduceData>() {
			@Override
			public void operationComplete(final FutureMapReduceData future) throws Exception {
				FutureDone<Void> futuresCompleted = futureTask.futuresCompleted();
				if (futuresCompleted != null) {
					futureTask.futuresCompleted().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
						@Override
						public void operationComplete(final FutureDone<Void> future) throws Exception {
							channelCreator.shutdown();
						}
					});
				} else {
					channelCreator.shutdown();
				}
			}
		});
	}

	// TODO: have two queues, direct queue + potential queue.
	private static <K extends BaseFuture> void parallelRequests(RequestP2PConfiguration p2pConfiguration,
			NavigableSet<PeerAddress> directHit, NavigableSet<PeerAddress> potentialHit, FutureMapReduceData future,
			boolean cancleOnFinish, ChannelCreator channelCreator, MapReduceOperationMapper operation) {
		// the potential hits may contain same values as in directHit, so remove it from potentialHit
		for (PeerAddress peerAddress : directHit) {
			potentialHit.remove(peerAddress);
		}

		if (p2pConfiguration.minimumResults() == 0) {
			operation.response(future, null);
			return;
		}
		FutureResponse[] futures = new FutureResponse[p2pConfiguration.parallel()];
		// here we split min and pardiff, par=min+pardiff
		loopRec(directHit, potentialHit, p2pConfiguration.minimumResults(), new AtomicInteger(0),
				p2pConfiguration.maxFailure(), p2pConfiguration.parallelDiff(),
				new AtomicReferenceArray<FutureResponse>(futures), future, cancleOnFinish, channelCreator, operation);
	}

	private static void loopRec(final NavigableSet<PeerAddress> directHit, final NavigableSet<PeerAddress> potentialHit,
			final int min, final AtomicInteger nrFailure, final int maxFailure, final int parallelDiff,
			final AtomicReferenceArray<FutureResponse> futures, final FutureMapReduceData futureDHT,
			final boolean cancelOnFinish, final ChannelCreator channelCreator,
			final MapReduceOperationMapper operation) {
		// final int parallel=min+parallelDiff;
		int active = 0;
		for (int i = 0; i < min + parallelDiff; i++) {
			// System.err.println("res " + (min + parallelDiff));
			if (futures.get(i) == null) {
				PeerAddress next = directHit.pollFirst();
				if (next == null) {
					next = potentialHit.pollFirst();
				}
				if (next != null) {
					active++;
					FutureResponse futureResponse = operation.create(channelCreator, next);
					futures.set(i, futureResponse);
					futureDHT.addRequests(futureResponse);
				}
			} else {
				active++;
			}
		}
		if (active == 0) {
			operation.response(futureDHT, null);
			if (cancelOnFinish) {
				cancel(futures);
			}
			return;
		}
		logger.debug("fork/join status: {}/{} ({})", min, active, parallelDiff);

		FutureForkJoin<FutureResponse> fp = new FutureForkJoin<FutureResponse>(Math.min(min, active), false, futures);
		fp.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
			@Override
			public void operationComplete(final FutureForkJoin<FutureResponse> future) throws Exception {
				for (FutureResponse futureResponse : future.completed()) {
					operation.interMediateResponse(futureResponse);
					// if (futureDHT.isCompleted()) {
					// cancel(futures);
					// return;
					// }
				}

				// we are finished if forkjoin says so or we got too many
				// failures
				if (future.isSuccess() || nrFailure.incrementAndGet() > maxFailure) {
					if (cancelOnFinish) {
						cancel(futures);
					}
					operation.response(futureDHT, future.futuresCompleted());
				} else {
					loopRec(directHit, potentialHit, min - future.successCounter(), nrFailure, maxFailure, parallelDiff,
							futures, futureDHT, cancelOnFinish, channelCreator, operation);
				}
			}
		});
	}

	/**
	 * Cancel the future that causes the underlying futures to cancel as well.
	 */
	private static void cancel(final AtomicReferenceArray<FutureResponse> futures) {
		int len = futures.length();
		for (int i = 0; i < len; i++) {
			BaseFuture baseFuture = futures.get(i);
			if (baseFuture != null) {
				baseFuture.cancel();
			}
		}
	}
}
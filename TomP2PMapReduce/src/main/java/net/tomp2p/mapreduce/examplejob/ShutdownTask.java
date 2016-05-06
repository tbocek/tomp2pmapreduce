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
package net.tomp2p.mapreduce.examplejob;

import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * ShutdownTask demonstrates how a user needs to take care of a graceful disconnection of the peer once a job finished.
 * In the current implementation, a user specifies how many times a shutdown task needs to be called. Once this number
 * is reached, the shutdown is initiated and the peer disconnected from the DHT after 15 seconds of waiting time. The
 * waiting time is provided such that all connected peers can receive the required number of messages and needs to be
 * tested beforehand by the user.
 * 
 * @author Oliver Zihler
 *
 */
public class ShutdownTask extends Task {
	private static Logger logger = LoggerFactory.getLogger(ShutdownTask.class);
	private static final long serialVersionUID = -5543401293112052880L;
	public static int DEFAULT_SLEEPING_TIME_REPS = 15;
	public static long DEFAULT_SLEEPING_TIME = 1000;

	private long sleepingTime = DEFAULT_SLEEPING_TIME;
	private int sleepingTimeReps = DEFAULT_SLEEPING_TIME_REPS;
	private int retrievalCounter = 0;
	private int nrOfShutdownMessagesToAwait;
	public AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

	/**
	 * 
	 * @param previousId
	 *            ID of the {@link PrintTask}
	 * 
	 * @param currentId
	 *            ID of this ShutdownTask
	 * @param nrOfShutdownMessagesToAwait
	 *            how many times this ShutdownTask needs to be invoked before the peer is actually disconnected from the
	 *            overlay.
	 * @param sleepingTimeReps
	 *            number of times sleepingTime should be repeated
	 * @param sleepingTime
	 *            Sleeping tim ein milliseconds
	 */
	public ShutdownTask(Number640 previousId, Number640 currentId, int nrOfShutdownMessagesToAwait) {
		super(previousId, currentId);
		this.nrOfShutdownMessagesToAwait = nrOfShutdownMessagesToAwait;
	}

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {

		if (!input.containsKey(NumberUtils.OUTPUT_STORAGE_KEY)) {
			logger.info("Received shutdown but not for the printing task. Ignored");
			return;
		}
		logger.info("Received REAL shutdown from ACTUAL PRINTING TASK. shutdown initiated.");

		if (shutdownInitiated.get()) {
			logger.info("Shutdown already initiated. ignored");
			return;
		}
		++retrievalCounter;
		logger.info("Retrieval counter: " + retrievalCounter + ", (" + retrievalCounter + " >= "
				+ nrOfShutdownMessagesToAwait + ")? " + (retrievalCounter >= nrOfShutdownMessagesToAwait));
		if (retrievalCounter >= nrOfShutdownMessagesToAwait) {
			shutdownInitiated.set(true);
			logger.info("Received shutdown message. Counter is: " + retrievalCounter + ": SHUTDOWN IN 5 SECONDS");
			new Thread(new Runnable() {

				@Override
				public void run() {

					int cnt = 0;
					while (cnt < sleepingTimeReps) {
						logger.info("[" + (cnt++) + "/" + sleepingTimeReps + "] times slept for "
								+ (sleepingTime / 1000) + "s");
						try {
							Thread.sleep(sleepingTime);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

					try {
						pmr.peer().shutdown().await().addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								if (future.isSuccess()) {
									logger.info("Success on shutdown peer [" + pmr.peer().peerID().shortValue() + "]");
								} else {
									logger.info("NO SUCCEESSS on shutdown peer [" + pmr.peer().peerID().shortValue()
											+ "], reason: " + future.failedReason());
								}
								System.exit(0);
							}
						});
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					logger.info("Shutdown peer.");

				}
			}).start();
		} else {
			logger.info("RetrievalCounter is only: " + retrievalCounter + ", no shutdown.");
		}

	}

}
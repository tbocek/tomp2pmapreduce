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
package net.tomp2p.mapreduce.evaljob;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureMapReduceData;
import net.tomp2p.mapreduce.MapReducePutBuilder;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.InputUtils;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class MapTaskEval extends Task {

	private static AtomicInteger counter = new AtomicInteger(0);

	private static Logger logger = LoggerFactory.getLogger(MapTaskEval.class);
	// public static long cntr = 0;
	int nrOfExecutions;

	public MapTaskEval(Number640 previousId, Number640 currentId, int nrOfExecutions) {
		super(previousId, currentId);
		this.nrOfExecutions = nrOfExecutions;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7150229043957182808L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
 
//		startTaskCounter.incrementAndGet();

		// logger.info();

		int execID = counter.getAndIncrement();

		logger.info(">>>>>>>>>>>>>>>>>>>> START EXECUTING MAPTASK [" + execID + "],[" + ((Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object()).locationKey().intValue() + "]");
//		TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> START EXECUTING MAPTASK [" + execID + "],[" + ((Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object()).locationKey().intValue() + "]");

		Number640 inputStorageKey = (Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object();
		Number160 outputLocationKey = inputStorageKey.locationKey();
		Number160 outputDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));
		// try {
		// Thread.sleep(new Random().nextInt(1000));
		// } catch (InterruptedException e1) {
		// e1.printStackTrace();
		// }
//		Thread.sleep(new Random().nextInt(3000));

		pmr.get(inputStorageKey.locationKey(), inputStorageKey.domainKey(), new TreeMap<>()/*input*/).start().addListener(new BaseFutureAdapter<FutureMapReduceData>() {

			@Override
			public void operationComplete(FutureMapReduceData future) throws Exception {
				try {
					logger.info("MAP TASK [" + execID + "] future.isSuccess()?:" + future.isSuccess());
					if (future.isSuccess()) {
						String text = ((String) future.data().object()).replaceAll("[\t\n\r]", " ");
						String[] ws = text.split(" ");

						Map<String, Integer> fileResults = new HashMap<String, Integer>();
						for (String word : ws) {
							if (word.trim().length() == 0) {
								continue;
							}
							synchronized (fileResults) {
								Integer ones = fileResults.get(word);
								if (ones == null) {
									ones = 0;
								}
								++ones;
								fileResults.put(word, ones);
							}
						}
						logger.info(this + " [" + execID + "]: input produced output[" + fileResults.keySet().size() + "] words");
						MapReducePutBuilder put = pmr.put(outputLocationKey, outputDomainKey, fileResults, nrOfExecutions);
//						put.execId = "MapTASK [" + execID + "]_Peer[" + pmr.peer().peerID().shortValue() + "]";
						put.start().addListener(new BaseFutureAdapter<BaseFuture>() {

							@Override
							public void operationComplete(BaseFuture future) throws Exception {
								try {
									logger.info("MAPTASK[" + execID + "] put future.isSuccess()?" + future.isSuccess());
									if (future.isSuccess()) {
										NavigableMap<Number640, Data> newInput = new TreeMap<>();
										InputUtils.keepInputKeyValuePairs(input, newInput, new String[] { "JOB_KEY", "NUMBEROFFILES", "INPUTTASKID", "MAPTASKID", "REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID", "RECEIVERS" });
										newInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
										newInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("MAPTASKID")));
										newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("REDUCETASKID")));
										// newInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKey("SHUTDOWNTASKID")));
										newInput.put(NumberUtils.INPUT_STORAGE_KEY, input.get(NumberUtils.OUTPUT_STORAGE_KEY));
										newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(new Number640(outputLocationKey, outputDomainKey, Number160.ZERO, Number160.ZERO)));
										logger.info(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING MAPTASK [" + execID + "],[" + ((Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object()).locationKey().intValue() + "]");
//										TestInformationGatherUtils.addLogEntry(">>>>>>>>>>>>>>>>>>>> FINISHED EXECUTING MAPTASK [" + execID + "],[" + ((Number640) input.get(NumberUtils.OUTPUT_STORAGE_KEY).object()).locationKey().intValue() + "]");
										pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();
//										finishedTaskCounter.incrementAndGet();

									} else {
										logger.info("!future.isSuccess(), failed reason: " + future.failedReason());
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						});
					} else {// Do nothing
						logger.info("!future.isSuccess(), failed reason: " + future.failedReason());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		});

	}

}
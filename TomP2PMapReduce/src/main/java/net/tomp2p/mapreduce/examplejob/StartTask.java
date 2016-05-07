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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureMapReduceData;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.FileSize;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.FileUtils;
import net.tomp2p.mapreduce.utils.InputUtils;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class StartTask extends Task {

	private static Logger logger = LoggerFactory.getLogger(StartTask.class);
	private int nrOfExecutions = 2;

	public StartTask(Number640 previousId, Number640 currentId, int nrOfExecutions) {
		super(previousId, currentId);
		this.nrOfExecutions = nrOfExecutions;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5879889214195971852L;

	@Override
	public void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		Number160 filesDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));

		// =====END NEW BC DATA===========================================================
		// Only used to avoid adding it in each future listener...
		Map<Number640, Data> tmpNewInput = Collections.synchronizedMap(new TreeMap<>());
		// These keys and values are from the old input and should be reused in the next task. That's why they need to
		// be copied to the new broadcast input
		InputUtils.keepInputKeyValuePairs(input, tmpNewInput, new String[] { "JOB_KEY", "INPUTTASKID", "MAPTASKID",
				"REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID", "NUMBEROFFILES" });
		// Sender is currently needed for the MapReduceBroadcastHandler to determine from where the message was sent
		tmpNewInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
		// Such that all receiving nodes can deserialise them if they just connected to the overlay and can start
		// execution
		tmpNewInput.put(NumberUtils.RECEIVERS, input.get(NumberUtils.RECEIVERS));
		// To determine which task to execute next.
//		tmpNewInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKey("INPUTTASKID")));
		tmpNewInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKeys("MAPTASKID")));
		// =====END NEW BC DATA===========================================================

		// ============GET ALL THE FILES ==========
		String filesPath = (String) input.get(NumberUtils.allSameKeys("DATAFILEPATH")).object();
		List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
		FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
		// ===== FINISHED GET ALL THE FILES =================

		// ===== SPLIT AND DISTRIBUTE ALL THE DATA TO THE DHT AND SEND BROADCASTS==========
		final List<FutureMapReduceData> futurePuts = Collections.synchronizedList(new ArrayList<>());

		// To distribute all the files in parallel like the MapReduceBroadcastReceiver does
		int nrOfFiles = (int) input.get(NumberUtils.allSameKeys("NUMBEROFFILES")).object();
		ThreadPoolExecutor e = new ThreadPoolExecutor(nrOfFiles, nrOfFiles, Long.MAX_VALUE, TimeUnit.DAYS,
				new LinkedBlockingQueue<>());

		// Read all the files and distribute them to the DHT in parallel. Sends a broadcast on every successful put into
		// the DHT
		for (String filePath : pathVisitor) {
			e.submit(new Runnable() {

				@Override
				public void run() {
					try {
						// Currently, the file sizes need to be smaller then the specified FileSize here, as the user
						// needs to know before how many files are generated. Else, ReduceTask does not know for how
						// many tasks it has to wait...
						Map<Number160, FutureMapReduceData> tmp = FileSplitter.splitWithWordsAndWrite(filePath, pmr,
								nrOfExecutions, filesDomainKey, FileSize.THIRTY_TWO_MEGA_BYTES.value(), "UTF-8");

						futurePuts.addAll(tmp.values());
						int cnt = 0;
						for (Number160 fileKey : tmp.keySet()) {
							System.err.println(cnt++);
							tmp.get(fileKey).addListener(new BaseFutureAdapter<BaseFuture>() {

								@Override
								public void operationComplete(BaseFuture future) throws Exception {
									if (future.isSuccess()) {
										Number640 storageKey = new Number640(fileKey, filesDomainKey, Number160.ZERO,
												Number160.ZERO);

										if (future.isSuccess()) {
											NavigableMap<Number640, Data> newInput = new TreeMap<>();
											synchronized (tmpNewInput) {
												newInput.putAll(tmpNewInput);
											}
											newInput.put(NumberUtils.OUTPUT_STORAGE_KEY, new Data(storageKey));
											logger.info("success on put(k[" + storageKey.locationKey().intValue()
													+ "], v[content of ()])");

											pmr.peer().broadcast(new Number160(new Random())).dataMap(newInput).start();

										} else {
											logger.info("No success on put(fileKey, actualValues) for key "
													+ storageKey.locationAndDomainKey().intValue());
										}
									}
								}
							});
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});

		}

	}

}
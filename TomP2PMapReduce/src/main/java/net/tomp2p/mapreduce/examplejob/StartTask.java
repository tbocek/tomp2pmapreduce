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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.mapreduce.FutureMapReduceData;
import net.tomp2p.mapreduce.Job;
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

/**
 * This is the first task to execute. It will be determined by the job instance on {@link Job#start()} as it's
 * previousId is null, meaning there is no task before this one in the chain. It retrieves all files specified in the
 * input {@link NavigableMap}, read the from the local file system, and store them in the DHT. On every put of a file, a
 * broadcast is submitted for all connected nodes to start execution, too.
 * 
 * @author Oliver Zihler
 *
 */
public class StartTask extends Task {

	private static Logger logger = LoggerFactory.getLogger(StartTask.class);
	/** How many times the stored input should be accessible by other nodes. Default is twice */
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
		logger.info(">>>>>>>>>>>>>>>>>>>> EXECUTING START TASK ");
		Number160 filesDomainKey = Number160.createHash(pmr.peer().peerID() + "_" + (new Random().nextLong()));

		// =====NEW BC DATA===========================================================
		/*
		 * Here, all broadcast input needed from the old input to execute one of the next tasks in the chain is
		 * specified.
		 */
		Map<Number640, Data> tmpNewInput = Collections.synchronizedMap(new TreeMap<>());
		InputUtils.keepInputKeyValuePairs(input, tmpNewInput, new String[] { "JOB_KEY", "INPUTTASKID", "MAPTASKID",
				"REDUCETASKID", "WRITETASKID", "SHUTDOWNTASKID", "NUMBEROFFILES" });
		tmpNewInput.put(NumberUtils.SENDER, new Data(pmr.peer().peerAddress()));
		tmpNewInput.put(NumberUtils.RECEIVERS, input.get(NumberUtils.RECEIVERS));
		tmpNewInput.put(NumberUtils.CURRENT_TASK, input.get(NumberUtils.allSameKeys("INPUTTASKID")));
		tmpNewInput.put(NumberUtils.NEXT_TASK, input.get(NumberUtils.allSameKeys("MAPTASKID")));
		// =====END NEW BC DATA===========================================================

		// ============GET ALL THE FILES ==========
		String filesPath = (String) input.get(NumberUtils.allSameKeys("DATAFILEPATH")).object();
		List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
		FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
		// ===== FINISHED GET ALL THE FILES =================

		// ===== SPLIT AND DISTRIBUTE ALL THE DATA ==========
		final List<FutureMapReduceData> futurePuts = Collections.synchronizedList(new ArrayList<>());
		// Map<Number160, FutureTask> all = Collections.synchronizedMap(new HashMap<>());
		int nrOfFiles = (int) input.get(NumberUtils.allSameKeys("NUMBEROFFILES")).object();
		ThreadPoolExecutor e = new ThreadPoolExecutor(nrOfFiles, nrOfFiles, Long.MAX_VALUE, TimeUnit.DAYS,
				new LinkedBlockingQueue<>());
		for (String filePath : pathVisitor) {
			e.submit(new Runnable() {

				@Override
				public void run() {
					try {
						// Get the all the files' content from the local disk and put them into the DHT
						Map<Number160, FutureMapReduceData> tmp = FileSplitter.splitWithWordsAndWrite(filePath, pmr,
								nrOfExecutions, filesDomainKey, FileSize.THIRTY_TWO_MEGA_BYTES.value(), "UTF-8");
						futurePuts.addAll(tmp.values());

						// For each file, emit a broadcast containing the storage key to retrieve the file's content
						// from
						for (Number160 fileKey : tmp.keySet()) {
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

											// Emit the broadcast
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
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			});

		} 
	}

}
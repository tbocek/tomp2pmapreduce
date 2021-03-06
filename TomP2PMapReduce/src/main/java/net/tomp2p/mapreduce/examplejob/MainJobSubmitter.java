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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;
import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.PeerConnectionCloseListener;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.FileSize;
import net.tomp2p.mapreduce.utils.FileUtils;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;

/**
 * This class is the main entry point to submit a job. First of all, a {@link Peer} instance is configured to connect to a well known node to establish an overlay network. This peer is then added to
 * {@link PeerMapReduce} that abstracts all DHT calls. Currently, all connection related timeouts are set to Integer.MAX_VALUE to avoid problems (needs resolution). After establishing the connection,
 * the {@link Job} to submit is composed. In this example, the complete job is sent with every broadcast. Instead, it could be stored in the DHT and only the id sent around. The first is in order if
 * the job is small. The second is better for larger jobs as many broadcasts can add up to several megabytes if the complete job is sent over the network. The user is responsible to allow the
 * {@link IMapReduceBroadcastReceiver} to start execution of the next task int the chain. See also the documentation <a href="http://tinyurl.com/csgmtmapred">here</a>, chapter 4 (conceptual workflow)
 * and 5 (more implementation details and usage description) for more detailed explanations.
 * 
 * @author Oliver Zihler
 * 
 * @see StartTask
 * @see MapTask
 * @see ReduceTask
 * @see PrintTask
 * @see ShutdownTask
 * @see ExampleJobBroadcastReceiver
 * @see <a href="http://tinyurl.com/csgmtmapred">Documentation</a>
 * 
 *
 */
public class MainJobSubmitter {
	private static final int waitingTime = 5000; // Time to wait before get is invoked

	private static NavigableMap<Number640, Data> getJob(int nrOfShutdownMessagesToAwait, int nrOfExecutions, String filesPath, int nrOfFiles, Job job) throws IOException {
		// Initialise all user-defined Tasks and chain them together. The previousId of the succeeding Task becomes the currentId of the previous Task.
		Task startTask = new StartTask(null, NumberUtils.next(), nrOfExecutions);// previousId is null for the first task!!!
		Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next(), nrOfExecutions);
		Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next(), nrOfExecutions);
		Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
		Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(), nrOfShutdownMessagesToAwait, 10, 1000);

		// Add all tasks to a job
		job.addTask(startTask);
		job.addTask(mapTask);
		job.addTask(reduceTask);
		job.addTask(writeTask);
		job.addTask(initShutdown);

		// Add receiver to handle BC messages (job specific handler, defined by user)
		IMapReduceBroadcastReceiver receiver = new ExampleJobBroadcastReceiver(job.id());
		job.addBroadcastReceiver(receiver);

		// Compose the input data needed for executing StartTask
		NavigableMap<Number640, Data> input = new TreeMap<>();

		// The broadcast receiver needs to be available on every node when they receive a broadcast to find and execute the next task. Thus, it is sent as broadcast, too.
		List<TransferObject> broadcastReceiversTransferObjects = job.serializeBroadcastReceivers();
		input.put(NumberUtils.RECEIVERS, new Data(broadcastReceiversTransferObjects));// Required by MapReduceBroadcastReceiver!
		input.put(NumberUtils.JOB_ID, new Data(job.id())); // Not necessarily required --> e.g. if the complete Job was put into the DHT and not send via broadcast.
		input.put(NumberUtils.JOB_DATA, new Data(job.serialize()));// Send the complete job

		// The following inputs are needed by StartTask as defined by the user
		// All keys of every task are added. If the user knows them in the tasks, it could be omitted.
		input.put(NumberUtils.allSameKeys("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKeys("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKeys("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKeys("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKeys("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));

		input.put(NumberUtils.allSameKeys("DATAFILEPATH"), new Data(filesPath));// Location of the files to process
		input.put(NumberUtils.allSameKeys("NUMBEROFFILES"), new Data(nrOfFiles));// How many files. This is needed beforehand for the ReduceTask to know how many different keys to await

		return input;
	}

	public static void main(String[] args) throws Exception {

		// DISTRIBUTED EXECUTION
		boolean tmpShouldBootstrap = true;
		int tmpNrOfShutdownMessagesToAwait = 2;
		int tmpNrOfExecutions = 2;

		// if (args[0].equalsIgnoreCase("local")) { // Local execution
		// tmpShouldBootstrap = false;
		// tmpNrOfShutdownMessagesToAwait = 1;
		// tmpNrOfExecutions = 1;
		// }
		final boolean shouldBootstrap = tmpShouldBootstrap;
		final int nrOfShutdownMessagesToAwait = tmpNrOfShutdownMessagesToAwait;
		final int nrOfExecutions = tmpNrOfExecutions;

		String filesPath = new File("").getAbsolutePath().replace("\\", "/") + "/src/main/java/net/tomp2p/mapreduce/examplejob/inputfiles";
		int nrOfFiles = localCalculation(filesPath);
		ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_TCP_IDLE_MILLIS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP = Integer.MAX_VALUE;
		// Should be less than shutdown time (reps*sleepingTime)
		PeerConnectionCloseListener.WAITING_TIME = Integer.MAX_VALUE;

		int bootstrapperPortToConnectTo = 4004;
		String bootstrapperToConnectTo = "192.168.0.19";
		Number160 id = new Number160(1);

		PeerMapConfiguration pmc = new PeerMapConfiguration(id);
		pmc.peerNoVerification();
		PeerMap pm = new PeerMap(pmc);
		PeerBuilder peerBuilder = new PeerBuilder(id).peerMap(pm).ports(bootstrapperPortToConnectTo);

		PeerMapReduce peerMapReduce = new PeerMapReduce(peerBuilder, waitingTime);
		if (shouldBootstrap) {
			// int bootstrapperPortToConnectTo = 4004;
			peerMapReduce.peer().bootstrap().ports(bootstrapperPortToConnectTo).inetAddress(InetAddress.getByName(bootstrapperToConnectTo))
					// .ports(bootstrapperPortToConnectTo)
					.start().awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {

						@Override
						public void operationComplete(FutureBootstrap future) throws Exception {
							if (future.isSuccess()) {
								System.err.println("successfully bootstrapped to " + bootstrapperToConnectTo + "/" + bootstrapperPortToConnectTo);
							} else {
								System.err.println("No success on bootstrapping: fail reason: " + future.failedReason());
							}
						}

					});
		}
		final PeerMapReduce pmr = peerMapReduce;
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					// Create a new job instance
					Job job = new Job(new Number640(new Random()));
					NavigableMap<Number640, Data> input = getJob(nrOfShutdownMessagesToAwait, nrOfExecutions, filesPath, nrOfFiles, job);
					// Locally start the first task StartTask
					job.start(input, pmr);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}).start();

	}

	private static int localCalculation(String filesPath) {
		try {
			List<String> pathVisitor = new ArrayList<>();
			FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
			Map<String, Integer> fileResults = new TreeMap<String, Integer>();

			for (String filePath : pathVisitor) {

				try {
					RandomAccessFile aFile = new RandomAccessFile(filePath, "r");
					FileChannel inChannel = aFile.getChannel();
					ByteBuffer buffer = ByteBuffer.allocate(FileSize.SIXTY_FOUR_MEGA_BYTES.value());
					// int filePartCounter = 0;
					String split = "";
					String actualData = "";
					String remaining = "";
					while (inChannel.read(buffer) > 0) {
						buffer.flip();
						// String all = "";
						// for (int i = 0; i < buffer.limit(); i++) {
						byte[] data = new byte[buffer.limit()];
						buffer.get(data);
						// }
						// System.out.println(all);
						split = new String(data);
						split = remaining += split;

						remaining = "";
						// System.out.println(all);d
						// Assure that words are not split in parts by the buffer: only
						// take the split until the last occurrance of " " and then
						// append that to the first again

						if (split.getBytes(Charset.forName("UTF-8")).length >= FileSize.SIXTY_FOUR_MEGA_BYTES.value()) {
							actualData = split.substring(0, split.lastIndexOf(" ")).trim();
							remaining = split.substring(split.lastIndexOf(" ") + 1, split.length()).trim();
						} else {
							actualData = split.trim();
							remaining = "";
						}
						// System.err.println("Put data: " + actualData + ", remaining data: " + remaining);
						String[] ws = actualData.replaceAll("[\t\n\r]", " ").split(" ");

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
						buffer.clear();
						split = "";
						actualData = "";
					}
					inChannel.close();
					aFile.close();
				} catch (Exception e) {
					System.err.println("Exception on reading file at location: " + filePath);
					e.printStackTrace();
				}

			}
			System.err.println("Nr of words to expect: " + fileResults.keySet().size());
			PrintTask.printResults("localOutput", fileResults, -10);
			return pathVisitor.size();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}
}

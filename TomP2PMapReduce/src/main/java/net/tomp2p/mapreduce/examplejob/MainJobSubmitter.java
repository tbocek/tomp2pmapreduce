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
import java.net.InetAddress;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;
import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.PeerConnectionCloseListener;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.p2p.Peer;
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

	public static void main(String[] args) throws Exception {

		// DISTRIBUTED EXECUTION
		boolean tmpShouldBootstrap = true;
		int tmpNrOfShutdownMessagesToAwait = 2;
		int tmpNrOfExecutions = 2;

		if (args.length > 0 && args[0].equalsIgnoreCase("local")) { // Local execution
			tmpShouldBootstrap = false;
			tmpNrOfShutdownMessagesToAwait = 1;
			tmpNrOfExecutions = 1;
		}
		final boolean shouldBootstrap = tmpShouldBootstrap;
		final int nrOfShutdownMessagesToAwait = tmpNrOfShutdownMessagesToAwait;
		final int nrOfExecutions = tmpNrOfExecutions;

		String filesPath = new File("").getAbsolutePath().replace("\\", "/") + "/src/main/java/net/tomp2p/mapreduce/evaljob/inputfiles";
		int nrOfFiles = 24;
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
		int getWaitingTime = 5000;
		PeerMapReduce peerMapReduce = new PeerMapReduce(peerBuilder, getWaitingTime);
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

		// Create a new job instance
		Job job = new Job();

		// Initialise all user-defined Tasks and chain them together. The previousId of the succeeding Task becomes the currentId of the previous Task.
		Task startTask = new StartTask(null, NumberUtils.next(), nrOfExecutions); // previousId is null for the first task!!!
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
		input.put(NumberUtils.RECEIVERS, new Data(broadcastReceiversTransferObjects)); // Required by MapReduceBroadcastReceiver!
		// input.put(NumberUtils.JOB_ID, new Data(job.id())); // Not necessarily required --> e.g. if the complete Job was put into the DHT and not send via broadcast.
		input.put(NumberUtils.JOB_DATA, new Data(job.serialize())); // Send the complete job

		// The following inputs are needed by StartTask as defined by the user
		// All keys of every task are added. If the user knows them in the tasks, it could be omitted.
		input.put(NumberUtils.allSameKeys("INPUTTASKID"), new Data(startTask.currentId()));
		input.put(NumberUtils.allSameKeys("MAPTASKID"), new Data(mapTask.currentId()));
		input.put(NumberUtils.allSameKeys("REDUCETASKID"), new Data(reduceTask.currentId()));
		input.put(NumberUtils.allSameKeys("WRITETASKID"), new Data(writeTask.currentId()));
		input.put(NumberUtils.allSameKeys("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));

		input.put(NumberUtils.allSameKeys("DATAFILEPATH"), new Data(filesPath)); // Location of the files to process
		input.put(NumberUtils.allSameKeys("NUMBEROFFILES"), new Data(nrOfFiles)); // How many files. This is needed beforehand for the ReduceTask to know how many different keys to await
																					// before executing

		// Locally start the first task StartTask
		job.start(input, pmr);

	}

}

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
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;
import net.tomp2p.storage.Data;

public class MainJobSubmitter {

	public static void main(String[] args) throws Exception {

		// false -- will only execute locally, true --- will connect to bootstrapper specifeid by bootstrapperIP and
		// bootstrapperPort
		boolean shouldBootstrap = true;
		int nrOfShutdownMessagesToAwait = 2;
		int nrOfExecutions = 2;

		String filesPath = "C:/Users/Oliver/Desktop/evaluation/512kb/12MB";
		int nrOfFiles = 2;
		ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_TCP_IDLE_MILLIS = Integer.MAX_VALUE;
		ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP = Integer.MAX_VALUE;

		PeerConnectionCloseListener.WAITING_TIME = Integer.MAX_VALUE;

		// how long the get method should wait until it actually starts retrieving the data from the dht
		PeerMapReduce.DEFAULT_WAITING_TIME = 75000;
		ShutdownTask.DEFAULT_SLEEPING_TIME = 1000;
		ShutdownTask.DEFAULT_SLEEPING_TIME_REPS = 10;
		// Bootstrapping node IP
		String bootstrapperIP = "192.168.0.19";
		// Bootstrapping node port
		int bootstrapperPort = 4004;

		// Peer configuration
		Number160 id = new Number160(1);

		PeerMapConfiguration pmc = new PeerMapConfiguration(id);
		pmc.peerNoVerification();
		PeerMap pm = new PeerMap(pmc);
		PeerBuilder peerBuilder = new PeerBuilder(id).peerMap(pm).ports(bootstrapperPort);

		PeerMapReduce peerMapReduce = new PeerMapReduce(peerBuilder);
		if (shouldBootstrap) {
			// int bootstrapperPortToConnectTo = 4004;
			peerMapReduce.peer().bootstrap().ports(bootstrapperPort).inetAddress(InetAddress.getByName(bootstrapperIP))
					// .ports(bootstrapperPortToConnectTo)
					.start().awaitUninterruptibly().addListener(new BaseFutureAdapter<FutureBootstrap>() {

						@Override
						public void operationComplete(FutureBootstrap future) throws Exception {
							if (future.isSuccess()) {
								System.err.println(
										"successfully bootstrapped to " + bootstrapperIP + "/" + bootstrapperPort);
							} else {
								System.err
										.println("No success on bootstrapping: fail reason: " + future.failedReason());
							}
						}

					});
		}
		final PeerMapReduce pmr = peerMapReduce;
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					System.err.println("MainJobSubmitter: nrOfShutdownMessagesToAwait[" + nrOfShutdownMessagesToAwait
							+ "], nrOfExecutions[" + nrOfExecutions + "], ConnectionBean.DEFAULT_TCP_IDLE_MILLIS["
							+ ConnectionBean.DEFAULT_TCP_IDLE_MILLIS + "], PeerConnectionCloseListener.WAITING_TIME ["
							+ PeerConnectionCloseListener.WAITING_TIME + "], filesPath[" + filesPath + "], nrOfFiles ["
							+ nrOfFiles + "]");
					System.err.println("MainJobSubmitter: START JOB");

					Job job = new Job(new Number640(new Random()));
					Task startTask = new StartTask(null, NumberUtils.next(), nrOfExecutions);
					Task mapTask = new MapTask(startTask.currentId(), NumberUtils.next(), nrOfExecutions);
					Task reduceTask = new ReduceTask(mapTask.currentId(), NumberUtils.next(), nrOfExecutions);
					Task writeTask = new PrintTask(reduceTask.currentId(), NumberUtils.next());
					Task initShutdown = new ShutdownTask(writeTask.currentId(), NumberUtils.next(),
							nrOfShutdownMessagesToAwait);

					job.addTask(startTask);
					job.addTask(mapTask);
					job.addTask(reduceTask);
					job.addTask(writeTask);
					job.addTask(initShutdown);

					// Add receiver to handle BC messages (job specific handler, defined by user)
					IMapReduceBroadcastReceiver receiver = new ExampleJobBroadcastReceiver(job.id());
					job.addBroadcastReceiver(receiver);

					NavigableMap<Number640, Data> input = new TreeMap<>();
					input.put(NumberUtils.allSameKey("INPUTTASKID"), new Data(startTask.currentId()));
					input.put(NumberUtils.allSameKey("MAPTASKID"), new Data(mapTask.currentId()));
					input.put(NumberUtils.allSameKey("REDUCETASKID"), new Data(reduceTask.currentId()));
					input.put(NumberUtils.allSameKey("WRITETASKID"), new Data(writeTask.currentId()));
					input.put(NumberUtils.allSameKey("SHUTDOWNTASKID"), new Data(initShutdown.currentId()));
					input.put(NumberUtils.allSameKey("DATAFILEPATH"), new Data(filesPath));
					input.put(NumberUtils.allSameKey("NUMBEROFFILES"), new Data(nrOfFiles));

					job.start(input, pmr);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}).start();

	}

}

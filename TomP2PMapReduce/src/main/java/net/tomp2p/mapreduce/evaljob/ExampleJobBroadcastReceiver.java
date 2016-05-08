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

import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;
import net.tomp2p.mapreduce.Job;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.message.Message;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * Exemplary implementation of {@link IMapReduceBroadcastReceiver} that is invoked by {@link MapReduceBroadcastHandler}
 * after receiving a broadcast {@link Message}. This implementation receives the job on every broadcast, deserialises
 * it, and adds it to a set of jobs to assure the same job is only added once. This is required as the
 * {@link ReduceTask} depends on the job as it stores certain result temporarily. If no dependencies are needed,
 * instead, the job would not have to be stored. Once the job is added, the next task to execute is distinguished and
 * invoked using the received broadcast input. Every instance of {@link ExampleJobBroadcastReceiver} is responsible
 * for exactly one job.
 * 
 * @author Oliver Zihler
 *
 */
public class ExampleJobBroadcastReceiver implements IMapReduceBroadcastReceiver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6201919213334638897L;
	private static Logger logger = LoggerFactory.getLogger(ExampleJobBroadcastReceiver.class);

	/**
	 * identifier to be declared such that the same IMapReduceBroadcastReceiver is not instantiated multiple times. see
	 * {@link IMapReduceBroadcastReceiver#id()} private String id; /** Listens to this job only
	 */
	private String id;
	/**
	 * The job this IMapReduceBroadcastReceiver is responsible for.
	 */
	private Number640 jobId;

	/**
	 * To catch multiple same jobs if received in parallel.
	 */
	public Set<Job> jobs = Collections.synchronizedSet(new HashSet<>());

	/**
	 * 
	 * @param jobId
	 *            the job for which this {@link ExampleJobBroadcastReceiver} instance is responsible for (only one)
	 */
	public ExampleJobBroadcastReceiver(Number640 jobId) {
		this.jobId = jobId;
		this.id = ExampleJobBroadcastReceiver.class.getSimpleName() + "_" + jobId;
	}

	@Override
	public void receive(Message message, PeerMapReduce peerMapReduce) {
		// get the input from the broadcast message
		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {
			// the job data from the broadcast message (optional)
			Data jobData = input.get(NumberUtils.JOB_DATA);
			if (jobData != null) {
				synchronized (jobs) {
					// Deserialise the job from the broadcast message
					JobTransferObject serializedJob = ((JobTransferObject) jobData.object());
					Job job = Job.deserialize(serializedJob);

					// If it is not the job this instance is responsible for, simply return
					if (!job.id().equals(jobId)) {
						logger.info("Received job for wrong id: observing job [" + jobId.locationKey().shortValue()
								+ "], received job[" + job.id().locationKey().shortValue() + "]");
						return;
					}

					// Add the job if it is not contained
					if (!jobs.contains(job)) {
						jobs.add(job);
					} else {
						for (Job j : jobs) {
							if (j.id().equals(jobId)) {
								job = j;
							}
						}
					}

					// Invoke the next task to execute, determined from the broadcast input
					if (input.containsKey(NumberUtils.NEXT_TASK)) {
						Number640 nextTaskId = (Number640) input.get(NumberUtils.NEXT_TASK).object();
						Task task = job.findTask(nextTaskId);
						task.broadcastReceiver(input, peerMapReduce);
					} else {
						logger.info("Job was null");
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		ExampleJobBroadcastReceiver other = (ExampleJobBroadcastReceiver) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String id() {
		return id;
	}

}

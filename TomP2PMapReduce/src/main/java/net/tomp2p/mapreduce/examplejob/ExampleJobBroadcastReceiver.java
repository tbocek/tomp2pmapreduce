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

	public ExampleJobBroadcastReceiver(Number640 jobId) {
		this.jobId = jobId;
		this.id = ExampleJobBroadcastReceiver.class.getSimpleName() + "_" + jobId;
	}

	@Override
	public void receive(Message message, PeerMapReduce peerMapReduce) {

		NavigableMap<Number640, Data> input = message.dataMapList().get(0).dataMap();
		try {
			Data jobData = input.get(NumberUtils.JOB_DATA);
			if (jobData != null) {
				synchronized (jobs) {
					JobTransferObject serializedJob = ((JobTransferObject) jobData.object());
					Job job = Job.deserialize(serializedJob);
					if (!job.id().equals(jobId)) {
						logger.info("Received job for wrong id: observing job [" + jobId.locationKey().shortValue()
								+ "], received job[" + job.id().locationKey().shortValue() + "]");
						return;
					}
					if (!jobs.contains(job)) {
						jobs.add(job);
					} else {
						for (Job j : jobs) {
							if (j.id().equals(jobId)) {
								job = j;
							}
						}
					}
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

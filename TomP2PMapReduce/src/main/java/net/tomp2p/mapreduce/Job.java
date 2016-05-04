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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import net.tomp2p.mapreduce.utils.JobTransferObject;
import net.tomp2p.mapreduce.utils.NumberUtils;
import net.tomp2p.mapreduce.utils.SerializeUtils;
import net.tomp2p.mapreduce.utils.TransferObject;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 *
 * Users need to add <code>Task</code> and <code>IMapReduceBroadcastReceiver</code> to a <code>Job</code> to be able to execute a MapReduce job. Once these classes are added, <code>Job.start()</code>
 * will determine the first local <code>Task</code> to execute and start it.
 * 
 * @author Oliver Zihler
 */
final public class Job {

	private List<Task> tasks;
	private List<IMapReduceBroadcastReceiver> broadcastReceivers;
	private Number640 id;

	public Job() {
		this(new Number640(new Random()));
	}

	public Job(Number640 id) {
		this.id = id;
		this.broadcastReceivers = new ArrayList<>();
		this.tasks = new ArrayList<>();
	}

	public Number640 id() {
		return this.id;
	}

	public void addTask(Task task) {
		this.tasks.add(task);
	}

	public JobTransferObject serialize() throws IOException {
		JobTransferObject jTO = new JobTransferObject();
		jTO.id(id);
		for (Task task : tasks) {
			Map<String, byte[]> taskClassFiles = SerializeUtils.serializeClassFile(task.getClass());
			byte[] taskData = SerializeUtils.serializeJavaObject(task);
			TransferObject tto = new TransferObject(taskData, taskClassFiles, task.getClass().getName());
			jTO.addTask(tto);
		}
		return jTO;
	}

	public static Job deserialize(JobTransferObject jobToDeserialize) throws ClassNotFoundException, IOException {

		Job job = new Job(jobToDeserialize.id());
		for (TransferObject taskTransferObject : jobToDeserialize.taskTransferObjects()) {
			Map<String, Class<?>> taskClasses = SerializeUtils.deserializeClassFiles(taskTransferObject.classFiles());
			Task task = (Task) SerializeUtils.deserializeJavaObject(taskTransferObject.data(), taskClasses);
			job.addTask(task);
		}
		return job;
	}

	public void start(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		if (tasks.size() == 0) {
			throw new Exception("No Task defined. Cannot start execution without any Task to execute.");
		}
		if (broadcastReceivers.size() == 0) {
			throw new Exception("No IMapReduceBroadcastReceiver specified. Cannot start distributed execution without any implementation of these.");
		}
		List<TransferObject> broadcastReceiversTransferObjects = new ArrayList<>();
		for (IMapReduceBroadcastReceiver receiver : broadcastReceivers) {
			Map<String, byte[]> bcClassFiles = SerializeUtils.serializeClassFile(receiver.getClass());
			String bcClassName = receiver.getClass().getName();
			byte[] bcObject = SerializeUtils.serializeJavaObject(receiver);
			TransferObject t = new TransferObject(bcObject, bcClassFiles, bcClassName);
			broadcastReceiversTransferObjects.add(t);
		}
		input.put(NumberUtils.RECEIVERS, new Data(broadcastReceiversTransferObjects));
		input.put(NumberUtils.JOB_ID, new Data(id));
		input.put(NumberUtils.JOB_DATA, new Data(serialize()));
		Task startTask = this.findStartTask();
		if(startTask == null){
			throw new Exception("Could not find local task to execute. Did you specify the start task to have previousId set to null?");
		}else{
			startTask.broadcastReceiver(input, pmr);
		}

	}

	public Task findStartTask() {
		for (Task task : tasks) {
			if (task.previousId() == null) {// This marks the start
				return task;
			}
		}
		return null;
	}

	public Task findTask(Number640 taskId) {
		for (Task task : tasks) {
			if (task.currentId().equals(taskId)) {
				return task;
			}
		}
		return null;
	}

	public void addBroadcastReceiver(IMapReduceBroadcastReceiver receiver) {
		this.broadcastReceivers.add(receiver);
	}

	public List<Task> tasks() {
		return tasks;
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
		if (getClass() != obj.getClass())
			return false;
		Job other = (Job) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
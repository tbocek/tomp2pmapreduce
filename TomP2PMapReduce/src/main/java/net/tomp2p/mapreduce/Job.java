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
 * Users need to add {@link Task} and {@link IMapReduceBroadcastReceiver} to a {@link Job} to be able to execute a
 * MapReduce job. Once these classes are added, {@link #start()} will determine the first local {@link Task} (compare
 * {@link StartTask}) to execute (specified by the {@link Task#previousId()} to be null) and start it.
 * 
 * @see StartTask
 * 
 * 
 * @author Oliver Zihler
 */
final public class Job {

	/** All tasks to execute in a specified order (given by their id chaining, see {@link Task} */
	private List<Task> tasks;
	/** All actions taken on broadcast reception. See {@link IMapReduceBroadcastReceiver} */
	private List<IMapReduceBroadcastReceiver> broadcastReceivers;
	/** Job identifier to distinguish it from other jobs */
	private Number640 id;

	/**
	 * Defines a new job object with a random id.
	 */
	public Job() {
		this(new Number640(new Random()));
	}

	/**
	 * Defines a new job object with a specified identifier
	 * 
	 * @param id
	 *            identifier of this job
	 */
	public Job(Number640 id) {
		this.id = id;
		this.broadcastReceivers = new ArrayList<>();
		this.tasks = new ArrayList<>();
	}

	/**
	 * * Add a task to the job. Chaining of tasks needs to be defined outside a job before it is added.
	 * 
	 * @param task
	 *            task to execute
	 */
	public void addTask(Task task) {
		this.tasks.add(task);
	}

	/**
	 * 
	 * @return the serialised job containing all tasks and broadcast receivers in a serialised form, to be put into the
	 *         DHT using {@link PeerMapReduce#get()} or sent via broadcast
	 * @throws IOException
	 */
	public JobTransferObject serialize() throws IOException {
		JobTransferObject jTO = new JobTransferObject();
		jTO.jobId(id);
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
			Map<String, Class<?>> taskClasses = SerializeUtils.deserializeClassFiles(taskTransferObject.serialisedClassFiles());
			Task task = (Task) SerializeUtils.deserializeJavaObject(taskTransferObject.serialisedObject(), taskClasses);
			job.addTask(task);
		}
		return job;
	}

	/**
	 * Main entrance point to start a MapReduce job. The input is the same as for {@link Task#broadcastReceiver()}. This
	 * is intentional as #start() will determine the first task (whose {@link Task#previousId()} is null) and execute it
	 * locally, where it will also pass the input and {@link PeerMapReduce} instance for the first task to use it.
	 * 
	 * @param input
	 * @param pmr
	 * @throws Exception
	 */
	public void start(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception {
		if (tasks.size() == 0) {
			throw new Exception("No Task defined. Cannot start execution without any Task to execute.");
		}
		if (broadcastReceivers.size() == 0) {
			throw new Exception(
					"No IMapReduceBroadcastReceiver specified. Cannot start distributed execution without any implementation of these.");
		}
		List<TransferObject> broadcastReceiversTransferObjects = serializeBroadcastReceivers();
		input.put(NumberUtils.RECEIVERS, new Data(broadcastReceiversTransferObjects));
		input.put(NumberUtils.JOB_ID, new Data(id));
		input.put(NumberUtils.JOB_DATA, new Data(serialize()));
		Task startTask = this.findStartTask();
		if (startTask == null) {
			throw new Exception(
					"Could not find local task to execute. Did you specify the start task to have previousId set to null?");
		} else {
			startTask.broadcastReceiver(input, pmr);
		}

	}

	private List<TransferObject> serializeBroadcastReceivers() throws IOException {
		List<TransferObject> broadcastReceiversTransferObjects = new ArrayList<>();
		for (IMapReduceBroadcastReceiver receiver : broadcastReceivers) {
			Map<String, byte[]> bcClassFiles = SerializeUtils.serializeClassFile(receiver.getClass());
			String bcClassName = receiver.getClass().getName();
			byte[] bcObject = SerializeUtils.serializeJavaObject(receiver);
			TransferObject t = new TransferObject(bcObject, bcClassFiles, bcClassName);
			broadcastReceiversTransferObjects.add(t);
		}
		return broadcastReceiversTransferObjects;
	}

	/**
	 * @return the first task encountered whose previoudId() is null. Returns null if none is found.
	 */
	public Task findStartTask() {
		for (Task task : tasks) {
			if (task.previousId() == null) {// This marks the start
				return task;
			}
		}
		return null;
	}

	/**
	 * @param taskId
	 *            the task to find
	 * @return the task that has the corresponding taskId as {@link Task#currentId()}. Else null
	 */
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

	/**
	 * @return identifier of the job
	 */
	public Number640 id() {
		return this.id;
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
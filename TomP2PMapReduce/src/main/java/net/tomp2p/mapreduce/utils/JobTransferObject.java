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
package net.tomp2p.mapreduce.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.peers.Number640;

/**
 * Complete serialised job with all contained tasks. {@link IMapReduceBroadcastReceiver} instances are not contained as
 * they need to be sent via broadcast for every node to instantiate them on reception (there is no reason why they
 * should be stored within the DHT). On the other hand, {@link Task}s of a {@link Job} should be possible to send via
 * broadcast or put the into the DHT. This is what this class allows: it is completely serialised and can thus be
 * transferred via broadcast or stored withing the DHT. This is actually only a convenience method as every {@link Task}
 * and {@link Job} id could also be directly stored in the DHT or sent via broadcast.
 * 
 * @author Oliver Zihler
 *
 */
public class JobTransferObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6702141212731486921L;
	/** All tasks in serialised form */
	private List<TransferObject> taskTransferObjects = new ArrayList<>();
	/** The id of the job */
	private Number640 jobId;

	/**
	 * 
	 * @param tto
	 *            {@link Task} in a serialised form of {@link TransferObject}
	 */
	public void addTask(TransferObject tto) {
		this.taskTransferObjects.add(tto);
	}

	/**
	 * 
	 * @return all {@link Task} in a serialised form of {@link TransferObject}
	 */
	public List<TransferObject> taskTransferObjects() {
		return taskTransferObjects;
	}

	public void jobId(Number640 id) {
		this.jobId = id;
	}

	public Number640 id() {
		return jobId;
	}

}

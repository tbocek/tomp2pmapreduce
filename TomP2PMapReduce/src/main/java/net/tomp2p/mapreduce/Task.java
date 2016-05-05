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

import java.io.Serializable;
import java.util.NavigableMap;

import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

/**
 * Main Abstraction Point for a Map or Reduce Function. Users need to define previousId and currentId. currentId
 * corresponds to the id of this task. previousId is the id of the task that comes before in the task chain.
 *
 * @author Oliver Zihler
 */
public abstract class Task implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9198452155865807410L;
	/** the ID of the task to be executed before this task */
	private final Number640 previousId;
	/** The ID of this task. To be used as a previousId in the succeeding task */
	private final Number640 currentId;

	public Task(Number640 previousId, Number640 currentId) {
		this.previousId = previousId;
		this.currentId = currentId;
	}

	/**
	 * Main extension point. Corresponds to map(K key, V value) and reduce(K key, Iterator<V> values) interfaces of
	 * MapReduce, see e.g.
	 * http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf.
	 * 
	 * Define map or reduce functions (or any other extension) by implementing this method. Input provides the
	 * 
	 * @param input
	 *            defines the input for this task. E.g. location of files to process locally
	 * @param pmr
	 *            connection to the dht. Used to get and put data from and to the DHT and to send broadcast messages.
	 * @throws Exception
	 *             any exception that can occur in the task.
	 */
	public abstract void broadcastReceiver(NavigableMap<Number640, Data> input, PeerMapReduce pmr) throws Exception;

	public Number640 currentId() {
		return this.currentId;
	}

	public Number640 previousId() {
		return this.previousId;
	}

	

}

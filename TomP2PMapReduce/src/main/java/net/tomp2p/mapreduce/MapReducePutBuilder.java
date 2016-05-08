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

import net.tomp2p.dht.GetBuilder;
import net.tomp2p.mapreduce.utils.MapReduceValue;
import net.tomp2p.peers.Number160;

/**
 * Class similar to {@link PutBuilder}. Additionally provides the possibility to restrict access to that data
 * item. Created when {@link PeerMapReduce#put()} is invoked. Do not create this externally but use the provided wrapper
 * of {@link PeerMapReduce}.
 * 
 * @author Oliver Zihler
 *
 */
public class MapReducePutBuilder extends BaseMapReduceBuilder<MapReducePutBuilder> {
	/** Actual data to store */
	private MapReduceValue data;

	public MapReducePutBuilder(PeerMapReduce peerMapReduce, Number160 locationKey, Number160 domainKey) {
		super(peerMapReduce, locationKey, domainKey);
		self(this);
	}

	/**
	 * @return future to add a listener to to define what happens once the put finished.
	 */
	public FutureMapReduceData start() {
		return new DistributedTask(peerMapReduce.peer().distributedRouting(), peerMapReduce.taskRPC()).putTaskData(this,
				super.start());
	}

	/**
	 * @param value
	 *            to store
	 * @param nrOfExecutions
	 *            the number of times this value should be accessible
	 * @return
	 */
	public MapReducePutBuilder data(Object value, int nrOfExecutions) {
		this.data = new MapReduceValue(value, nrOfExecutions);
		return this;
	}

	public MapReduceValue data() {
		return this.data;
	}

}

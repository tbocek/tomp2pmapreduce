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

import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;
import net.tomp2p.mapreduce.Task;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

/**
 * Contains some convenience methods to easier define own Number640 keys and required and optional keys to be used in
 * the broadcast input {@link NavigableMap} (see e.g.
 * {@link Task#broadcastReceiver(java.util.NavigableMap, net.tomp2p.mapreduce.PeerMapReduce)})
 * 
 * @author Oliver Zihler
 *
 * @see <a href="http://tinyurl.com/csgmtmapred">Documentation</a>
 */
public class NumberUtils {

	/** The input data --> used to remove active flag listeners --> REQUIRED */
	public static final Number640 INPUT_STORAGE_KEY = NumberUtils.allSameKeys("INPUT_STORAGE_KEY");
	/** Currently required to know who sent the broadcast message */
	public static final Number640 SENDER = allSameKeys("SENDER");
	/** The result of the processing of the input data */
	public static final Number640 OUTPUT_STORAGE_KEY = NumberUtils.allSameKeys("OUTPUT_STORAGE_KEY");
	/** Used internally to transfer the value */
	public static final Number640 VALUE = NumberUtils.allSameKeys("VALUE");
	/** Used internally to transfer the broadcast input */
	public static final Number640 OLD_BROADCAST = NumberUtils.allSameKeys("OLD_BROADCAST");
	/**
	 * Used internally to store the {@link IMapReduceBroadcastReceiver} to retrieve them by the
	 * {@link MapReduceBroadcastHandler}
	 */
	public static final Number640 RECEIVERS = allSameKeys("RECEIVERS");
	/** The task that is currently executed */
	public static final Number640 CURRENT_TASK = allSameKeys("CURRENT_TASK");
	/** The next task to execute */
	public static final Number640 NEXT_TASK = allSameKeys("NEXT_TASK");
	/** If the complete job is sent over broadcast */
	public static final Number640 JOB_DATA = allSameKeys("JOB_KEY");
	/** If only the job id to retrieve it from the DHT should be sent */
	public static final Number640 JOB_ID = allSameKeys("JOB_ID");

	/** Local counter to generate a Number640 key that has location- and domainKeys as hashes of the counter */
	private static int counter = 0;

	/**
	 * 
	 * @return a new Number640 key with location- and domainKeys corresponding to an internal counter (starts with 0)
	 */
	public static Number640 next() {
		++counter;
		return new Number640(Number160.createHash(counter), Number160.createHash(counter), Number160.ZERO,
				Number160.ZERO);
	}

	/**
	 * Resets the counter invoked on each call to {#next()} to 0 again
	 */
	public static void reset() {
		counter = 0;
	}

	/**
	 * 
	 * @return the current counter used in {@link #next()}
	 */
	public static int counter() {
		return counter;
	}

	/**
	 * Convenience method that creates a new {@link Number640} key (hash(key),hash(key),ZERO,ZERO) based on a
	 * {@link String} input key
	 * 
	 * @param key
	 *            {@link String} input key to generate {@link Number640} from
	 * @return {@link Number640} key of the form (hash(key),hash(key),Number160,ZERO,ZERO)
	 */
	public static Number640 allSameKeys(String key) {
		return new Number640(Number160.createHash(key), Number160.createHash(key), Number160.ZERO, Number160.ZERO);
	}

	/**
	 * Convenience method that creates a new {@link Number640} key (hash(key),hash(key),ZERO,ZERO) based on a int input
	 * key
	 * 
	 * @param key
	 *            int input key to generate {@link Number640} from
	 * @return {@link Number640} key of the form (hash(key),hash(key),Number160,ZERO,ZERO)
	 */
	public static Number640 allSameKeys(int key) {
		return new Number640(Number160.createHash(key), Number160.createHash(key), Number160.ZERO, Number160.ZERO);
	}
}

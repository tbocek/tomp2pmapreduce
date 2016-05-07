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

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

public class NumberUtils {
	/** The input data --> used to remove active flag listeners */
	public static final Number640 INPUT_STORAGE_KEY = NumberUtils.allSameKeys("INPUT_STORAGE_KEY");
	/** The result of the processing of the input data */
	public static final Number640 OUTPUT_STORAGE_KEY = NumberUtils.allSameKeys("OUTPUT_STORAGE_KEY");
	public static final Number640 VALUE = NumberUtils.allSameKeys("VALUE");
	public static final Number640 OLD_BROADCAST = NumberUtils.allSameKeys("OLD_BROADCAST");
	public static final Number640 RECEIVERS = allSameKeys("RECEIVERS");
	public static final Number640 CURRENT_TASK = allSameKeys("CURRENT_TASK");
	public static final Number640 NEXT_TASK = allSameKeys("NEXT_TASK");
	public static final Number640 JOB_DATA = allSameKeys("JOB_KEY");
	public static final Number640 SENDER = allSameKeys("SENDER");
	public static final Number640 JOB_ID = allSameKeys("JOB_ID");
	// public static final Number640 INPUT_STORAGE_KEYS = allSameKey("INPUT_STORAGE_KEY");
	private static int counter = 0;

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

	public static Number640 allSameKeys(String key) {
		return new Number640(Number160.createHash(key), Number160.createHash(key), Number160.ZERO, Number160.ZERO);
	}

	public static Number640 allSameKeys(int key) {
		return new Number640(Number160.createHash(key), Number160.createHash(key), Number160.ZERO, Number160.ZERO);
	}
}

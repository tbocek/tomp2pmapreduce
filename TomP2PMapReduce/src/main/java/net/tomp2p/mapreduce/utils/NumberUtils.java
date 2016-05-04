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
	public static final Number640 INPUT_STORAGE_KEY = NumberUtils.allSameKey("INPUT_STORAGE_KEY"); //The input data --> used to remove active flag listeners
	public static final Number640 OUTPUT_STORAGE_KEY = NumberUtils.allSameKey("OUTPUT_STORAGE_KEY"); //The result of the processing of the input data
	public static final Number640 VALUE = NumberUtils.allSameKey("VALUE");
	public static final Number640 OLD_BROADCAST = NumberUtils.allSameKey("OLD_BROADCAST");
	public static final Number640 RECEIVERS = allSameKey("RECEIVERS");
	public static final Number640 CURRENT_TASK = allSameKey("CURRENT_TASK");
	public static final Number640 NEXT_TASK = allSameKey("NEXT_TASK");
	public static final Number640 JOB_DATA = allSameKey("JOB_KEY");
	public static final Number640 SENDER = allSameKey("SENDER");
	public static final Number640 JOB_ID = allSameKey("JOB_ID");
//	public static final Number640 INPUT_STORAGE_KEYS = allSameKey("INPUT_STORAGE_KEY");
	private static int counter = 0;

	public static Number640 next() {
		++counter;
		return new Number640(Number160.createHash(counter), Number160.createHash(counter), Number160.ZERO, Number160.ZERO);
	}

	public static void reset() {
		counter = 0;
	}

	public static Number640 allSameKey(String string) {
		return new Number640(Number160.createHash(string), Number160.createHash(string), Number160.ZERO, Number160.ZERO);
	}

	public static Number640 allSameKey(int nr) {
		return new Number640(Number160.createHash(nr), Number160.createHash(nr), Number160.ZERO, Number160.ZERO);
	}
}

/* 
 * Copyright 2016 Oliver Zihler 
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

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.mapreduce.FutureMapReduceData;
import net.tomp2p.mapreduce.MapReducePutBuilder;
import net.tomp2p.mapreduce.PeerMapReduce;
import net.tomp2p.peers.Number160;

/**
 * Convenience method used in the example job to read and split files into certain sizes. Caution: Splitting was
 * abandonned eventually and is not tested currently. The maxFileSize parameter should therefore stay larger than the
 * actual size of the files to simply read it completely and distribute it to the DHT. Probably better to use own
 * implementations instead until this is resolved and tested again.
 * 
 * @author Oliver Zihler
 *
 */
public class FileSplitter {
	private static Logger logger = LoggerFactory.getLogger(FileSplitter.class);

	/**
	 * Splits a text file located at keyFilePath into pieces of at max maxFileSize. Words are not split! Meaning, this
	 * method is appropriate for word count. Then, file splits are put into the DHT and a map with all resulting
	 * location keys and corresponding {@link FutureMapReduceData} is returned to add listener to them outside.
	 * 
	 * @param keyfilePath
	 * @param dht
	 *            connection to the DHT
	 * @param maxFileSize
	 *            how much a split should contain
	 * @param fileEncoding
	 *            (e.g. UTF-8)
	 * @return a map containing all generated dht keys of the file splits to retrieve them together with the FuturePut
	 *         to be called in Futures.whenAllSucess(...)
	 */
	public static Map<Number160, FutureMapReduceData> splitWithWordsAndWrite(String keyfilePath, PeerMapReduce pmr,
			int nrOfExecutions, Number160 domainKey, int maxFileSize, String fileEncoding) {
		Map<Number160, FutureMapReduceData> dataKeysAndFuturePuts = Collections.synchronizedMap(new HashMap<>());
		try {
			RandomAccessFile aFile = new RandomAccessFile(keyfilePath, "r");
			FileChannel inChannel = aFile.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(maxFileSize);
			String split = "";
			String actualData = "";
			String remaining = "";
			while (inChannel.read(buffer) > 0) {
				buffer.flip();
				byte[] data = new byte[buffer.limit()];
				buffer.get(data);
				split = new String(data);
				split = remaining += split;

				remaining = "";

				if (split.getBytes(Charset.forName(fileEncoding)).length >= maxFileSize) {
					actualData = split.substring(0, split.lastIndexOf(" ")).trim();
					remaining = split.substring(split.lastIndexOf(" ") + 1, split.length()).trim();
				} else {
					actualData = split.trim();
					remaining = "";
				}
				Number160 dataKey = Number160.createHash(keyfilePath);
				MapReducePutBuilder put = pmr.put(dataKey, domainKey, actualData, nrOfExecutions);
				FutureMapReduceData futureTask = put.start();
				dataKeysAndFuturePuts.put(dataKey, futureTask);

				buffer.clear();
				split = "";
				actualData = "";
			}
			inChannel.close();
			aFile.close();
		} catch (Exception e) {
			logger.warn("Exception on reading file at location: " + keyfilePath, e);
		}

		return dataKeysAndFuturePuts;
	}

}

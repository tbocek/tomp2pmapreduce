package net.tomp2p.mapreduce.utils;

import java.util.Map;
import java.util.NavigableMap;

import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class InputUtils {
	/**
	 * Simplified way of reusing inputs again from previous input if not all inputs should be sent by broadcast. Only
	 * usable if NumberUtils.allSameKeys() was used to define the keys.
	 * 
	 * @param input
	 *            received input
	 * @param keptInput
	 *            part of the input to keep
	 * @param keyStringsToKeep
	 *            strings of the input to keep.
	 */
	public static void keepInputKeyValuePairs(NavigableMap<Number640, Data> input, Map<Number640, Data> keptInput,
			String[] keyStringsToKeep) {
		for (String keyString : keyStringsToKeep) {
			if (input.containsKey(NumberUtils.allSameKey(keyString))) {
				keptInput.put(NumberUtils.allSameKey(keyString), input.get(NumberUtils.allSameKey(keyString)));
			}
		}
	}
}

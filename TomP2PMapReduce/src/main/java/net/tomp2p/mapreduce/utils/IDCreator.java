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


import java.util.Random;

/**
 * More or less impossible to ever have colliding id's if I do it like that...
 * 
 * @author Oliver Zihler
 *
 */
public enum IDCreator {
	INSTANCE;
	private static long yetAnotherLocalCounter = 0;
	private final Random random = new Random();

	public String createTimeRandomID(final String name) { 
		// TS == Timestamp
		// RND == Random long
		// LC == local counter... just such that at least locally, the id's are counted
		return name.toUpperCase() + 
				"[TS(" + System.currentTimeMillis() + ")_RND(" + random.nextLong() + ")"
						+ "_LC(" + yetAnotherLocalCounter++ + ")]";
	}

}
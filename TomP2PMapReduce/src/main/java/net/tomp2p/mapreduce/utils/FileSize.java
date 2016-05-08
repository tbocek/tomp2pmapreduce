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

/**
 * Determines some standard file sizes to be used without knowing the actual value... Simply for convenience and
 * readability.
 * 
 * @author Oliver Zihler
 *
 */
public enum FileSize {
	BYTE(1), TWO_BYTES(2), FOUR_BYTES(4), EIGHT_BYTES(8), SIXTEEN_BYTES(16), THIRTY_TWO_BYTES(32), SIXTY_FOUR_BYTES(
			64), ONE_TWENTY_EIGHT_BYTES(128), KILO_BYTE(1024), TWO_KILO_BYTES(2 * KILO_BYTE.value()), FOUR_KILO_BYTES(
					4 * KILO_BYTE.value()), EIGHT_KILO_BYTES(8 * KILO_BYTE.value()), SIXTEEN_KILO_BYTES(
							16 * KILO_BYTE.value()), THIRTY_TWO_KILO_BYTES(
									32 * KILO_BYTE.value()), SIXTY_FOUR_KILO_BYTES(64 * KILO_BYTE.value()), MEGA_BYTE(
											KILO_BYTE.value() * KILO_BYTE.value()), TWO_MEGA_BYTES(2 * MEGA_BYTE
													.value()), FOUR_MEGA_BYTES(4 * MEGA_BYTE.value()), EIGHT_MEGA_BYTES(
															8 * MEGA_BYTE.value()), SIXTEEN_MEGA_BYTES(
																	16 * MEGA_BYTE.value()), THIRTY_TWO_MEGA_BYTES(32
																			* MEGA_BYTE.value()), SIXTY_FOUR_MEGA_BYTES(
																					64 * KILO_BYTE.value());

	/** The actual value behind the enum */
	private int value;

	/**
	 * Specifies a new enum with a certain number associated to it
	 * 
	 * @param value
	 *            the actual value behind the enum
	 */
	FileSize(int value) {
		this.value = value;
	}

	/**
	 * get the value associated with this file size name
	 * 
	 * @return the actual value behind the enum
	 */
	public int value() {
		return value;
	}

}
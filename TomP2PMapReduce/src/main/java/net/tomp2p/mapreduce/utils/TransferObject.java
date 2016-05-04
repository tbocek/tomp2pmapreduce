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
import java.util.Map;

public class TransferObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8971732001157216939L;
	private byte[] data;
	private Map<String, byte[]> classFiles;
	private String className;

	public TransferObject(byte[] data, Map<String, byte[]> classFiles, String className) {
		this.data = data;
		this.classFiles = classFiles;
		this.className = className;
	}

	public byte[] data() {
		return this.data;
	}

	public Map<String, byte[]> classFiles() {
		return this.classFiles;
	}

	public String className() {
		return this.className;
	}
}

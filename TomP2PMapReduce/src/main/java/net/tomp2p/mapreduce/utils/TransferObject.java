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

import net.tomp2p.mapreduce.IMapReduceBroadcastReceiver;

/**
 * Wraps a serialised Java object and its corresponding serialised class files and the main class's name to be
 * transferred to other nodes. See also {@link SerializeUtils}. It is mostly used to store user-defined {@link Task} and
 * {@link IMapReduceBroadcastReceiver} instances inside a {@link JobTransferObject}. As it is completely serialised, it
 * can be stored in the DHT or sent over the network via broadcasts, however the user likes.
 * 
 * @author Oliver Zihler
 *
 */
public class TransferObject implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8971732001157216939L;
	/** The transfered object in serialised form. See {@link SerializeUtils#serializeJavaObject(Object)} and {@link SerializeUtils#deserializeJavaObject(byte[], Map)(Object)} */
	private byte[] serialisedObject;
	/** The corresponding class files (together with all contained declared and anonymous classes. See {@link SerializeUtils#serializeClassFile(Class)} and {@link SerializeUtils#deserializeClassFiles(Map)} */
	private Map<String, byte[]> serialisedClassFiles;
	/** The actual name of the serialised object to know which one to invoke */
	private String mainClassName;

	public TransferObject(byte[] serialisedObject, Map<String, byte[]> serialisedClassFiles, String mainClassName) {
		this.serialisedObject = serialisedObject;
		this.serialisedClassFiles = serialisedClassFiles;
		this.mainClassName = mainClassName;
	}

	/**
	 * @return The transfered object in serialised form
	 */
	public byte[] serialisedObject() {
		return this.serialisedObject;
	}

	/**
	 * @return The corresponding class files (together with all contained declared and anonymous classes
	 */
	public Map<String, byte[]> serialisedClassFiles() {
		return this.serialisedClassFiles;
	}

	/**
	 * @return The actual name of the serialised object to know which one to invoke
	 */
	public String mainClassName() {
		return this.mainClassName;
	}
}

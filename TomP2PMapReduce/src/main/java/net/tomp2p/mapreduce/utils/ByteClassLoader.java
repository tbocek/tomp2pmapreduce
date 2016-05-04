/* 
 * Copyright 2016 Oliver Zihler 
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

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Custom class loader to load classes from serialised (byte arrays) classes.
 * 
 * @author Oliver Zihler
 *
 */
public class ByteClassLoader extends ClassLoader {

	private final Map<String, byte[]> extraClassDefs;

	public ByteClassLoader(ClassLoader parent, Map<String, byte[]> extraClassDefs) {
		super(parent);
		this.extraClassDefs = new HashMap<String, byte[]>(extraClassDefs);
	}

	@Override
	protected Class<?> findClass(final String name) throws ClassNotFoundException {
		byte[] classBytes = this.extraClassDefs.remove(name);
		if (classBytes != null) {
			return defineClass(name, classBytes, 0, classBytes.length);
		}
		return super.findClass(name);
	}
}
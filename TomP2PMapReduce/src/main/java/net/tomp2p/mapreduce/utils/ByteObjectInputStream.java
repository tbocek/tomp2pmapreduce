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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Map;

public class ByteObjectInputStream extends ObjectInputStream {
	private Map<String, Class<?>> classes;

	public ByteObjectInputStream(InputStream in, Map<String, Class<?>> classes) throws IOException {
		super(in);
		this.classes = classes;
	}

	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
		Class<?> c = classes.get(desc.getName());
		if (c != null) {
			return c;
		}
		return super.resolveClass(desc); // To change body of generated methods,
											// choose Tools | Templates.
	}

}
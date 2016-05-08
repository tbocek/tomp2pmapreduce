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
package net.tomp2p.mapreduce;

import java.io.Serializable;

import net.tomp2p.message.Message;

/**
 * Actions taken on every broadcast reception. {@link #receive()} will be invoked by {@link MapReduceBroadcastReceiver},
 * which passes the complete broadcast message to it. The interface is intended to then find the next task to execute
 * and pass the input wrapped inside message to it. See also the documentation
 * <a href="http://tinyurl.com/csgmtmapred">here</a>, chapter 5.
 * 
 * @author Oliver Zihler
 *
 */
public interface IMapReduceBroadcastReceiver extends Serializable {

	/**
	 * 
	 * @param message
	 *            the complete broadcast received from the MapReduceBroadcastHandler
	 * @param PeerMapReduce
	 *            instance to be passed to the next task to retrieve and store data in the DHT or send broadcasts.
	 */
	public void receive(Message message, PeerMapReduce PeerMapReduce);

	/**
	 * @return a subclass-wide ID to avoid adding the same handler on a node multiple times.
	 */
	public String id();

}

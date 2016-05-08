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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to remove an active on data flag when a peer successfully processed a data item such that the broadcast is not
 * sent out again. See {@link PeerConnectionCloseListener} which is responsible for sending out a broadcast in case the
 * node accessing a data item fails. Invoked by {@link MapReduceBroadcastHandler} on receiving a message that the node
 * with the according {@link PeerAddress} successfully finished processing this data item.
 * 
 * @author Oliver Zihler
 *
 */
public class PeerConnectionActiveFlagRemoveListener {
	private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionActiveFlagRemoveListener.class);

	private AtomicBoolean activeOnDataFlag;
	private PeerAddressStorageKeyTuple toAcquire;

	public PeerConnectionActiveFlagRemoveListener(PeerAddressStorageKeyTuple toAcquire,
			AtomicBoolean activeOnDataFlag) {
		this.toAcquire = toAcquire;
		this.activeOnDataFlag = activeOnDataFlag;
	}

	public boolean turnOffActiveOnDataFlag(PeerAddressStorageKeyTuple received) throws Exception {
		if (this.toAcquire.equals(received)) {
			LOG.info("Received triple I'm observing: active set to false for triple [" + toAcquire + "]!");
			activeOnDataFlag.set(false);
			return true;
		} else {
			LOG.info("Ignored triple: listener observes: [" + toAcquire + "] but received: [" + received + "]");
			return false;
		}
	}

	public PeerAddressStorageKeyTuple tupleToAcquire() {
		return toAcquire;
	}

}

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

import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;

public class Triple {
	PeerAddress peerAddress;
	Number640 storageKey; 
	public Triple(PeerAddress peerAddress, Number640 storageKey) {
 		this.peerAddress = peerAddress;
		this.storageKey = storageKey; 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((peerAddress == null) ? 0 : peerAddress.hashCode());
		result = prime * result + ((storageKey == null) ? 0 : storageKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false; 
		Triple other = (Triple) obj;
		if (peerAddress == null) {
			if (other.peerAddress != null)
				return false;
		} else if (!peerAddress.equals(other.peerAddress))
			return false;
		if (storageKey == null) {
			if (other.storageKey != null)
				return false;
		} else if (!storageKey.equals(other.storageKey))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Triple [peerAddress=" + peerAddress.peerId().shortValue() + ", storageKey=" + storageKey.locationAndDomainKey().intValue() + "]";
	}
	
	

 
}

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

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.PeerAddress;

/**
 * Used internally by {@link DistributedTask}
 * 
 * @author Oliver Zihler
 *
 */
public interface MapReduceOperationMapper {

	FutureResponse create(ChannelCreator channelCreator, PeerAddress next);

	void interMediateResponse(FutureResponse futureResponse);

	void response(FutureMapReduceData futureTask, FutureDone<Void> futuresCompleted);

}

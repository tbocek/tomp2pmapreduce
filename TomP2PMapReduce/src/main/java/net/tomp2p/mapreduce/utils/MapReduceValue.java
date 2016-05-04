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

public final class MapReduceValue implements Serializable {
	private static final long serialVersionUID = 4597498234385313114L;
	private final String id;
	private final Object value;
	private final int nrOfExecutions;
	private int currentNrOfExecutions;

	public MapReduceValue(final Object value, final int nrOfExecutions) {
		if (value == null) {
			throw new NullPointerException("Value cannot be null");
		}
		this.value = value;
		this.id = IDCreator.INSTANCE.createTimeRandomID(MapReduceValue.class.getSimpleName() + "_" + value);
		this.nrOfExecutions = (nrOfExecutions <= 1 ? 1 : nrOfExecutions);
		this.currentNrOfExecutions = 0;
	}

	/**
	 * 
	 * @return the actual value if it can be executed. Else returns null.
	 */
	public Object tryAcquireValue() {
		if (nrOfExecutions > this.currentNrOfExecutions) {
			++this.currentNrOfExecutions;
			return value;
		} else {
			return null;
		}
	}
	
	public int currentNrOfExecutions(){
		return this.currentNrOfExecutions;
	}

	/**
	 * Decrements the number of executions of this value. Allows a value to become executable again. Used when a peer does not complete execution. Should not be used when peer completed execution (number of execution should stay as high as the successful execution).
	 */
	public void tryDecrementCurrentNrOfExecutions() {
		if (this.currentNrOfExecutions > 0) {
			--this.currentNrOfExecutions;
		}
	}

	@Override
	public String toString() {
		return "MapReduceValue([" + value + "], #execs[" + nrOfExecutions + "], #current[" + currentNrOfExecutions + "])";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + currentNrOfExecutions;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + nrOfExecutions;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MapReduceValue other = (MapReduceValue) obj;
		if (currentNrOfExecutions != other.currentNrOfExecutions)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (nrOfExecutions != other.nrOfExecutions)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public int nrOfExecutions() {
		// TODO Auto-generated method stub
		return nrOfExecutions;
	}

}

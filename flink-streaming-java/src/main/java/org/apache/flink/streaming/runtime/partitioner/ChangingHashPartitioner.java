/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.partitioner;

import hu.sztaki.drc.partitioner.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.repartitioning.PartitionerChangeListener;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.MathUtils;

public class ChangingHashPartitioner<T> extends HashPartitioner<T> implements PartitionerChangeListener {

	private Partitioner partitioner;

	public ChangingHashPartitioner(final KeySelector<T, ?> keySelector, final int numPartitions) {
		super(keySelector);

		// Default partitioner
		partitioner = new Partitioner() {

			@Override
			public int size() {
				return numPartitions;
			}

			@Override
			public int get(Object key) {
				return MathUtils.murmurHash(key.hashCode()) % numPartitions;
			}
		};

	}

	private void setPartitioner(Partitioner partitioner) {
		this.partitioner = partitioner;
	}

	@Override
	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record, int numberOfOutputChannels) {
		Object key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}

		try {
			returnArray[0] = partitioner.get(key);
		} catch (IndexOutOfBoundsException e) {
			System.out.println();
			partitioner.get(key);
		}

		return returnArray;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return super.copy();
	}

	@Override
	public void onPartitionerChange(Partitioner partitioner) {
		setPartitioner(partitioner);
	}
}

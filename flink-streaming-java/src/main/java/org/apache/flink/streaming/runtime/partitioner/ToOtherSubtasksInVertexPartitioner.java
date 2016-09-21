package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.core.io.IOReadableWritable;

// TODO
public class ToOtherSubtasksInVertexPartitioner extends StreamPartitioner {

	private final int[] outputs = new int[1];

	@Override
	public int[] selectChannels(IOReadableWritable record, int numChannels) {
		outputs[0] = 1;
		return outputs;
	}

	@Override
	public StreamPartitioner copy() {
		return this;
	}
}

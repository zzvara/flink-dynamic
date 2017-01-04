package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class CheckpointBarrierWithRepartitioner extends CheckpointBarrier {

	private int partitionerVersion;

	public CheckpointBarrierWithRepartitioner() {}

	public CheckpointBarrierWithRepartitioner(long id, long timestamp, int partitionerVersion) {
		super(id, timestamp);
		this.partitionerVersion = partitionerVersion;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		out.writeInt(partitionerVersion);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		partitionerVersion = in.readInt();
	}

	public int getPartitionerVersion() {
		return partitionerVersion;
	}
}

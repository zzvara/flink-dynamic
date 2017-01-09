package org.apache.flink.streaming.repartitioning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class RepartitioningTest {

	@Test
	public void test() throws Exception {
		final long sleepTimeInMillis = 5;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setParallelism(4);

		env.addSource(new SourceFunction<String>() {

			public boolean running = false;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				ArrayList<String> keys = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f"));
				Random random = new Random();

				running = true;

				while (running) {
					String randomKey = keys.get(random.nextInt(keys.size()));
					ctx.collect(randomKey);
					Thread.sleep(sleepTimeInMillis);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		}).map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		}).setParallelism(4)
			.keyBy(new KeySelector<String, String>() {
				@Override
				public String getKey(String value) throws Exception {
					return value;
				}
			})
			.map(new RichMapFunction<String, Tuple2<String, Integer>>() {

				private transient ValueState<Integer> cnt;

				@Override
				public void open(Configuration config) {
					ValueStateDescriptor<Integer> descriptor =
						new ValueStateDescriptor<Integer>(
							"cnt", // the state name
							TypeInformation.of(new TypeHint<Integer>() {}), // type information
							0); // default value of the state, if nothing was set
					cnt = getRuntimeContext().getState(descriptor);
				}

				@Override
				public Tuple2<String, Integer> map(String value) throws Exception {
					int currCnt = cnt.value() + 1;
					cnt.update(currCnt);
					return new Tuple2<>(value, currCnt);
				}

			}).setParallelism(4)
			.map(new MapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {
				@Override
				public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
					return value;
				}
			}).setParallelism(4)
			.addSink(new SinkFunction<Tuple2<String, Integer>>() {
				@Override
				public void invoke(Tuple2<String, Integer> value) throws Exception {

				}
			});


		env.execute();
	}
}

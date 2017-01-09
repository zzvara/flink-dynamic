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
package org.apache.flink.streaming.repartitioning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SimpleRepartTest {

	@Test
	public void test() throws Exception {
		final long sleepTimeInMillis = 5;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setParallelism(4);

		env.addSource(new SourceFunction<String>() {
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				List<String> elems = Arrays.asList("a", "b", "c", "d", "e", "f");
				for (int i = 0; i < 1000; i++) {

				}
			}

			@Override
			public void cancel() {

			}
		})
			.map(new MapFunction<String, String>() {
				@Override
				public String map(String value) throws Exception {
					return value;
				}
			})
			.keyBy(new KeySelector<String, String>() {
				@Override
				public String getKey(String value) throws Exception {
					return value;
				}
			})
			.map(new RichMapFunction<String, Tuple2<String, Integer>>() {

				private transient ValueState<Integer> cnt;
				private int subtask;

				@Override
				public void open(Configuration config) {
					ValueStateDescriptor<Integer> descriptor =
						new ValueStateDescriptor<Integer>(
							"cnt", // the state name
							TypeInformation.of(new TypeHint<Integer>() {
							}), // type information
							0); // default value of the state, if nothing was set
					cnt = getRuntimeContext().getState(descriptor);
					subtask = getRuntimeContext().getIndexOfThisSubtask();
				}

				@Override
				public Tuple2<String, Integer> map(String value) throws Exception {
					int currCnt = cnt.value() + 1;
					cnt.update(currCnt);
					return new Tuple2<>(value, currCnt);
				}

			})
			.addSink(new SinkFunction<Tuple2<String, Integer>>() {
				@Override
				public void invoke(Tuple2<String, Integer> value) throws Exception {
					System.out.println("OUT: " + value);
				}
			});

		env.execute();
	}
}

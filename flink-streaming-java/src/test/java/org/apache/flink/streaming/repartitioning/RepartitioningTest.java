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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.repartitioning.RedistributeStateHandler;
import org.apache.flink.runtime.repartitioning.RedistributeStateHandler$;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;
import utils.distribution.Distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class RepartitioningTest {
	@Test
	public void test() throws Exception {
		final int sleepTimeInMillis = 500000;
		final int parallelism = 50;
		RedistributeStateHandler$.MODULE$.setPartitions(parallelism);

		GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR"));
		Configuration conf = GlobalConfiguration.getConfiguration();
		conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		final StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment(parallelism, conf);

		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setParallelism(parallelism);

		env.addSource(new SourceFunction<String>() {
			protected Distribution distribution = Distribution.zeta(1.5, 1000, 100000);
			//protected Distribution distribution = Distribution.uniform(100000);

			public boolean running = false;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				running = true;

				while (running) {
					String randomKey = Integer.toString(distribution.sample());
					ctx.collect(randomKey);
					Thread.sleep(0, sleepTimeInMillis);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		})
		.map(new MapFunction<String, String>() {
		@Override
		public String map(String value) throws Exception {
			return value;
		}
	})
		.setParallelism(parallelism)
		.keyBy(new KeySelector<String, String>() {
			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		})
		.map(new RichMapFunction<String, Tuple3<Integer, String, Integer>>() {

			private transient ValueState<Integer> cnt;
			private transient Integer taskIndex;

			@Override
			public void open(Configuration config) {
				ValueStateDescriptor<Integer> descriptor =
					new ValueStateDescriptor<Integer>(
						"cnt", // the state name
						TypeInformation.of(new TypeHint<Integer>() {}), // type information
						0); // default value of the state, if nothing was set
				cnt = getRuntimeContext().getState(descriptor);
				taskIndex = getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public Tuple3<Integer, String, Integer> map(String value) throws Exception {
				Integer currCnt = cnt.value() + 1;
				cnt.update(currCnt);
				return new Tuple3<>(taskIndex, value, currCnt);
			}

		})
		.setParallelism(parallelism)
		.map(new MapFunction<Tuple3<Integer, String, Integer>, Tuple3<Integer, String, Integer>>() {
			@Override
			public Tuple3<Integer, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
				return value;
			}
		})
		.setParallelism(parallelism)
		.addSink(new RichSinkFunction<Tuple3<Integer, String, Integer>>() {
			@Override
			public void invoke(Tuple3<Integer, String, Integer> value) throws Exception {

			}
		});

		env.execute();
	}
}

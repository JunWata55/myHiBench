/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.flinkbench.microbench;

import com.intel.hibench.flinkbench.datasource.StreamBase;
import com.intel.hibench.flinkbench.util.FlinkBenchConfig;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;

public class WordCount extends StreamBase {

  @Override
  public void processStream(final FlinkBenchConfig config, long interval, int method, int logic) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setBufferTimeout(config.bufferTimeout);
    env.enableCheckpointing(interval);    

    // set state backend
    switch(method) {
      case 0: memoryStateBackend(env); break;
      case 1: notIncrementalBackend(env); break;
      case 2: rocksDBStateBackend(env); break;
    }

    createDataStream(config);
    DataStream<Tuple2<String, String>> dataStream = env.fromSource(getKafkaSource(), WatermarkStrategy.noWatermarks(), "Kafka Source");

    if (logic == 0) {
      dataStream
        .map(new Format())
        .keyBy(value -> value.f0)
        .map(new LogicNormal(config.reportTopic, config.brokerList))
        .print();
    } else {
      dataStream
        .map(new Format())
        .keyBy(value -> value.f0)
        .map(new LogicBigState(config.reportTopic, config.brokerList))
        .print();
    }
    env.execute("Word Count Job");
  }
  
  void memoryStateBackend(StreamExecutionEnvironment env) {
    env.setStateBackend(new HashMapStateBackend());
    env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints-directory/"));
  }

  void fsStateBackend(StreamExecutionEnvironment env) {
    env.setStateBackend(new HashMapStateBackend());
    env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints-directory/"));
  }

  void notIncrementalBackend(StreamExecutionEnvironment env) {
    env.setStateBackend(new EmbeddedRocksDBStateBackend(false));
    env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints-directory/"));
  }

  void rocksDBStateBackend(StreamExecutionEnvironment env) {
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/flink-checkpoints-directory/"));
  }
}

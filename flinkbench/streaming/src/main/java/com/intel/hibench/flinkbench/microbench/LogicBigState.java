package com.intel.hibench.flinkbench.microbench;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import com.intel.hibench.common.streaming.metrics.KafkaReporter;

public class LogicBigState extends RichMapFunction<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>> {
    private transient ValueState<Integer> sum;
    private transient ValueState<BigState> state;
    private String reportTopic;
    private String brokerList;

    public LogicBigState(String reportTopic, String brokerList) {
        this.reportTopic = reportTopic;
        this.brokerList = brokerList;
    }

    @Override
    public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
        if (sum.value() == null) {
            sum.update(0);
        }

        int currentSum = sum.value();
        currentSum += input.f1.f1;
        sum.update(currentSum);

        // if (state.value() == null) {
        //     state.update(new BigState(new int[10000]));
        // } else {
        //     if (currentSum == 2) 
        //         state.update(new BigState(new int[1000]));
        // }

        if (state.value() == null) {
            state.update(new BigState(new int[1000]));
        }

        KafkaReporter kafkaReporter = new KafkaReporter(reportTopic, brokerList);
        kafkaReporter.report(Long.parseLong(input.f1.f0), System.currentTimeMillis());
        return new Tuple2<String, Tuple2<String, Integer>>(input.f0, new Tuple2<String, Integer>(input.f1.f0, currentSum));
    }

    @Override
    public void open(Configuration config) {
        // StateTtlConfig ttl = StateTtlConfig
        //   .newBuilder(Time.seconds(1))
        //   .cleanupFullSnapshot()
        //   .build();
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
            "count",
            Types.INT
        );
        // descriptor.enableTimeToLive(ttl);
        sum = getRuntimeContext().getState(descriptor);

        ValueStateDescriptor<BigState> descBS = new ValueStateDescriptor<>(
            "state",
            TypeInformation.of(new TypeHint<BigState>(){})
        );
        state = getRuntimeContext().getState(descBS);
    }
}


class LogicNormal extends RichMapFunction<Tuple2<String, Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>> {
    private transient ValueState<Integer> sum;
    private String reportTopic;
    private String brokerList;

    public LogicNormal(String reportTopic, String brokerList) {
        this.reportTopic = reportTopic;
        this.brokerList = brokerList;
    }

    @Override
    public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, Tuple2<String, Integer>> input) throws Exception {
        if (sum.value() == null) {
            sum.update(0);
        }

        int currentSum = sum.value();
        currentSum += input.f1.f1;
        sum.update(currentSum);

        KafkaReporter kafkaReporter = new KafkaReporter(reportTopic, brokerList);
        kafkaReporter.report(Long.parseLong(input.f1.f0), System.currentTimeMillis());
        return new Tuple2<String, Tuple2<String, Integer>>(input.f0, new Tuple2<String, Integer>(input.f1.f0, currentSum));
    }

    @Override
    public void open(Configuration config) {
        // StateTtlConfig ttl = StateTtlConfig
        //   .newBuilder(Time.seconds(1))
        //   .cleanupFullSnapshot()
        //   .build();
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
            "count",
            Types.INT
        );
        // descriptor.enableTimeToLive(ttl);
        sum = getRuntimeContext().getState(descriptor);
    }
}
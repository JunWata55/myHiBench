package com.intel.hibench.flinkbench.microbench;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import com.intel.hibench.common.streaming.UserVisitParser;

public class Format implements MapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<String, Integer>>> {
    @Override
    public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, String> input) throws Exception {
        String ip = UserVisitParser.parse(input.f1).getIp();
        // map record to <browser, <timeStamp, 1>> type
        return new Tuple2<String, Tuple2<String, Integer>>(ip, new Tuple2<String, Integer>(input.f0, 1));
    }
}

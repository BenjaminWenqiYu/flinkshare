package com.benjamin.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-03-04
 * Time: 15:18
 * Description:
 */
public class FlinkSocketTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("127.0.0.1", 6666, "\n");

        DataStream<Tuple2<String, Integer>> dataStream = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word: s.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum(1);

        dataStream.print();

        env.execute("log");
    }
}

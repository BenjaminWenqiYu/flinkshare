package com.benjamin.examples.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-02-02
 * Time: 11:19
 * Description:
 */
public class FunctionAndRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Integer> dataStream = env.fromElements(1, 3, 6, 5, 8);

        dataStream.map(new MyRichMapFunction()).print();

        env.execute("");
    }

}
class MyMapFunction implements MapFunction<Integer, String> {

    @Override
    public String map(Integer value) throws Exception {
        return String.valueOf(value.intValue() * value.intValue());
    }
}

class MyRichMapFunction extends RichMapFunction<Integer, String> {

    private RuntimeContext ctx;
    private String taskName;
    private int taskId;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ctx = getRuntimeContext();
        taskId = ctx.getIndexOfThisSubtask();
        taskName = ctx.getTaskName();
        System.out.printf("Open %s-%d \n", taskName, taskId);
    }


    @Override
    public String map(Integer integer) throws Exception {
        return "taskName: " + taskName + ",taskId: " + taskId + ", value: " + integer;
    }
}

package com.benjamin.examples.batch;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-02-02
 * Time: 09:26
 * Description:
 */
public class DataSetAPI {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        flatMapTest(env);
    }

    private static void flatMapTest(ExecutionEnvironment env) throws Exception {
        DataSet<Integer> input = env.fromElements(1, 2, 3);
        input.flatMap((Integer number, Collector<String> out) -> {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < number; i++) {
                builder.append("a");
            }
            out.collect(builder.toString());
        }).returns(Types.STRING).print();
    }


}

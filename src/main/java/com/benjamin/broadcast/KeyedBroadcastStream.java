package com.benjamin.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-02-06
 * Time: 18:25
 * Description: 广播流
 */
public class KeyedBroadcastStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 广播流
        DataStream<String> ds = env
                .readTextFile("E:/GitProject/flinkshare/flinkshare/src/main/resources/log1.txt");
        SingleOutputStreamOperator<User> user = ds.map(s -> new User(s.split(",")[0], s.split(",")[1]));

//        user.print("user: ");
        // 非广播流
        KeyedStream<Order, String> order = env
                .readTextFile("E:/GitProject/flinkshare/flinkshare/src/main/resources/log2.txt")
                .map(s -> new Order(s.split(",")[0], s.split(",")[1],
                        Integer.valueOf(s.split(",")[2]).intValue()))
                .keyBy((KeySelector<Order, String>) value -> value.name());

//        order.print("order: ");
        // 定义广播状态描述
        MapStateDescriptor<String, User> descriptor =
                new MapStateDescriptor<String, User>("user", String.class, User.class);
        // 广播广播流
        BroadcastStream<User> broadcast = user.broadcast(descriptor);

        // 将非广播流与广播流关联在一起
        order.connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String, Order, User, String>() {
                    /**
                     * readOnlyContext对广播流只有只读功能
                     * @param order
                     * @param readOnlyContext
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Order order, ReadOnlyContext readOnlyContext,
                                               Collector<String> collector) throws Exception {
                        ReadOnlyBroadcastState<String, User> broadcastState =
                                readOnlyContext.getBroadcastState(descriptor);
                        // 从广播中获取对应的key的value
                        User user = broadcastState.get(order.name());
                        if (null != user) {
                            String s = "user:(name -> " + user.name() + ", job -> " + user.job() +
                                    "); order:(sex -> " + order.sex() + ", age -> " + order.age() + ")";
                            collector.collect(s);
                        }
                    }

                    /**
                     * context对广播流有读写功能
                     * 为广播流的每个记录调用，
                     * @param user
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(User user, Context context,
                                                        Collector<String> collector) throws Exception {
                        BroadcastState<String, User> broadcastState = context.getBroadcastState(descriptor);
                        broadcastState.put(user.name(), user);
                    }
                }).print();

        env.execute("");

    }
}


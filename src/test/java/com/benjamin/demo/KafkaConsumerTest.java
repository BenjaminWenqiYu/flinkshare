package com.benjamin.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: ywq
 * Date: 2021-03-04
 * Time: 13:47
 * Description: 读取Kafka数据
 */
public class KafkaConsumerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "xxx.xxx.xxx.xxx:6667,xxx.xxx.xxx.xxx:6667,xxx.xxx.xxx.xxx:6667");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("meter_reading",
                new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStream<String> ds = env
                .addSource(consumer);

        ds.print();

        env.execute("test");
    }
}

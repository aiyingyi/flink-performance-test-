package com.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;


import java.util.Properties;

public class FlinkTest {
    public static void main(String[] args) throws Exception {

        String groupIdC = args[0];
        String groupIdP = args[1];
        String topicC = args[2];
        String topicP = args[3];
        int interval = Integer.parseInt(args[4]);
        int parrel = Integer.parseInt(args[5]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置算子的并行度
        env.setParallelism(parrel);


        // 定义kafka consumer 配置文件
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka35:9092");
        properties.setProperty("group.id", groupIdC);
        properties.setProperty("auto.offset.reset", "earliest");

        // 定义kafka producer配置文件
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "kafka35:9092");
        pro.setProperty("group.id", groupIdP);
        pro.setProperty("acks", "1");

        // 创建数据源
        DataStreamSource<String> originalStream = env.addSource(new FlinkKafkaConsumer010<String>(topicC, new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<String> dataStream = originalStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).process(new KeyedProcessFunction<String, String, String>() {

            boolean first = true;   // 每个算子的并行度都会创建这个变量
            long start = 0L;

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                // 如果是第一条数据
                if (first) {
                    start = System.currentTimeMillis();
                    first = false;

                    // 创建定时器
                    ctx.timerService().registerProcessingTimeTimer(start + interval * 1000);
                }
                System.out.println(value);
                Thread.sleep(5);
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                long end = System.currentTimeMillis();
                System.exit(-1);
            }
        });
        // 将流中的数据写入到kafka
        dataStream.addSink(new FlinkKafkaProducer010<String>(topicP, new SimpleStringSchema(), pro));
        env.execute("flinktest");
    }
}

package com.jointest;

import com.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class intervalJoinTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2)
        );


        SingleOutputStreamOperator<Event> stream2 = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
        );

        SingleOutputStreamOperator<String> processedStream = stream1.keyBy(data -> data.f0)
                .intervalJoin(stream2.keyBy(data -> data.user))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {

                    @Override
                    public void processElement(Tuple3<String, String, Long> left,
                                               Event right,
                                               ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        out.collect(left.f0 + "=>" + right);
                    }
                });

        processedStream.print();


        env.execute();

    }

}

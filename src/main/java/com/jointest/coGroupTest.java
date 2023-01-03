package com.jointest;

import com.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.util.Collector;

public class coGroupTest {

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

        DataStream<String> apply = stream1
                .coGroup(stream2)
                .where(data -> data.f0)
                .equalTo(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> first,
                                        Iterable<Event> second,
                                        Collector<String> out) throws Exception {
                        out.collect(first + "=>" + second);
                    }
                });

        apply.print();

        env.execute();

    }

}

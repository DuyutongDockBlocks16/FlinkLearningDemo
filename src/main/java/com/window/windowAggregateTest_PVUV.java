package com.window;

import com.source.Event;
import com.source.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

//统计PV与UV，两者相除
public class windowAggregateTest_PVUV {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new clickSource()).
                assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Event>forBoundedOutOfOrderness(Duration.ZERO).
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

//        todo PV
        SingleOutputStreamOperator<Integer> PV = stream.map(new MapFunction<Event, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Event value) throws Exception {
                return Tuple2.of("key", 1);
            }
        }).keyBy(data -> data.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of("key", value1.f1 + value2.f1);
            }
        }).map(data -> data.f1);
//        PV.print();


//        todo UV
//        SingleOutputStreamOperator<Integer> UV = stream.map(new MapFunction<Event, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(Event value) throws Exception {
//                        return Tuple2.of(value.user, 1);
//                    }
//                })
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .aggregate(new AggregateFunction<Tuple2<String, Integer>, HashSet, Integer>() {
//                    @Override
//                    public HashSet createAccumulator() {
//                        return new HashSet<String>();
//                    }
//
//                    @Override
//                    public HashSet add(Tuple2<String, Integer> value, HashSet accumulator) {
//                        accumulator.add(value.f0);
//                        return accumulator;
//                    }
//
//                    @Override
//                    public Integer getResult(HashSet accumulator) {
//                        return accumulator.size();
//                    }
//
//                    @Override
//                    public HashSet merge(HashSet a, HashSet b) {
//                        return null;
//                    }
//                });

//        UV.print();

        SingleOutputStreamOperator<Double> Activation = stream.map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.user;
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<String, Tuple2<HashSet, Integer>, Double>() {

                    @Override
                    public Tuple2<HashSet, Integer> createAccumulator() {

                        HashSet<Object> set = new HashSet<>();
                        return Tuple2.of(set, 0);
                    }

                    @Override
                    public Tuple2<HashSet, Integer> add(String value, Tuple2<HashSet, Integer> accumulator) {
                        accumulator.f0.add(value);

                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<HashSet, Integer> accumulator) {

//                        System.out.println(accumulator.f1);
//                        for (Object string: accumulator.f0){
//                            System.out.println(string);
//                        }

                        return ((accumulator.f1+ 0.0)/(accumulator.f0.size()+ 0.0)) ;
                    }

                    @Override
                    public Tuple2<HashSet, Integer> merge(Tuple2<HashSet, Integer> a, Tuple2<HashSet, Integer> b) {
                        return null;
                    }
                });

        Activation.print();


        env.execute();

    }
}

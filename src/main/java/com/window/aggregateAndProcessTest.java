package com.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class aggregateAndProcessTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Integer>> stream = env.addSource(new integerSource());

        SingleOutputStreamOperator<Tuple2<Long, Integer>> streamWithWatermark = stream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<Long, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<Long, Integer>>() {

                            @Override
                            public long extractTimestamp(Tuple2<Long, Integer> element, long recordTimestamp) {
                                return element.f0;
                            }
                        }));

        streamWithWatermark
                .keyBy(data->true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new myAggregateFunction(), new myProcessFunction()).print();


        env.execute();

    }

    public static class myAggregateFunction implements AggregateFunction<Tuple2<Long, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<Long, Integer> value, Integer accumulator) {
            System.out.println(value.f1);
            return accumulator+value.f1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class myProcessFunction extends ProcessWindowFunction<Integer, String, Boolean, TimeWindow>{


        @Override
        public void process(Boolean aBoolean,
                            ProcessWindowFunction<Integer, String, Boolean, TimeWindow>.Context context,
                            Iterable<Integer> elements,
                            Collector<String> out) throws Exception {

            int sum_num = elements.iterator().next();

            out.collect("求和为"+sum_num);

        }
    }

}

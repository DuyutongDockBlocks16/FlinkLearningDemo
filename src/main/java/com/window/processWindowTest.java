package com.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class processWindowTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Integer>> stream = env.addSource(new integerSource());


        SingleOutputStreamOperator<Tuple2<Long, Integer>> tuple2SingleOutputStreamOperator = stream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<Long, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<Long, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<Long, Integer> element, long recordTimestamp) {
                                return element.f0;
                            }
                        }));


        tuple2SingleOutputStreamOperator.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new myProcessWindowFunc())
                .print();


        env.execute();


    }

public static class myProcessWindowFunc extends ProcessWindowFunction<Tuple2<Long, Integer>, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean,
                            Context context,
                            Iterable<Tuple2<Long, Integer>> elements,
                            Collector<String> out) throws Exception {
            ArrayList<Integer> num_List = new ArrayList<>();


            for (Tuple2<Long, Integer> time_num_tuple : elements) {
                num_List.add(time_num_tuple.f1);
            }


            Collections.sort(num_List);

//            System.out.println(num_List.get(4));
//            System.out.println(num_List.get(4));

            int index_a = (int)Math.floor((num_List.size()-1)/2.0);
            int index_b = (int)Math.ceil((num_List.size()-1)/2.0);

//            System.out.println(num_List.size());
//            System.out.println(index_a);
//            System.out.println((num_List.size()-1)/2.0);


            double middleNum = (num_List.get(index_a)+num_List.get(index_b))/2.0;

//            System.out.println(middleNum);

            String middleNumString= middleNum +"";

            out.collect("整数串："+num_List.toString()+"的中位数是:"+ middleNumString);

        }
    }
}

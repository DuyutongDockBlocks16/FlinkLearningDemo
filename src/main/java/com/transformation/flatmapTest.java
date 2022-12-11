package com.transformation;

import com.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flatmapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream1 = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("bob", "./cart", 2000L)
        );

//        SingleOutputStreamOperator<String> mary_url = stream1.flatMap(new MyflapMap());
//        mary_url.print();

//        SingleOutputStreamOperator<String> mary_url_2 = stream1.flatMap(new FlatMapFunction<Event, String>() {
//            @Override
//            public void flatMap(Event value, Collector<String> out) throws Exception {
//                out.collect(value.user);
//                out.collect(value.url);
//            }
//        });
//        mary_url_2.print();


        SingleOutputStreamOperator<String> user_url_3 = stream1.flatMap((Event value, Collector<String> out) -> {
            out.collect(value.user);
            out.collect(value.url);
        }).returns(new TypeHint<String>() {});

        user_url_3.print();

        env.execute();

    }

    public static class MyflapMap implements FlatMapFunction<Event,String>{

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
        }
    }

}

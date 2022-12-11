package com.transformation;

import com.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class reduceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> Event_Stream = env.fromElements(
                new Event("mary", "/home", 1000L),
                new Event("bob", "/home", 1000L),
                new Event("alice", "/home", 1000L),
                new Event("alice", "/home", 1000L),
                new Event("alice", "/home", 1000L),
                new Event("alice", "/home", 1000L),
                new Event("bob", "/home", 1000L),
                new Event("bob", "/home", 1000L),
                new Event("bob", "/home", 1000L),
                new Event("bob", "/home", 1000L)
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> user_clicks_Tuple = Event_Stream.map(new MapFunction<Event, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1);
            }
        }).keyBy(data -> data.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1++;
                return Tuple2.of(value1.f0, value1.f1);

            }
        });

//        user_clicks_Tuple.print();

        SingleOutputStreamOperator<String> the_most_active_user = user_clicks_Tuple.keyBy(data -> "key").reduce(
                new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }
        ).map(data -> data.f0);


        the_most_active_user.print();
        env.execute();

    }
}

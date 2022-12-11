package com.transformation;

import com.source.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class simpleAggTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000l),
                new Event("mary", "./cart", 2000l)
        );

        stream.keyBy(new KeySelector<Event, String>() {

            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print();

        env.execute();
    }
}

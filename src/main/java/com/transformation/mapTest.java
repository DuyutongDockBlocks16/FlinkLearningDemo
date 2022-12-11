package com.transformation;

import com.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class mapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L)
        );

//        stream.map(new MyMapper()).print();

//        stream.map(new MapFunction<Event,String>() {
//
//                    @Override
//                    public String map(Event value) throws Exception {
//                        return value.user;
//                    }
//                }).print();

        stream.map(data->data.user).print();

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String >{

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}

package com.transformation;

import com.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class filterTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 3000l),
                new Event("bob", "./cart", 1000l)
        );

//        SingleOutputStreamOperator<Event> mary = stream.filter(new FilterFunction<Event>() {
//            @Override
//            public boolean filter(Event value) throws Exception {
//                return value.user.equals("mary");
//            }
//        });
//        mary.print();

//        SingleOutputStreamOperator<Event> mar = stream.filter(new Myfilter());
//        mar.print();

        SingleOutputStreamOperator<Event> mary = stream.filter(Event -> Event.user.equals("mary"));

        mary.print();

        env.execute();

    }

    public static class Myfilter implements FilterFunction<Event>{


        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("mary");
        }
    }
}

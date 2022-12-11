package com.transformation;

import com.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class filterWithArgumentTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> stream = env.fromElements(
                new Event("alice", "./home", 1000L),
                new Event("bob", "./cart", 2000L)
        );

        SingleOutputStreamOperator<Event> home = stream.filter(new FilterForUrl("home"));

        home.print();

        env.execute();

    }

    public static class FilterForUrl implements FilterFunction<Event>{

        private static String Keyword;

        public FilterForUrl(String keyword){
            this.Keyword=keyword;
        }

        @Override
        public boolean filter(Event value) throws Exception {
            return value.url.contains(this.Keyword);
        }
    }
}

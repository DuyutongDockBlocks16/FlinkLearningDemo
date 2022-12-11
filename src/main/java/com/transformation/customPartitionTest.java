package com.transformation;

import com.source.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class customPartitionTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000l),
                new Event("bob", "./cart", 2000L),
                new Event("du", "./cart", 2000L),
                new Event("jack", "./cart", 2000L),
                new Event("alice", "/.favr", 3000L)
        );

        DataStream<Event> eventDataStream = stream.partitionCustom(
                new Partitioner<Event>() {

                    @Override
                    public int partition(Event key, int numPartitions) {
                        if (key.user.equals("mary") || key.user.equals("bob")) {
//                            System.out.println("00000");
                            return 0;
                        } else {
//                            System.out.println("11111");
                            return 1;
                        }
                    }
                },
                new KeySelector<Event, Event>() {
                    @Override
                    public Event getKey(Event value) throws Exception {
                        return value;
                    }
                }
        );

//        SingleOutputStreamOperator<Tuple2<Integer, String>> map = eventDataStream.map(new RichMapFunction<Event, Tuple2<Integer, String>>() {
//            @Override
//            public Tuple2<Integer, String> map(Event value) throws Exception {
//                return Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), value.user.toString());
//            }
//        });

        eventDataStream.print().setParallelism(4);


        env.execute();


    }
}

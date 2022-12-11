package com.watermark;

import com.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Time;
import java.time.Duration;

public class waterMarkTest {
    public static void main(String[] args) throws Exception{


//        int myint = 1;
//
//        String[] mystring = {"1","2","3"};
//
//        System.out.println(mystring[0]);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);

        env.setParallelism(4);

        DataStream<Event> stream = env.fromElements(
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L)
        ).
//                todo:有序流的watermark生成
//                assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                return element.timestamp;
//                            }
//                        }));
//        todo:有序流的watermark生成
                assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1)).
                    withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                        @Override
                        public long extractTimestamp(Event element, long recordTimestamp) {
                            return element.timestamp;
                        }
                    }));

    }
}

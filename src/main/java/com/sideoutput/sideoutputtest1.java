package com.sideoutput;

import com.source.Event;
import com.source.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class sideoutputtest1 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        创建带有watermark的数据流
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new clickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {

                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));


//        定义侧输出流标签

        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("Bob"){};
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {};

        SingleOutputStreamOperator<Event> spiltstream = dataStream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Bob")) {
//                    拆出Bob的数据
                    ctx.output(bobTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Mary")) {
//                    拆出Mary的数据
                    ctx.output(maryTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });

//        获取侧输出流数据
        DataStream<Tuple3<String, String, Long>> sideOutputBob = spiltstream.getSideOutput(bobTag);

        sideOutputBob.print("Bobtag");

        spiltstream.print("spiltstream");

        env.execute();


    }


}

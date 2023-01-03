package com.sideoutput;

import com.source.Event;
import com.source.clickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class sideoutputtest2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> dataStream = env.addSource(new clickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        OutputTag<Event> BobOutputTag = new OutputTag<Event>("Bob") {};

        SingleOutputStreamOperator<Event> splitStream = dataStream.process(new ProcessFunction<Event, Event>() {

            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Bob")) {
                    ctx.output(BobOutputTag, value);
                } else {
                    out.collect(value);
                }
            }
        });

        splitStream.getSideOutput(BobOutputTag).print("Bobtag");

        splitStream.print("splitstream");

        env.execute();

    }
}

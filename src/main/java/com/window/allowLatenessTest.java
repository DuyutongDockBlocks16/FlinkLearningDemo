package com.window;

import com.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class allowLatenessTest {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.socketTextStream("DESKTOP-VEQF9SF", 7777)
                .map(new MapFunction<String, Event>() {

                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(",");
                        String[] trimedFields = new String[fields.length];

                        for (int i = 0; i < fields.length; i++) {
                            trimedFields[i] = fields[i].trim();
                        }

                        return new Event(trimedFields[0], trimedFields[1], Long.valueOf(trimedFields[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        eventSingleOutputStreamOperator.print("input");

//        定义一个输出标签,必须使用内部匿名类，因为会有泛型擦除
        OutputTag<Event> late = new OutputTag<Event>("id"){};


//        统计每一个url的访问量
        SingleOutputStreamOperator<String> result = eventSingleOutputStreamOperator
                .keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UrlViewCountAgg(), new myProcessingFunc());

//        输出正式流
        result.print("result");

//        基于late标签获取侧输出流
        result.getSideOutput(late).print("late");

        env.execute();

    }

    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long>{


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class myProcessingFunc extends ProcessWindowFunction<Long, String, String, TimeWindow>{

        @Override
        public void process(String url,
                            ProcessWindowFunction<Long, String, String, TimeWindow>.Context context,
                            Iterable<Long> elements,
                            Collector<String> out) throws Exception {

            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long UV_num = elements.iterator().next();

            out.collect("窗口起始时间："+start+"窗口终止时间"+end+",url:"+url+",的UV值为："+UV_num);

        }
    }

}

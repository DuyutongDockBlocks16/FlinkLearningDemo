package com.window;

import com.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class windowTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);

        env.setParallelism(4);

        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L),
                new Event("bob", "./home", 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        KeyedStream<Event, String> eventStringKeyedStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;

            }
        });

        WindowedStream<Event, String, TimeWindow> window1 = eventStringKeyedStream.
                window(TumblingEventTimeWindows.of(Time.seconds(10), Time.hours(-8)));
        //滚动-事件时间窗口参数（第一个参数：窗口大小，第二个参数：偏移量）

        WindowedStream<Event, String, TimeWindow> window2 = eventStringKeyedStream.
                window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)));
        //滑动-事件时间窗口参数（第一个参数：窗口大小，第二个参数：滑动步长）

        WindowedStream<Event, String, GlobalWindow> window3 = eventStringKeyedStream.countWindow(3);
        //滚动-计数窗口，第一个参数：窗口大小

        WindowedStream<Event, String, GlobalWindow> window4 = eventStringKeyedStream.countWindow(3,1);
        //滑动-计数窗口，第一个参数：窗口大小，第二个窗口，步长

        WindowedStream<Event, String, TimeWindow> window5 = eventStringKeyedStream
                .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Event>() {
            @Override
            public long extract(Event element) {
                return element.timestamp;
            }
        }));
        //会话-事件事件窗口，根据泛型动态分配gap

        WindowedStream<Event, String, TimeWindow> window6 = eventStringKeyedStream
                .window(EventTimeSessionWindows.withGap(Time.seconds(100)));
        //会话-事件时间窗口，静态gap

    }
}
